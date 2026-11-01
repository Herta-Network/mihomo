package packetbatch

import (
	"io"
	"net"

	"github.com/metacubex/sing/common/buf"
	"github.com/metacubex/sing/common/bufio"
	M "github.com/metacubex/sing/common/metadata"
	N "github.com/metacubex/sing/common/network"
)

// ProgressConn is an explicit adapter for transports whose public API sends
// complete datagrams (QUIC engines and userspace network stacks). It counts
// their accepted prefix; it does not claim to batch kernel socket calls.
// Stream-backed packet APIs must set terminalErrors so a partial frame cannot
// be followed by another packet on a desynchronized stream.
type ProgressConn struct {
	N.NetPacketConn
	state          State
	reader         packetReader
	terminalErrors bool
}

func NewProgressConn(conn net.PacketConn, terminalErrors bool) *ProgressConn {
	return &ProgressConn{NetPacketConn: bufio.NewPacketConn(conn), reader: newPacketReader(conn), terminalErrors: terminalErrors}
}
func (c *ProgressConn) WritePacketBatchCount(buffers []*buf.Buffer, destinations []M.Socksaddr) (written int, err error) {
	defer buf.ReleaseMulti(buffers)
	c.state.Access.Lock()
	defer c.state.Access.Unlock()
	if err = c.state.Check(); err != nil {
		return
	}
	if err = Validate(buffers, destinations); err != nil {
		return
	}
	for i, buffer := range buffers {
		if err = c.state.Check(); err != nil {
			return
		}
		var addr net.Addr = destinations[i]
		if destinations[i].IsIP() {
			addr = destinations[i].UDPAddr()
		}
		n, writeErr := c.NetPacketConn.WriteTo(buffer.Bytes(), addr)
		if n < 0 || n > buffer.Len() {
			n, writeErr = 0, io.ErrShortWrite
		} else if n != buffer.Len() && writeErr == nil {
			writeErr = io.ErrShortWrite
		}
		if n == buffer.Len() && (writeErr == nil || n > 0) {
			written++
		}
		if writeErr != nil {
			if c.terminalErrors {
				writeErr = c.state.Fail(c.reader, writeErr)
			}
			return written, writeErr
		}
	}
	return
}
func (c *ProgressConn) WriteTo(p []byte, addr net.Addr) (int, error) {
	n, err := c.WritePacketBatchCount([]*buf.Buffer{buf.As(p)}, []M.Socksaddr{M.SocksaddrFromNet(addr)})
	if n == 1 {
		return len(p), err
	}
	return 0, err
}
func (c *ProgressConn) WritePacket(b *buf.Buffer, destination M.Socksaddr) error {
	_, err := c.WritePacketBatchCount([]*buf.Buffer{b}, []M.Socksaddr{destination})
	return err
}
func (c *ProgressConn) Close() error            { return c.state.Close(c.reader) }
func (c *ProgressConn) Upstream() any           { return c.reader.EnhancePacketConn }
func (c *ProgressConn) ReaderReplaceable() bool { return true }
func (c *ProgressConn) WriterReplaceable() bool { return false }

func (c *ProgressConn) WaitReadFrom() ([]byte, func(), net.Addr, error) {
	return c.reader.WaitReadFrom()
}
func (c *ProgressConn) ReadFrom(p []byte) (int, net.Addr, error)      { return c.reader.ReadFrom(p) }
func (c *ProgressConn) ReadPacket(b *buf.Buffer) (M.Socksaddr, error) { return c.reader.readPacket(b) }

func (c *ProgressConn) UpstreamWriter() any { return nil }
