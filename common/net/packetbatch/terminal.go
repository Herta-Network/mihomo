package packetbatch

import (
	"io"
	"net"
)

// TerminalConn keeps stream-carried packet failures terminal without advertising
// a batch capability. Close does not wait for a blocked writer.
type TerminalConn struct {
	packetReader
	state State
}

func NewTerminalConn(conn net.PacketConn) *TerminalConn {
	return &TerminalConn{packetReader: newPacketReader(conn)}
}

func (c *TerminalConn) WriteTo(p []byte, addr net.Addr) (int, error) {
	c.state.Access.Lock()
	defer c.state.Access.Unlock()
	if err := c.state.Check(); err != nil {
		return 0, err
	}
	n, err := c.packetReader.WriteTo(p, addr)
	if n < 0 || n > len(p) {
		n, err = 0, io.ErrShortWrite
	} else if n != len(p) {
		n = 0 // An incomplete datagram is not a completed payload.
		if err == nil {
			err = io.ErrShortWrite
		}
	}
	if err != nil {
		return n, c.state.Fail(c.packetReader, err)
	}
	return n, nil
}

func (c *TerminalConn) Close() error            { return c.state.Close(c.packetReader) }
func (c *TerminalConn) Upstream() any           { return c.EnhancePacketConn }
func (c *TerminalConn) ReaderReplaceable() bool { return true }
func (c *TerminalConn) WriterReplaceable() bool { return false }
func (c *TerminalConn) UpstreamWriter() any     { return nil }
