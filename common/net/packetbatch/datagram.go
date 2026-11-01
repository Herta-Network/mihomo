package packetbatch

import (
	"errors"
	"io"
	"net"

	P "github.com/metacubex/mihomo/common/net/packet"
	"github.com/metacubex/sing/common/buf"
	"github.com/metacubex/sing/common/bufio"
	M "github.com/metacubex/sing/common/metadata"
	N "github.com/metacubex/sing/common/network"
)

// NewDatagramEncoder batches the output of an existing synchronous datagram
// codec. Encryption, session identifiers and address encoding remain owned by
// that codec. The supplied transport must explicitly support counted packets;
// opaque proxy-chain transports keep their existing scalar behavior.
func NewDatagramEncoder(transport net.PacketConn, build func(net.PacketConn) net.PacketConn) net.PacketConn {
	writer, ok := P.CreatePacketBatchWriterWithCount(transport)
	if !ok {
		return build(transport)
	}
	sink := &datagramSink{NetPacketConn: bufio.NewPacketConn(transport)}
	original := build(sink)
	codec := newPacketCodec(original)
	return &datagramEncoder{NetPacketConn: codec, reader: newPacketReader(original), sink: sink, writer: writer, options: N.NewReadWaitOptions(nil, codec)}
}

type datagramEncoder struct {
	N.NetPacketConn
	state   State
	reader  packetReader
	sink    *datagramSink
	writer  N.PacketBatchWriterWithCount
	options N.ReadWaitOptions
}

func (c *datagramEncoder) WritePacketBatchCount(buffers []*buf.Buffer, destinations []M.Socksaddr) (written int, err error) {
	c.state.Access.Lock()
	defer c.state.Access.Unlock()
	if err = c.state.Check(); err != nil {
		buf.ReleaseMulti(buffers)
		return
	}
	if err = Validate(buffers, destinations); err != nil {
		buf.ReleaseMulti(buffers)
		return
	}
	for start := 0; start < len(buffers); {
		if err = c.state.Check(); err != nil {
			buf.ReleaseMulti(buffers[start:])
			return
		}
		end := min(start+MaxPackets, len(buffers))
		var frames [MaxPackets]*buf.Buffer
		var targets [MaxPackets]M.Socksaddr
		for i := start; i < end; i++ {
			input := c.options.Copy(buffers[i])
			buffers[i] = input
			// Keep the encoded input alive through codecs that defer Release after
			// Write. The sink can retain an in-place encrypted buffer without copying.
			input.IncRef()
			c.sink.current = input
			c.sink.frame = nil
			c.sink.borrowed = false
			c.sink.active = true
			err = c.NetPacketConn.WritePacket(input, destinations[i])
			c.sink.active = false
			c.sink.current = nil
			input.DecRef()
			if !c.sink.borrowed {
				input.Release()
			}
			frame := c.sink.frame
			c.sink.frame = nil
			if err == nil && frame == nil {
				err = errors.New("packet codec did not emit one datagram")
			}
			if err != nil {
				frame.Release()
				buf.ReleaseMulti(frames[:i-start])
				buf.ReleaseMulti(buffers[i+1:])
				return
			}
			frames[i-start] = frame
			targets[i-start] = c.sink.destination
		}
		n, writeErr := c.writer.WritePacketBatchCount(frames[:end-start], targets[:end-start])
		n, writeErr = bufio.CheckPacketBatchCount(n, end-start, writeErr)
		written += n
		if writeErr != nil {
			buf.ReleaseMulti(buffers[end:])
			return written, writeErr
		}
		start = end
	}
	return
}
func (c *datagramEncoder) WritePacket(buffer *buf.Buffer, destination M.Socksaddr) error {
	c.state.Access.Lock()
	defer c.state.Access.Unlock()
	if err := c.state.Check(); err != nil {
		buffer.Release()
		return err
	}
	buffer = c.options.Copy(buffer)
	return c.NetPacketConn.WritePacket(buffer, destination)
}
func (c *datagramEncoder) WriteTo(p []byte, addr net.Addr) (int, error) {
	c.state.Access.Lock()
	defer c.state.Access.Unlock()
	if err := c.state.Check(); err != nil {
		return 0, err
	}
	return c.NetPacketConn.WriteTo(p, addr)
}
func (c *datagramEncoder) Close() error            { return c.state.Close(c.reader) }
func (c *datagramEncoder) Upstream() any           { return c.reader.EnhancePacketConn }
func (c *datagramEncoder) ReaderReplaceable() bool { return true }
func (c *datagramEncoder) WriterReplaceable() bool { return false }

// Only the synchronous codec write calls access capture fields, under the
// owning encoder's write lock. Reads and Close continue to the live transport.
type datagramSink struct {
	N.NetPacketConn
	active      bool
	current     *buf.Buffer
	frame       *buf.Buffer
	destination M.Socksaddr
	borrowed    bool
}

func (c *datagramSink) WriteTo(p []byte, addr net.Addr) (int, error) {
	if !c.active {
		n, err := c.NetPacketConn.WriteTo(p, addr)
		if n < 0 || n > len(p) {
			return 0, io.ErrShortWrite
		}
		if n != len(p) && err == nil {
			err = io.ErrShortWrite
		}
		return n, err
	}
	if c.frame != nil {
		return 0, errors.New("packet codec emitted multiple datagrams")
	}
	current := c.current.Bytes()
	if len(p) > 0 && len(p) == len(current) && &p[0] == &current[0] {
		c.frame = c.current
		c.borrowed = true
	} else {
		c.frame = buf.NewSize(len(p))
		_, _ = c.frame.Write(p)
	}
	c.destination = M.SocksaddrFromNet(addr)
	return len(p), nil
}
func (c *datagramSink) WritePacket(buffer *buf.Buffer, destination M.Socksaddr) error {
	defer buffer.Release()
	var addr net.Addr = destination
	if destination.IsIP() {
		addr = destination.UDPAddr()
	}
	_, err := c.WriteTo(buffer.Bytes(), addr)
	return err
}

func (c *datagramEncoder) WaitReadFrom() ([]byte, func(), net.Addr, error) {
	return c.reader.WaitReadFrom()
}
func (c *datagramEncoder) ReadFrom(p []byte) (int, net.Addr, error) { return c.reader.ReadFrom(p) }
func (c *datagramEncoder) ReadPacket(b *buf.Buffer) (M.Socksaddr, error) {
	return c.reader.readPacket(b)
}

func (c *datagramEncoder) UpstreamWriter() any { return c.NetPacketConn }
