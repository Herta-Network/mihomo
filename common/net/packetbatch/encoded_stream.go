package packetbatch

import (
	"errors"
	"io"
	"net"

	"github.com/metacubex/sing/common/buf"
	"github.com/metacubex/sing/common/bufio"
	M "github.com/metacubex/sing/common/metadata"
	N "github.com/metacubex/sing/common/network"
)

// StreamEncoderOptions describes the logical destination and protocol limits.
// Validate runs for the complete batch before any nonce or lazy header changes.
type StreamEncoderOptions struct {
	Destination M.Socksaddr
	Validate    func(*buf.Buffer, M.Socksaddr) error
}

// NewStreamEncoder wraps a synchronous packet codec at its transport boundary.
// The codec still owns encryption, padding and lazy request headers. Each
// logical packet's encoded byte boundary is recorded before a joined write.
// This is for codecs whose write path never waits for a transport response.
func NewStreamEncoder(transport net.Conn, build func(net.Conn) (net.Conn, error), options StreamEncoderOptions) (*StreamEncoder, error) {
	sink := &streamSink{Conn: transport}
	codec, err := build(sink)
	if err != nil {
		return nil, err
	}
	packets, ok := codec.(net.PacketConn)
	if !ok {
		return nil, errors.New("stream codec does not implement net.PacketConn")
	}
	return &StreamEncoder{Conn: codec, reader: newPacketReader(packets), packets: newPacketCodec(packets), sink: sink, options: options, headroom: N.NewReadWaitOptions(nil, codec)}, nil
}

type StreamEncoder struct {
	net.Conn
	packets  N.NetPacketConn
	sink     *streamSink
	state    State
	reader   packetReader
	options  StreamEncoderOptions
	headroom N.ReadWaitOptions
}

func (c *StreamEncoder) WritePacketBatchCount(buffers []*buf.Buffer, destinations []M.Socksaddr) (written int, err error) {
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
	for i, buffer := range buffers {
		if buffer.Len() > 65535 {
			err = io.ErrShortBuffer
		} else if c.options.Validate != nil {
			err = c.options.Validate(buffer, destinations[i])
		}
		if err != nil {
			buf.ReleaseMulti(buffers)
			return
		}
	}
	for start := 0; start < len(buffers); {
		if err = c.state.Check(); err != nil {
			buf.ReleaseMulti(buffers[start:])
			return
		}
		end := min(start+MaxPackets, len(buffers))
		var ends [MaxPackets]int64
		c.sink.active = true
		c.sink.size = 0
		for i := start; i < end; i++ {
			input := c.headroom.Copy(buffers[i])
			buffers[i] = input
			input.IncRef()
			c.sink.current = input
			c.sink.transferred = false
			before := c.sink.size
			err = c.packets.WritePacket(input, destinations[i])
			input.DecRef()
			if !c.sink.transferred {
				input.Release()
			}
			c.sink.current = nil
			if err == nil && c.sink.size == before {
				err = errors.New("packet codec emitted no frame")
			}
			if err != nil {
				c.sink.active = false
				c.sink.release()
				buf.ReleaseMulti(buffers[i+1:])
				return written, c.state.Fail(c.reader, err)
			}
			ends[i-start] = c.sink.size
		}
		c.sink.active = false
		total := c.sink.size
		n, writeErr := bufio.NewVectorisedWriterWithCount(c.sink.Conn).WriteVectorisedCount(c.sink.records)
		c.sink.reset()
		if n < 0 || n > total {
			n, writeErr = 0, io.ErrShortWrite
		} else if n != total && writeErr == nil {
			writeErr = io.ErrShortWrite
		}
		for _, boundary := range ends[:end-start] {
			if boundary > n {
				break
			}
			written++
		}
		if writeErr != nil {
			buf.ReleaseMulti(buffers[end:])
			return written, c.state.Fail(c.reader, writeErr)
		}
		start = end
	}
	return
}
func (c *StreamEncoder) WritePacket(buffer *buf.Buffer, dest M.Socksaddr) error {
	_, err := c.WritePacketBatchCount([]*buf.Buffer{buffer}, []M.Socksaddr{dest})
	return err
}
func (c *StreamEncoder) WriteTo(p []byte, addr net.Addr) (int, error) {
	n, err := c.WritePacketBatchCount([]*buf.Buffer{buf.As(p)}, []M.Socksaddr{M.SocksaddrFromNet(addr)})
	if n == 1 {
		return len(p), err
	}
	return 0, err
}
func (c *StreamEncoder) Write(p []byte) (int, error)              { return c.WriteTo(p, c.options.Destination) }
func (c *StreamEncoder) ReadFrom(p []byte) (int, net.Addr, error) { return c.reader.ReadFrom(p) }
func (c *StreamEncoder) ReadPacket(buffer *buf.Buffer) (M.Socksaddr, error) {
	return c.reader.readPacket(buffer)
}
func (c *StreamEncoder) Close() error                     { return c.state.Close(c.reader) }
func (c *StreamEncoder) Upstream() any                    { return c.reader.EnhancePacketConn }
func (c *StreamEncoder) ReaderReplaceable() bool          { return true }
func (c *StreamEncoder) WriterReplaceable() bool          { return false }
func (c *StreamEncoder) NeedAdditionalReadDeadline() bool { return true }

// Only synchronous codec writes access capture state, under StreamEncoder's
// connection-wide lock. Constructor writes and reads use the live transport.
type streamSink struct {
	net.Conn
	active      bool
	records     []*buf.Buffer
	size        int64
	current     *buf.Buffer
	transferred bool
}

func (c *streamSink) capture(buffer *buf.Buffer) error {
	// Bound a malicious/incorrect encoder independently of its input length.
	if c.size+int64(buffer.Len()) > 8<<20 || len(c.records) >= MaxPackets*16 {
		buffer.Release()
		return io.ErrShortBuffer
	}
	c.records = append(c.records, buffer)
	c.size += int64(buffer.Len())
	if buffer == c.current {
		c.transferred = true
	}
	return nil
}
func (c *streamSink) Write(p []byte) (int, error) {
	if !c.active {
		n, err := c.Conn.Write(p)
		if n < 0 || n > len(p) {
			return 0, io.ErrShortWrite
		}
		if n != len(p) && err == nil {
			err = io.ErrShortWrite
		}
		return n, err
	}
	buffer := buf.NewSize(len(p))
	_, _ = buffer.Write(p)
	if err := c.capture(buffer); err != nil {
		return 0, err
	}
	return len(p), nil
}
func (c *streamSink) WriteBuffer(buffer *buf.Buffer) error {
	if c.active {
		return c.capture(buffer)
	}
	defer buffer.Release()
	_, err := c.Write(buffer.Bytes())
	return err
}
func (c *streamSink) WriteVectorised(buffers []*buf.Buffer) error {
	_, err := c.WriteVectorisedCount(buffers)
	return err
}
func (c *streamSink) WriteVectorisedCount(buffers []*buf.Buffer) (int64, error) {
	if !c.active {
		return bufio.NewVectorisedWriterWithCount(c.Conn).WriteVectorisedCount(buffers)
	}
	var n int64
	for i, b := range buffers {
		size := b.Len()
		if err := c.capture(b); err != nil {
			buf.ReleaseMulti(buffers[i+1:])
			return n, err
		}
		n += int64(size)
	}
	return n, nil
}
func (c *streamSink) release()                { buf.ReleaseMulti(c.records); c.reset() }
func (c *streamSink) reset()                  { clear(c.records); c.records = c.records[:0]; c.size = 0 }
func (c *streamSink) Upstream() any           { return c.Conn }
func (c *streamSink) ReaderReplaceable() bool { return true }
func (c *streamSink) WriterReplaceable() bool { return false }

func (c *StreamEncoder) WaitReadFrom() ([]byte, func(), net.Addr, error) {
	return c.reader.WaitReadFrom()
}
func (c *StreamEncoder) Read(p []byte) (int, error) { n, _, err := c.ReadFrom(p); return n, err }

func (c *StreamEncoder) UpstreamWriter() any { return c.packets }
func (c *streamSink) UpstreamWriter() any    { return nil }

// NewPacketStreamEncoder is the packet-only form for framed stream codecs
// whose public wrapper intentionally exposes only net.PacketConn.
func NewPacketStreamEncoder(transport net.Conn, build func(net.Conn) net.PacketConn, options StreamEncoderOptions) *StreamEncoder {
	sink := &streamSink{Conn: transport}
	codec := build(sink)
	return &StreamEncoder{Conn: transport, reader: newPacketReader(codec), packets: newPacketCodec(codec), sink: sink, options: options, headroom: N.NewReadWaitOptions(nil, codec)}
}
