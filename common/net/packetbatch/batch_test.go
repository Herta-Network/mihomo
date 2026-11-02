package packetbatch

import (
	"encoding/binary"
	"errors"
	"io"
	"net"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/metacubex/sing/common/buf"
	"github.com/metacubex/sing/common/bufio"
	M "github.com/metacubex/sing/common/metadata"
	N "github.com/metacubex/sing/common/network"
)

var target = M.ParseSocksaddr("127.0.0.1:5353")

func inputs(payloads ...string) ([]*buf.Buffer, []M.Socksaddr) {
	bs := make([]*buf.Buffer, len(payloads))
	ds := make([]M.Socksaddr, len(payloads))
	for i, p := range payloads {
		bs[i] = buf.NewSize(max(1, len(p)))
		_, _ = bs[i].WriteString(p)
		ds[i] = target
	}
	return bs, ds
}
func released(t *testing.T, bs []*buf.Buffer) {
	t.Helper()
	for _, b := range bs {
		if b.RawCap() != 0 {
			t.Fatal("owned input retained")
		}
	}
}

type wire struct {
	net.Conn
	limit   int64
	failure error
	calls   int
	closed  bool
	sizes   []int
}

func (w *wire) Write(p []byte) (int, error) { return len(p), nil }
func (w *wire) Close() error                { w.closed = true; return nil }
func (w *wire) WriteVectorisedCount(bs []*buf.Buffer) (int64, error) {
	defer buf.ReleaseMulti(bs)
	w.calls++
	w.sizes = append(w.sizes, len(bs))
	var n int64
	for _, b := range bs {
		n += int64(b.Len())
	}
	if w.limit >= 0 {
		n = min(n, w.limit)
	}
	return n, w.failure
}
func TestPacketStreamCompletePrefix(t *testing.T) {
	for _, tc := range []struct {
		limit   int64
		want    int
		failure error
	}{{0, 0, io.ErrClosedPipe}, {2, 1, io.ErrClosedPipe}, {3, 1, nil}, {5, 2, io.ErrClosedPipe}, {9, 3, io.ErrClosedPipe}} {
		raw := &wire{limit: tc.limit, failure: tc.failure}
		state := &State{}
		bs, ds := inputs("", "a", "bc")
		n, err := state.WriteStream(raw, raw, bs, ds, PrepareLengthPacket)
		wantErr := tc.failure
		if wantErr == nil {
			wantErr = io.ErrShortWrite
		}
		if n != tc.want || !errors.Is(err, wantErr) || !raw.closed {
			t.Fatalf("limit=%d n=%d err=%v", tc.limit, n, err)
		}
		released(t, bs)
		retry, targets := inputs("retry")
		n, err = state.WriteStream(raw, raw, retry, targets, PrepareLengthPacket)
		if n != 0 || err == nil || raw.calls != 1 {
			t.Fatal("failed stream was replayed")
		}
		released(t, retry)
	}
}
func TestPacketStreamChunkBoundAndValidation(t *testing.T) {
	raw := &wire{limit: -1}
	state := &State{}
	payloads := make([]string, 70)
	bs, ds := inputs(payloads...)
	n, err := state.WriteStream(raw, raw, bs, ds, PrepareLengthPacket)
	if err != nil || n != 70 || len(raw.sizes) != 2 || raw.sizes[0] != 64 || raw.sizes[1] != 6 {
		t.Fatalf("n=%d sizes=%v err=%v", n, raw.sizes, err)
	}
	released(t, bs)
	bs, ds = inputs("small", string(make([]byte, 65536)))
	n, err = state.WriteStream(raw, raw, bs, ds, PrepareLengthPacket)
	if n != 0 || !errors.Is(err, io.ErrShortBuffer) || raw.calls != 2 || raw.closed {
		t.Fatalf("invalid batch n=%d err=%v", n, err)
	}
	released(t, bs)
	bs, ds = inputs("after")
	n, err = state.WriteStream(raw, raw, bs, ds, PrepareLengthPacket)
	if n != 1 || err != nil {
		t.Fatal("validation poisoned stream", err)
	}
}

type packetWire struct {
	net.PacketConn
	prefix  int
	failure error
	calls   int
	scalar  int
	frames  []string
	targets []M.Socksaddr
	closed  bool
}

func (w *packetWire) WriteTo(p []byte, _ net.Addr) (int, error) { w.scalar++; return len(p), nil }
func (w *packetWire) Close() error                              { w.closed = true; return nil }
func (w *packetWire) WritePacketBatchCount(bs []*buf.Buffer, ds []M.Socksaddr) (int, error) {
	defer buf.ReleaseMulti(bs)
	w.calls++
	for i, b := range bs {
		w.frames = append(w.frames, string(b.Bytes()))
		w.targets = append(w.targets, ds[i])
	}
	if w.prefix >= 0 {
		return min(w.prefix, len(bs)), w.failure
	}
	return len(bs), w.failure
}

type datagramCodec struct {
	net.PacketConn
	copy     bool
	sequence byte
}

func (c *datagramCodec) FrontHeadroom() int                          { return 3 }
func (c *datagramCodec) ReadPacket(*buf.Buffer) (M.Socksaddr, error) { return M.Socksaddr{}, io.EOF }
func (c *datagramCodec) WritePacket(b *buf.Buffer, d M.Socksaddr) error {
	defer b.Release()
	c.sequence++
	if string(b.Bytes()) == "bad" {
		return os.ErrInvalid
	}
	var p []byte
	if c.copy {
		p = make([]byte, 3+b.Len())
		copy(p[3:], b.Bytes())
	} else {
		b.ExtendHeader(3)
		p = b.Bytes()
	}
	p[0] = c.sequence
	binary.BigEndian.PutUint16(p[1:], d.Port)
	_, err := c.PacketConn.WriteTo(p, net.UDPAddrFromAddrPort(M.ParseSocksaddr("127.0.0.2:9999").AddrPort()))
	return err
}
func (c *datagramCodec) WriteTo(p []byte, addr net.Addr) (int, error) {
	b := buf.NewSize(3 + len(p))
	b.Resize(3, 0)
	_, _ = b.Write(p)
	err := c.WritePacket(b, M.SocksaddrFromNet(addr))
	if err != nil {
		return 0, err
	}
	return len(p), nil
}
func TestPacketDatagramEncoderPreservesCodec(t *testing.T) {
	for _, copyMode := range []bool{false, true} {
		raw := &packetWire{prefix: -1}
		conn := NewDatagramEncoder(raw, func(sink net.PacketConn) net.PacketConn { return &datagramCodec{PacketConn: sink, copy: copyMode} })
		writer, ok := bufio.CreatePacketBatchWriterWithCount(conn.(N.PacketWriter))
		if !ok {
			t.Fatal("lost count capability")
		}
		bs, ds := inputs("one", "", "three")
		ds[2].Port = 5354
		n, err := writer.WritePacketBatchCount(bs, ds)
		if n != 3 || err != nil || raw.calls != 1 || raw.scalar != 0 {
			t.Fatalf("n=%d err=%v", n, err)
		}
		for i, payload := range []string{"one", "", "three"} {
			frame := []byte(raw.frames[i])
			if frame[0] != byte(i+1) || binary.BigEndian.Uint16(frame[1:]) != ds[i].Port || string(frame[3:]) != payload || raw.targets[i].Port != 9999 {
				t.Fatal("codec or relay address bypassed")
			}
		}
		released(t, bs)
		raw.prefix = 1
		raw.failure = io.ErrClosedPipe
		bs, ds = inputs("first", "second")
		n, err = writer.WritePacketBatchCount(bs, ds)
		if n != 1 || !errors.Is(err, io.ErrClosedPipe) || raw.closed {
			t.Fatalf("UDP prefix=%d err=%v", n, err)
		}
		released(t, bs)
		bs, ds = inputs("valid", "bad")
		before := raw.calls
		n, err = writer.WritePacketBatchCount(bs, ds)
		if n != 0 || err == nil || raw.calls != before {
			t.Fatal("unvalidated cipher batch reached socket")
		}
		released(t, bs)
	}
}

type framedCodec struct {
	net.Conn
	started bool
	writes  int
	fail    bool
}

func (c *framedCodec) FrontHeadroom() int { return 2 }
func (c *framedCodec) WritePacket(b *buf.Buffer, _ M.Socksaddr) error {
	c.writes++
	if !c.started {
		c.started = true
		if _, err := io.WriteString(c.Conn, "req"); err != nil {
			b.Release()
			return err
		}
	}
	size := b.Len()
	binary.BigEndian.PutUint16(b.ExtendHeader(2), uint16(size))
	err := bufio.NewExtendedWriter(c.Conn).WriteBuffer(b)
	if err == nil && c.fail {
		return os.ErrInvalid
	}
	return err
}
func (c *framedCodec) ReadPacket(*buf.Buffer) (M.Socksaddr, error) { return M.Socksaddr{}, io.EOF }
func (c *framedCodec) ReadFrom([]byte) (int, net.Addr, error)      { return 0, nil, io.EOF }
func (c *framedCodec) WriteTo(p []byte, a net.Addr) (int, error) {
	b := buf.NewSize(2 + len(p))
	b.Resize(2, 0)
	_, _ = b.Write(p)
	if err := c.WritePacket(b, M.SocksaddrFromNet(a)); err != nil {
		return 0, err
	}
	return len(p), nil
}
func TestPacketEncodedStreamCountsLazyHeader(t *testing.T) {
	for _, tc := range []struct {
		limit   int64
		want    int
		failure error
	}{{2, 0, io.ErrClosedPipe}, {5, 1, io.ErrClosedPipe}, {6, 1, nil}, {8, 2, io.ErrClosedPipe}, {12, 3, io.ErrClosedPipe}} {
		raw := &wire{limit: tc.limit, failure: tc.failure}
		var codec *framedCodec
		conn, err := NewStreamEncoder(raw, func(c net.Conn) (net.Conn, error) { codec = &framedCodec{Conn: c}; return codec, nil }, StreamEncoderOptions{Destination: target})
		if err != nil {
			t.Fatal(err)
		}
		bs, ds := inputs("", "a", "bc")
		n, err := conn.WritePacketBatchCount(bs, ds)
		wantErr := tc.failure
		if wantErr == nil {
			wantErr = io.ErrShortWrite
		}
		if n != tc.want || !errors.Is(err, wantErr) || !raw.closed {
			t.Fatalf("limit=%d n=%d err=%v", tc.limit, n, err)
		}
		released(t, bs)
		if _, err = conn.Write([]byte("retry")); err == nil || codec.writes != 3 || raw.calls != 1 {
			t.Fatal("retry bypassed terminal state")
		}
	}
}
func TestPacketEncodedStreamValidationAndConcurrentWriters(t *testing.T) {
	raw := &wire{limit: -1}
	var codec *framedCodec
	conn, err := NewStreamEncoder(raw, func(c net.Conn) (net.Conn, error) { codec = &framedCodec{Conn: c}; return codec, nil }, StreamEncoderOptions{Destination: target, Validate: func(b *buf.Buffer, _ M.Socksaddr) error {
		if string(b.Bytes()) == "bad" {
			return os.ErrInvalid
		}
		return nil
	}})
	if err != nil {
		t.Fatal(err)
	}
	bs, ds := inputs("valid", "bad")
	n, err := conn.WritePacketBatchCount(bs, ds)
	if n != 0 || err == nil || codec.writes != 0 || raw.calls != 0 {
		t.Fatal("validation advanced encoder")
	}
	released(t, bs)
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			if i%2 == 0 {
				_, err := conn.WriteTo([]byte("scalar"), target)
				if err != nil {
					t.Error(err)
				}
			} else {
				bs, ds := inputs("a", "b")
				n, err := conn.WritePacketBatchCount(bs, ds)
				if n != 2 || err != nil {
					t.Errorf("n=%d err=%v", n, err)
				}
			}
		}(i)
	}
	wg.Wait()
	if codec.writes != 12 || raw.calls != 8 {
		t.Fatalf("writes=%d batches=%d", codec.writes, raw.calls)
	}
}
func TestPacketEncodedStreamOwnsBuffersOnEncoderError(t *testing.T) {
	raw := &wire{limit: -1}
	conn, err := NewStreamEncoder(raw, func(c net.Conn) (net.Conn, error) { return &framedCodec{Conn: c, fail: true}, nil }, StreamEncoderOptions{Destination: target})
	if err != nil {
		t.Fatal(err)
	}
	bs, ds := inputs("a", "b")
	n, err := conn.WritePacketBatchCount(bs, ds)
	if n != 0 || err == nil || !raw.closed || raw.calls != 0 {
		t.Fatalf("n=%d err=%v", n, err)
	}
	released(t, bs)
}

type blockingWire struct {
	net.Conn
	started chan struct{}
	closed  chan struct{}
	once    sync.Once
}

func (w *blockingWire) Write(p []byte) (int, error) { return len(p), nil }
func (w *blockingWire) WriteVectorisedCount(bs []*buf.Buffer) (int64, error) {
	defer buf.ReleaseMulti(bs)
	close(w.started)
	<-w.closed
	return 0, net.ErrClosed
}
func (w *blockingWire) Close() error { w.once.Do(func() { close(w.closed) }); return nil }
func TestPacketEncodedStreamCloseUnblocksWriter(t *testing.T) {
	raw := &blockingWire{started: make(chan struct{}), closed: make(chan struct{})}
	conn, err := NewStreamEncoder(raw, func(c net.Conn) (net.Conn, error) { return &framedCodec{Conn: c}, nil }, StreamEncoderOptions{Destination: target})
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	bs, ds := inputs("blocked")
	done := make(chan error, 1)
	go func() { _, err := conn.WritePacketBatchCount(bs, ds); done <- err }()
	select {
	case <-raw.started:
	case <-time.After(time.Second):
		t.Fatal("write not started")
	}
	conn.Close()
	select {
	case err := <-done:
		if !errors.Is(err, net.ErrClosed) {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("Close blocked behind writer")
	}
	released(t, bs)
}

type domainCodec struct {
	net.PacketConn
	got net.Addr
}

func (c *domainCodec) WriteTo(p []byte, addr net.Addr) (int, error) { c.got = addr; return len(p), nil }
func TestPacketCodecRetainsDomainDestination(t *testing.T) {
	raw := &domainCodec{}
	codec := newPacketCodec(raw)
	destination := M.ParseSocksaddr("example.org:5353")
	b := buf.NewSize(1)
	if err := codec.WritePacket(b, destination); err != nil || raw.got.String() != destination.String() {
		t.Fatalf("addr=%v err=%v", raw.got, err)
	}
	if b.RawCap() != 0 {
		t.Fatal("buffer retained")
	}
}
