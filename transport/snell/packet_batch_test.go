package snell

import (
	"errors"
	"io"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/metacubex/sing/common/buf"
	M "github.com/metacubex/sing/common/metadata"
)

type v3BatchFailureConn struct {
	net.Conn
	prefix    int
	calls     int
	closed    int
	fullFirst bool
	sizes     []int
}

func (c *v3BatchFailureConn) Write(p []byte) (int, error) { return len(p), nil }
func (c *v3BatchFailureConn) Close() error                { c.closed++; return nil }
func (c *v3BatchFailureConn) WriteVectorisedCount(records []*buf.Buffer) (int64, error) {
	defer buf.ReleaseMulti(records)
	c.calls++
	c.sizes = append(c.sizes, len(records))
	var n int64
	if c.fullFirst && c.calls == 1 {
		for _, r := range records {
			n += int64(r.Len())
		}
		return n, nil
	}
	for _, r := range records[:c.prefix] {
		n += int64(r.Len())
	}
	if c.prefix < len(records) {
		n++
	}
	return n, io.ErrClosedPipe
}

func TestPacketBatchV3CountsEarlierChunks(t *testing.T) {
	raw := &v3BatchFailureConn{prefix: 2, fullFirst: true}
	conn := PacketConn(StreamConn(raw, []byte("packet-batch-test"), Version3)).(*packetConn)
	buffers := make([]*buf.Buffer, 70)
	dests := make([]M.Socksaddr, len(buffers))
	for i := range buffers {
		buffers[i] = buf.NewSize(1)
		dests[i] = M.ParseSocksaddr("127.0.0.1:53")
	}
	n, err := conn.WritePacketBatchCount(buffers, dests)
	if n != 66 || !errors.Is(err, io.ErrClosedPipe) {
		t.Fatalf("prefix=%d err=%v", n, err)
	}
	if len(raw.sizes) != 2 || raw.sizes[0] != 64 || raw.sizes[1] != 6 {
		t.Fatalf("batch sizes=%v", raw.sizes)
	}
	for _, b := range buffers {
		if b.RawCap() != 0 {
			t.Fatal("input retained")
		}
	}
}

type blockedV3BatchConn struct {
	net.Conn
	started chan struct{}
	closed  chan struct{}
	once    sync.Once
}

func (c *blockedV3BatchConn) Write(p []byte) (int, error) { return len(p), nil }
func (c *blockedV3BatchConn) Close() error                { c.once.Do(func() { close(c.closed) }); return nil }
func (c *blockedV3BatchConn) WriteVectorisedCount(records []*buf.Buffer) (int64, error) {
	defer buf.ReleaseMulti(records)
	close(c.started)
	<-c.closed
	return 0, net.ErrClosed
}
func TestPacketBatchV3CloseUnblocksWriter(t *testing.T) {
	raw := &blockedV3BatchConn{started: make(chan struct{}), closed: make(chan struct{})}
	conn := PacketConn(StreamConn(raw, []byte("packet-batch-test"), Version3)).(*packetConn)
	defer conn.Close()
	b := buf.NewSize(1)
	done := make(chan error, 1)
	go func() {
		_, err := conn.WritePacketBatchCount([]*buf.Buffer{b}, []M.Socksaddr{M.ParseSocksaddr("127.0.0.1:53")})
		done <- err
	}()
	select {
	case <-raw.started:
	case <-time.After(3 * time.Second):
		t.Fatal("write not started")
	}
	conn.Close()
	select {
	case err := <-done:
		if !errors.Is(err, net.ErrClosed) {
			t.Fatal(err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("close blocked behind writer")
	}
	if b.RawCap() != 0 {
		t.Fatal("input retained")
	}
}
func TestPacketBatchV3PartialCountStopsRetry(t *testing.T) {
	for _, prefix := range []int{0, 1, 3} {
		raw := &v3BatchFailureConn{prefix: prefix}
		conn := PacketConn(StreamConn(raw, []byte("packet-batch-test"), Version3)).(*packetConn)
		buffers := []*buf.Buffer{buf.NewSize(1), buf.NewSize(3), buf.NewSize(4)}
		buffers[1].WriteString("one")
		buffers[2].WriteString("two")
		dest := M.ParseSocksaddr("127.0.0.1:53")
		n, err := conn.WritePacketBatchCount(buffers, []M.Socksaddr{dest, dest, dest})
		if n != prefix || !errors.Is(err, io.ErrClosedPipe) || raw.closed == 0 {
			t.Fatalf("n=%d err=%v", n, err)
		}
		for _, b := range buffers {
			if b.RawCap() != 0 {
				t.Fatal("input retained")
			}
		}
		if _, err := conn.WriteTo([]byte("retry"), dest.UDPAddr()); err == nil || raw.calls != 1 {
			t.Fatal("scalar write bypassed failed batch")
		}
	}
}

type shortV3Handshake struct {
	net.Conn
	writes int
}

func (c *shortV3Handshake) Write(p []byte) (int, error) {
	c.writes++
	if c.writes == 1 {
		return len(p), nil
	}
	return 1, nil
}
func (*shortV3Handshake) Close() error { return nil }
func TestPacketV3HandshakeRejectsShortWrite(t *testing.T) {
	stream := StreamConn(&shortV3Handshake{}, []byte("packet-batch-test"), Version3)
	n, err := stream.Write([]byte("request"))
	if n != 0 || !errors.Is(err, io.ErrShortWrite) {
		t.Fatalf("n=%d err=%v", n, err)
	}
}
