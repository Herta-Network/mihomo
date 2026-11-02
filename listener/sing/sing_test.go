package sing

import (
	"context"
	"errors"
	"io"
	"net"
	"sync"
	"testing"
	"time"

	C "github.com/metacubex/mihomo/constant"
	"github.com/metacubex/sing/common/buf"
	M "github.com/metacubex/sing/common/metadata"
	"github.com/metacubex/sing/common/network"
)

type blockingPacketConn struct {
	network.PacketConn
	writeStarted chan struct{}
	closed       chan struct{}
	once         sync.Once
	reads        int
}

func (c *blockingPacketConn) ReadPacket(b *buf.Buffer) (M.Socksaddr, error) {
	c.reads++
	if c.reads == 1 {
		b.WriteString("request")
		return M.ParseSocksaddr("127.0.0.1:53"), nil
	}
	<-c.writeStarted
	return M.Socksaddr{}, io.EOF
}
func (c *blockingPacketConn) WritePacket(b *buf.Buffer, _ M.Socksaddr) error {
	defer b.Release()
	close(c.writeStarted)
	<-c.closed
	return net.ErrClosed
}
func (c *blockingPacketConn) Close() error { c.once.Do(func() { close(c.closed) }); return nil }
func (c *blockingPacketConn) LocalAddr() net.Addr {
	return &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 1234}
}

type packetTestTunnel struct {
	C.Tunnel
	received chan C.UDPPacket
	written  chan error
	owned    bool
}

func (t *packetTestTunnel) HandleUDPPacket(p C.UDPPacket, _ *C.Metadata) {
	t.received <- p
	go func() {
		var err error
		if t.owned {
			_, err = C.GetWriteBackTarget(p).WriteBack([]byte("response"), M.ParseSocksaddr("127.0.0.1:53"))
		} else {
			_, err = p.WriteBack([]byte("response"), M.ParseSocksaddr("127.0.0.1:53"))
		}
		p.Drop()
		t.written <- err
	}()
}

func TestPacketConnectionCloseUnblocksWriteBack(t *testing.T) {
	testPacketConnectionCloseUnblocksWriteBack(t, false)
}

func TestPacketDetachedConnectionCloseUnblocksWriteBack(t *testing.T) {
	testPacketConnectionCloseUnblocksWriteBack(t, true)
}

func testPacketConnectionCloseUnblocksWriteBack(t *testing.T, owned bool) {
	conn := &blockingPacketConn{writeStarted: make(chan struct{}), closed: make(chan struct{})}
	defer conn.Close()
	tunnel := &packetTestTunnel{received: make(chan C.UDPPacket, 1), written: make(chan error, 1), owned: owned}
	handler := &ListenerHandler{ListenerConfig: ListenerConfig{Tunnel: tunnel, Type: C.SOCKS5}}
	done := make(chan error, 1)
	go func() {
		done <- handler.NewPacketConnection(context.Background(), conn, M.Metadata{Source: M.ParseSocksaddr("127.0.0.1:1000")})
	}()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("close waited for blocked WriteBack")
	}
	select {
	case err := <-tunnel.written:
		if !errors.Is(err, net.ErrClosed) {
			t.Fatal(err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("WriteBack did not stop")
	}
	packet := <-tunnel.received
	if _, err := packet.WriteBack(nil, &net.UDPAddr{}); err == nil {
		t.Fatal("stale packet writer still usable")
	}
}

type failedReplyWriter struct{ network.NetPacketWriter }

func (*failedReplyWriter) WriteTo([]byte, net.Addr) (int, error) { return 0, io.ErrClosedPipe }
func TestPacketReplyLifetimeAndFailure(t *testing.T) {
	for _, closed := range []bool{false, true} {
		reply := &packetReply{writer: &failedReplyWriter{}}
		p := &packet{reply: reply, buff: buf.NewSize(64), rAddr: &net.UDPAddr{}}
		target := C.GetWriteBackTarget(C.NewPacketAdapter(p, &C.Metadata{}))
		if target != reply {
			t.Fatal("reply retained packet envelope")
		}
		p.Drop()
		if closed {
			reply.close()
		}
		_, err := target.WriteBack([]byte("reply"), M.ParseSocksaddr("127.0.0.2:5353"))
		want := io.ErrClosedPipe
		if closed {
			want = net.ErrClosed
		}
		if !errors.Is(err, want) {
			t.Fatal(err)
		}
	}
}

func TestPacketTakeBufferKeepsReplyEndpoint(t *testing.T) {
	buffer := buf.NewSize(64)
	buffer.Resize(16, 0)
	buffer.WriteString("request")
	conn := &ownershipReplyConn{}
	p := &packet{buff: buffer, reply: &packetReply{writer: conn}}
	if taken := p.TakeBuffer(); taken != buffer || taken.Start() != 16 {
		t.Fatal("transfer lost the buffer or its headroom")
	}
	p.Drop()
	if string(buffer.Bytes()) != "request" || p.TakeBuffer() != nil {
		t.Fatal("Drop released transferred storage or repeated transfer succeeded")
	}
	addr := &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 53}
	if n, err := p.WriteBack([]byte("reply"), addr); err != nil || n != 5 || conn.writes != 1 {
		t.Fatalf("reply endpoint invalid after payload transfer: n=%d err=%v", n, err)
	}
	buffer.Release()
}

type ownershipReplyConn struct {
	network.NetPacketWriter
	writes int
}

func (c *ownershipReplyConn) WriteTo(b []byte, _ net.Addr) (int, error) {
	c.writes++
	return len(b), nil
}
