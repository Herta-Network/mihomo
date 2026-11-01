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
}

func (t *packetTestTunnel) HandleUDPPacket(p C.UDPPacket, _ *C.Metadata) {
	t.received <- p
	go func() {
		_, err := p.WriteBack([]byte("response"), &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 53})
		p.Drop()
		t.written <- err
	}()
}

func TestPacketConnectionCloseUnblocksWriteBack(t *testing.T) {
	conn := &blockingPacketConn{writeStarted: make(chan struct{}), closed: make(chan struct{})}
	defer conn.Close()
	tunnel := &packetTestTunnel{received: make(chan C.UDPPacket, 1), written: make(chan error, 1)}
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
