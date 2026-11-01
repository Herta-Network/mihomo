package outbound

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"testing"
	"time"

	"github.com/metacubex/mihomo/common/net/packetbatch"

	N "github.com/metacubex/mihomo/common/net"
	C "github.com/metacubex/mihomo/constant"
	"github.com/metacubex/mihomo/transport/shadowsocks/core"
	"github.com/metacubex/mihomo/transport/socks5"
	"github.com/metacubex/sing/common/buf"
	M "github.com/metacubex/sing/common/metadata"
)

func TestProtocolPacketBatchDirectThroughLoopbackGuard(t *testing.T) {
	peer, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	defer peer.Close()
	peer.SetReadDeadline(time.Now().Add(3 * time.Second))
	destination := M.SocksaddrFromNet(peer.LocalAddr())
	out := NewDirect()
	defer out.Close()
	conn, err := out.ListenPacketContext(context.Background(), &C.Metadata{NetWork: C.UDP, DstIP: destination.Addr, DstPort: destination.Port})
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	writer, ok := N.CreatePacketBatchWriterWithCount(conn)
	if !ok {
		t.Fatal("loopback callback hid native capability")
	}
	bs := []*buf.Buffer{buf.As(nil), buf.As([]byte("one")), buf.As([]byte("two"))}
	n, err := writer.WritePacketBatchCount(bs, []M.Socksaddr{destination, destination, destination})
	if err != nil || n != 3 {
		t.Fatalf("n=%d err=%v", n, err)
	}
	for _, want := range []string{"", "one", "two"} {
		var p [32]byte
		n, _, err := peer.ReadFrom(p[:])
		if err != nil || string(p[:n]) != want {
			t.Fatalf("payload=%q err=%v", p[:n], err)
		}
	}
}
func TestProtocolPacketBatchSSRRetainsCipherAndAddress(t *testing.T) {
	peer, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	defer peer.Close()
	peer.SetReadDeadline(time.Now().Add(3 * time.Second))
	relay := M.SocksaddrFromNet(peer.LocalAddr())
	cipher, err := core.PickCipher("aes-128-cfb", nil, "packet-batch-secret")
	if err != nil {
		t.Fatal(err)
	}
	reader := cipher.PacketConn(N.NewEnhancePacketConn(peer))
	out, err := NewShadowSocksR(ShadowSocksROption{Name: "batch", Server: relay.Addr.String(), Port: int(relay.Port), Cipher: "aes-128-cfb", Password: "packet-batch-secret", Protocol: "origin", Obfs: "plain", UDP: true})
	if err != nil {
		t.Fatal(err)
	}
	defer out.Close()
	destination := M.ParseSocksaddr("127.0.0.9:5353")
	conn, err := out.ListenPacketContext(context.Background(), &C.Metadata{NetWork: C.UDP, DstIP: destination.Addr, DstPort: destination.Port})
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	writer, ok := N.CreatePacketBatchWriterWithCount(conn)
	if !ok {
		t.Fatal("SSR batch capability absent")
	}
	payloads := [][]byte{nil, []byte("one"), bytes.Repeat([]byte{0x33}, 4096)}
	bs := make([]*buf.Buffer, len(payloads))
	ds := make([]M.Socksaddr, len(payloads))
	for i, p := range payloads {
		bs[i] = buf.As(p)
		ds[i] = destination
		if i == 1 {
			ds[i] = M.ParseSocksaddr("[::1]:5354")
		}
	}
	n, err := writer.WritePacketBatchCount(bs, ds)
	if err != nil || n != 3 {
		t.Fatalf("n=%d err=%v", n, err)
	}
	for i, want := range payloads {
		p := make([]byte, 65535)
		n, _, err := reader.ReadFrom(p)
		if err != nil {
			t.Fatal(err)
		}
		addr := socks5.SplitAddr(p[:n])
		if addr == nil || addr.String() != ds[i].String() || !bytes.Equal(p[len(addr):n], want) {
			t.Fatal("SSR protocol/cipher/address was bypassed")
		}
	}
}

func TestProtocolPacketBatchMieruFraming(t *testing.T) {
	left, right := net.Pipe()
	defer left.Close()
	defer right.Close()
	left.SetDeadline(time.Now().Add(3 * time.Second))
	right.SetDeadline(time.Now().Add(3 * time.Second))
	destination := M.ParseSocksaddr("127.0.0.1:5353")
	codec := packetbatch.NewPacketStreamEncoder(left, newMieruPacketConn, packetbatch.StreamEncoderOptions{Destination: destination})
	peer := newMieruPacketConn(right)
	payloads := [][]byte{nil, []byte("one"), bytes.Repeat([]byte{3}, 60000)}
	bs := make([]*buf.Buffer, len(payloads))
	ds := make([]M.Socksaddr, len(payloads))
	for i, p := range payloads {
		bs[i] = buf.As(p)
		ds[i] = destination
	}
	ds[1] = M.ParseSocksaddr("[::1]:5354")
	done := make(chan error, 1)
	go func() {
		for i, want := range payloads {
			p := make([]byte, 65535)
			n, addr, err := peer.ReadFrom(p)
			if err != nil {
				done <- err
				return
			}
			if !bytes.Equal(p[:n], want) || addr.String() != ds[i].String() {
				done <- errors.New("Mieru payload or destination changed")
				return
			}
		}
		done <- nil
	}()
	n, err := codec.WritePacketBatchCount(bs, ds)
	if n != 3 || err != nil {
		t.Fatalf("n=%d err=%v", n, err)
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

func TestProtocolPacketMieruEmptyAndEOF(t *testing.T) {
	left, right := net.Pipe()
	defer left.Close()
	defer right.Close()
	left.SetDeadline(time.Now().Add(3 * time.Second))
	right.SetDeadline(time.Now().Add(3 * time.Second))
	reader := newMieruPacketConn(left)
	writer := newMieruPacketConn(right)
	target := M.ParseSocksaddr("127.0.0.1:5353")
	done := make(chan error, 1)
	go func() { _, err := writer.WriteTo(nil, target); writer.Close(); done <- err }()
	p := make([]byte, 1)
	n, addr, err := reader.ReadFrom(p)
	if err != nil || n != 0 || addr.String() != target.String() {
		t.Fatalf("empty packet n=%d addr=%v err=%v", n, addr, err)
	}
	if _, _, err = reader.ReadFrom(p); !errors.Is(err, io.EOF) {
		t.Fatalf("transport EOF became a packet: %v", err)
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

type nativeDeadlinePacket struct {
	net.PacketConn
	deadlineCalls int
}

func (*nativeDeadlinePacket) ReadWithBuffer(func(int) []byte) (int, error) { return 0, io.EOF }
func (c *nativeDeadlinePacket) SetReadDeadline(time.Time) error            { c.deadlineCalls++; return nil }
func (*nativeDeadlinePacket) Close() error                                 { return nil }
func TestProtocolPacketProgressPreservesStackDeadline(t *testing.T) {
	raw := &nativeDeadlinePacket{}
	out := NewDirect()
	conn := NewPacketConn(withPacketProgress(raw, false), out)
	if err := conn.SetReadDeadline(time.Now().Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	if raw.deadlineCalls != 1 {
		t.Fatal("write adapter replaced the stack's native read deadline")
	}
	conn.Close()
}
