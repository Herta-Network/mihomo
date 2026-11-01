package snell

import (
	"bytes"
	"fmt"
	"io"
	"net"
	"strings"
	"testing"

	"github.com/metacubex/sing/common/buf"
	M "github.com/metacubex/sing/common/metadata"
)

type packetMemoryConn struct {
	net.Conn
	bytes.Buffer
}

func (c *packetMemoryConn) Read(p []byte) (int, error)  { return c.Buffer.Read(p) }
func (c *packetMemoryConn) Write(p []byte) (int, error) { return c.Buffer.Write(p) }
func (*packetMemoryConn) Close() error                  { return nil }

func TestPacketBatchV3OwnedFrames(t *testing.T) {
	for _, destination := range []M.Socksaddr{
		M.ParseSocksaddr("127.0.0.1:53"),
		M.ParseSocksaddr("[::1]:443"),
		M.ParseSocksaddr("[::ffff:127.0.0.1]:53"),
		M.ParseSocksaddrHostPort(strings.Repeat(strings.Repeat("a", 63)+".", 3)+strings.Repeat("a", 61), 5353),
	} {
		for _, sufficient := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/headroom=%v", destination, sufficient), func(t *testing.T) {
				wire := &packetMemoryConn{}
				psk := []byte("owned-packet-test")
				conn := PacketConn(StreamConn(wire, psk, Version3)).(*packetConn)
				header, _ := packetHeaderLength(destination)
				payloads := [][]byte{nil, []byte("payload"), bytes.Repeat([]byte{7}, maxLength-header)}
				buffers := make([]*buf.Buffer, len(payloads))
				destinations := make([]M.Socksaddr, len(payloads))
				original := make([]*buf.Buffer, len(payloads))
				for i, payload := range payloads {
					front, rear := 0, 0
					if sufficient {
						front, rear = conn.FrontHeadroom(), conn.RearHeadroom()
					}
					buffers[i] = buf.NewSize(front + len(payload) + rear)
					buffers[i].Resize(front, 0)
					_, _ = buffers[i].Write(payload)
					original[i] = buffers[i]
					destinations[i] = destination
				}
				if n, err := conn.WritePacketBatchCount(buffers, destinations); n != len(buffers) || err != nil {
					t.Fatalf("n=%d err=%v", n, err)
				}
				for i := range buffers {
					if original[i].RawCap() != 0 || buffers[i].RawCap() != 0 {
						t.Fatal("owned buffer retained")
					}
					if sufficient && buffers[i] != original[i] {
						t.Fatal("sufficient headroom did not use original buffer")
					}
				}
				reader := ServerStreamConn(wire, psk, Version3)
				frame := make([]byte, maxLength)
				for _, want := range payloads {
					n, err := reader.Read(frame)
					if err != nil {
						t.Fatal(err)
					}
					request, err := ParseUDPRequest(frame[:n])
					if err != nil || !bytes.Equal(request.Payload, want) || request.Port != destination.Port {
						t.Fatalf("decoded request: %+v err=%v", request, err)
					}
					if request.Host != destination.Fqdn || (!destination.IsFqdn() && request.Ip != destination.Addr.Unmap()) {
						t.Fatalf("destination changed: %+v", request)
					}
				}
			})
		}
	}
}

func TestPacketBatchV3ValidationDoesNotAdvanceCipher(t *testing.T) {
	wire := &packetMemoryConn{}
	psk := []byte("owned-packet-test")
	conn := PacketConn(StreamConn(wire, psk, Version3)).(*packetConn)
	destination := M.ParseSocksaddr("127.0.0.1:53")
	for _, invalid := range []M.Socksaddr{{}, M.ParseSocksaddrHostPort(strings.Repeat("a", 256), 53)} {
		buffers := []*buf.Buffer{buf.NewSize(1), buf.NewSize(1)}
		if n, err := conn.WritePacketBatchCount(buffers, []M.Socksaddr{destination, invalid}); n != 0 || err == nil || wire.Len() != 0 {
			t.Fatalf("invalid batch changed wire: n=%d err=%v size=%d", n, err, wire.Len())
		}
		for _, b := range buffers {
			if b.RawCap() != 0 {
				t.Fatal("invalid batch retained buffer")
			}
		}
	}
	// Plain WriteTo remains borrowed: framing/encryption cannot mutate payload.
	payload := []byte("plain payload")
	if n, err := conn.WriteTo(payload, destination.UDPAddr()); n != len(payload) || err != nil || string(payload) != "plain payload" {
		t.Fatalf("scalar packet n=%d err=%v payload=%q", n, err, payload)
	}
	reader := ServerStreamConn(wire, psk, Version3)
	frame := make([]byte, maxLength)
	n, err := reader.Read(frame)
	if err != nil {
		t.Fatal(err)
	}
	request, err := ParseUDPRequest(frame[:n])
	if err != nil || !bytes.Equal(request.Payload, payload) {
		t.Fatalf("nonce changed by invalid batch: err=%v payload=%q", err, request.Payload)
	}
}

type discardPacketConn struct{ net.Conn }

func (*discardPacketConn) Write(p []byte) (int, error) { return len(p), nil }
func (*discardPacketConn) Close() error                { return nil }

// Includes address framing, real AES-GCM and the joined-write fallback. The
// owned input buffers are reused, as they would be by an upstream buffer owner.
func BenchmarkSnellV3OwnedEncoding(b *testing.B) {
	for _, size := range []int{64, 1200} {
		b.Run(fmt.Sprint(size), func(b *testing.B) {
			conn := PacketConn(StreamConn(&discardPacketConn{}, []byte("benchmark-psk"), Version3)).(*packetConn)
			defer conn.Close()
			var buffers [4]*buf.Buffer
			var destinations [4]M.Socksaddr
			for i := range buffers {
				buffers[i] = buf.With(make([]byte, conn.FrontHeadroom()+size+conn.RearHeadroom()))
				destinations[i] = M.ParseSocksaddr("127.0.0.1:53")
			}
			write := func() {
				for _, buffer := range buffers {
					buffer.Resize(conn.FrontHeadroom(), size)
				}
				if n, err := conn.WritePacketBatchCount(buffers[:], destinations[:]); n != len(buffers) || err != nil {
					b.Fatalf("n=%d err=%v", n, err)
				}
			}
			write() // lazy salt, cipher and pool warm-up
			b.ReportAllocs()
			b.SetBytes(int64(len(buffers) * size))
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				write()
			}
		})
	}
}

var _ io.ReadWriter = (*packetMemoryConn)(nil)
