package packet

import (
	"bytes"
	"errors"
	"io"
	"net"
	"testing"
	"time"

	"github.com/metacubex/mihomo/constant/features"
	"github.com/metacubex/sing/common/buf"
	"github.com/metacubex/sing/common/bufio"
	M "github.com/metacubex/sing/common/metadata"
	N "github.com/metacubex/sing/common/network"
)

type batchSource struct {
	N.NetPacketConn
	packets         []*buf.Buffer
	dests           []M.Socksaddr
	reads, releases int
	options         N.ReadWaitOptions
}

func (s *batchSource) InitializeReadWaiter(options N.ReadWaitOptions) bool {
	s.options = options
	return false
}
func (s *batchSource) WaitReadPackets() ([]*buf.Buffer, []M.Socksaddr, error) {
	s.reads++
	if s.reads > 1 {
		return nil, nil, io.EOF
	}
	return append([]*buf.Buffer(nil), s.packets...), s.dests, nil
}
func (s *batchSource) ReleaseReadWaiter() { s.releases++ }
func (s *batchSource) Close() error       { return nil }

func TestPacketReaderPendingOwnership(t *testing.T) {
	if features.WithLowMemory {
		t.Skip("batch disabled in low memory build")
	}
	first, second := buf.NewSize(32), buf.NewSize(32)
	first.WriteString("first")
	second.WriteString("pending")
	dest := M.ParseSocksaddr("example.org:53")
	source := &batchSource{packets: []*buf.Buffer{first, second}, dests: []M.Socksaddr{dest, dest}}
	conn := newEnhanceSingPacketConn(source)
	data, put, addr, err := conn.WaitReadFrom()
	if err != nil || string(data) != "first" || addr.String() != dest.String() {
		t.Fatalf("read %q %v %v", data, addr, err)
	}
	if source.options.BatchSize != 4 {
		t.Fatalf("batch=%d", source.options.BatchSize)
	}
	if conn.ReaderReplaceable() {
		t.Fatal("queued reader can be bypassed")
	}
	if err = conn.Close(); err != nil {
		t.Fatal(err)
	}
	conn.Close()
	if second.Cap() != 0 || first.Cap() == 0 || source.releases != 1 {
		t.Fatalf("ownership first=%d pending=%d releases=%d", first.Cap(), second.Cap(), source.releases)
	}
	if string(data) != "first" {
		t.Fatal("caller buffer released by close")
	}
	put()
	if first.Cap() != 0 {
		t.Fatal("caller buffer not released")
	}
	if _, _, _, err = conn.WaitReadFrom(); !errors.Is(err, net.ErrClosed) {
		t.Fatal(err)
	}
}

func TestPacketReaderMixedReadsPreserveBatch(t *testing.T) {
	if features.WithLowMemory {
		t.Skip("batch disabled in low memory build")
	}
	wrappers := map[string]func(net.PacketConn) EnhancePacketConn{
		"plain":  NewEnhancePacketConn,
		"thread": NewThreadSafePacketConn,
		"ref":    func(c net.PacketConn) EnhancePacketConn { return NewRefPacketConn(c, new(int)) },
		"ref-thread": func(c net.PacketConn) EnhancePacketConn {
			return NewRefPacketConn(NewThreadSafePacketConn(c), new(int))
		},
	}
	for name, wrap := range wrappers {
		t.Run(name, func(t *testing.T) {
			dest := M.ParseSocksaddr("127.0.0.1:53")
			source := &batchSource{dests: []M.Socksaddr{dest, dest, dest}}
			for _, s := range []string{"first", "second", "third"} {
				b := buf.NewSize(32)
				b.WriteString(s)
				source.packets = append(source.packets, b)
			}
			conn := wrap(source).(EnhanceSingPacketConn)
			defer conn.Close()
			data, put, _, err := conn.WaitReadFrom()
			if err != nil || string(data) != "first" {
				t.Fatal(err)
			}
			put()
			p := make([]byte, 32)
			n, _, err := conn.ReadFrom(p)
			if err != nil || string(p[:n]) != "second" {
				t.Fatalf("%q %v", p[:n], err)
			}
			b := buf.NewSize(32)
			defer b.Release()
			_, err = conn.ReadPacket(b)
			if err != nil || string(b.Bytes()) != "third" {
				t.Fatalf("%q %v", b.Bytes(), err)
			}
			if source.reads != 1 {
				t.Fatalf("reads=%d", source.reads)
			}
		})
	}
}

func TestPacketReaderNativeDatagrams(t *testing.T) {
	for _, name := range []string{"udp", "sing", "opaque"} {
		t.Run(name, func(t *testing.T) {
			input, err := net.ListenPacket("udp4", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			sender, err := net.ListenPacket("udp4", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			defer sender.Close()
			sender.(*net.UDPConn).SetWriteBuffer(1 << 20)
			input.(*net.UDPConn).SetReadBuffer(1 << 20)
			var conn EnhancePacketConn
			if name == "sing" {
				conn = NewEnhancePacketConn(bufio.NewPacketConn(input))
			} else if name == "opaque" {
				conn = NewEnhancePacketConn(opaquePacketConn{input})
			} else {
				conn = NewEnhancePacketConn(input)
			}
			defer conn.Close()
			conn.SetReadDeadline(time.Now().Add(5 * time.Second))
			payloads := [][]byte{nil, []byte("two"), bytes.Repeat([]byte{0xab}, 60000), []byte("four")}
			for _, payload := range payloads {
				if _, err = sender.WriteTo(payload, input.LocalAddr()); err != nil {
					t.Fatal(err)
				}
			}
			for i, want := range payloads {
				if i%2 == 0 {
					data, put, addr, err := conn.WaitReadFrom()
					if err != nil {
						t.Fatal(err)
					}
					if put == nil && (name != "opaque" || len(want) > 0) {
						t.Fatal("missing owned buffer, including empty packet")
					}
					if !bytes.Equal(data, want) || addr.String() != sender.LocalAddr().String() {
						t.Fatalf("packet %d: size=%d address=%v", i, len(data), addr)
					}
					if put != nil {
						put()
					}
				} else {
					b := make([]byte, 65535)
					n, _, err := conn.ReadFrom(b)
					if err != nil || !bytes.Equal(b[:n], want) {
						t.Fatalf("packet %d: %d %v", i, n, err)
					}
				}
			}
			conn.SetReadDeadline(time.Now().Add(-time.Second))
			if _, _, _, err := conn.WaitReadFrom(); err == nil {
				t.Fatal("deadline ignored")
			}
		})
	}
}

func TestPacketReaderCloseInterruptsRead(t *testing.T) {
	input, err := net.ListenPacket("udp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	conn := NewEnhancePacketConn(input)
	defer conn.Close()
	done := make(chan error, 1)
	go func() {
		_, put, _, err := conn.WaitReadFrom()
		if put != nil {
			put()
		}
		done <- err
	}()
	if err = conn.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case err = <-done:
		if !errors.Is(err, net.ErrClosed) {
			t.Fatal(err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("read stuck")
	}
}

type copyingPacketWaiter struct {
	N.PacketReader
	packet   *buf.Buffer
	err      error
	releases int
}

func (w *copyingPacketWaiter) InitializeReadWaiter(N.ReadWaitOptions) bool { return true }
func (w *copyingPacketWaiter) WaitReadPacket() (*buf.Buffer, M.Socksaddr, error) {
	return w.packet, M.ParseSocksaddr("example.org:53"), w.err
}
func (w *copyingPacketWaiter) ReleaseReadWaiter() { w.releases++ }

func TestPacketReaderHeadroomAndErrorOwnership(t *testing.T) {
	for _, fail := range []bool{false, true} {
		name := "headroom"
		if fail {
			name = "read-error"
		}
		t.Run(name, func(t *testing.T) {
			original := buf.NewSize(16)
			original.WriteString("payload")
			source := &copyingPacketWaiter{packet: original}
			if fail {
				source.err = io.ErrUnexpectedEOF
			}
			reader := NewReader(source, N.ReadWaitOptions{FrontHeadroom: 32, RearHeadroom: 16})
			packet, _, err := reader.ReadPacket()
			if fail {
				if !errors.Is(err, io.ErrUnexpectedEOF) || packet != nil {
					t.Fatal("lost read error")
				}
			} else {
				if err != nil {
					t.Fatal(err)
				}
				defer packet.Release()
				if string(packet.Bytes()) != "payload" || packet.Start() < 32 || packet.FreeLen() < 16 {
					t.Fatal("headroom not reserved")
				}
			}
			if original.Cap() != 0 {
				t.Fatal("source buffer not released")
			}
			reader.Close()
			reader.Close()
			if source.releases != 1 {
				t.Fatalf("waiter released %d times", source.releases)
			}
		})
	}
}

func TestPacketReaderConnectedDatagrams(t *testing.T) {
	peer, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	defer peer.Close()
	source, err := net.DialUDP("udp4", nil, peer.LocalAddr().(*net.UDPAddr))
	if err != nil {
		t.Fatal(err)
	}
	reader := NewReader(bufio.NewPacketConn(source), N.ReadWaitOptions{})
	defer func() { source.Close(); reader.Close() }()
	if err = source.SetReadDeadline(time.Now().Add(3 * time.Second)); err != nil {
		t.Fatal(err)
	}
	for _, data := range [][]byte{nil, []byte("connected")} {
		if _, err = peer.WriteToUDP(data, source.LocalAddr().(*net.UDPAddr)); err != nil {
			t.Fatal(err)
		}
	}
	for _, want := range []string{"", "connected"} {
		buffer, from, err := reader.ReadPacket()
		if err != nil {
			t.Fatal(err)
		}
		got := string(buffer.Bytes())
		buffer.Release()
		if got != want || from.String() != peer.LocalAddr().String() {
			t.Fatalf("%q from %v", got, from)
		}
	}
}

type opaquePacketConn struct{ net.PacketConn }
