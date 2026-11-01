package tunnel

import (
	"net"
	"net/netip"
	"sync"
	"testing"
	"time"

	"github.com/metacubex/mihomo/component/nat"
	C "github.com/metacubex/mihomo/constant"
	"github.com/metacubex/sing/common/buf"
	M "github.com/metacubex/sing/common/metadata"
	SN "github.com/metacubex/sing/common/network"
)

// This isolates the steady sender path, including destination mapping,
// write-back updates, headroom preparation and buffer ownership. Input storage
// is already owned by the ingress; acquiring it and socket I/O are measured
// separately by the inbound loopback benchmarks.
func BenchmarkPacketSenderBatch(b *testing.B) {
	for _, owned := range []bool{false, true} {
		name := "borrowed"
		if owned {
			name = "owned"
		}
		b.Run(name, func(b *testing.B) {
			s := newPacketSender().(*packetSender)
			defer s.Close()
			metadata := &C.Metadata{NetWork: C.UDP, DstIP: netip.MustParseAddr("127.0.0.2"), DstPort: 53}
			s.AddMapping(metadata, metadata)
			var inputs [packetSendBatchSize]*buf.Buffer
			var payloads [packetSendBatchSize]senderBenchmarkPacket
			var packets [packetSendBatchSize]C.PacketAdapter
			for i := range packets {
				inputs[i] = buf.With(make([]byte, 32+1200+16))
				payloads[i].buffer = inputs[i]
				var packet C.UDPPacket = &payloads[i]
				if owned {
					packet = &senderBenchmarkOwnedPacket{&payloads[i]}
				}
				packets[i] = C.NewPacketAdapter(packet, metadata)
			}
			proxy := nat.NewWriteBackProxy(packets[0])
			conn := &senderBenchmarkConn{}
			b.ReportAllocs()
			b.SetBytes(packetSendBatchSize * 1200)
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				for j, buffer := range inputs {
					buffer.Resize(32, 1200)
					payloads[j].buffer = buffer
				}
				s.processBatch(conn, proxy, conn, packets[:])
			}
		})
	}
}

type senderBenchmarkPacket struct {
	C.UDPPacket
	buffer *buf.Buffer
	onDrop func()
}

func (p *senderBenchmarkPacket) Data() []byte { return p.buffer.Bytes() }
func (p *senderBenchmarkPacket) Drop() {
	if p.onDrop != nil {
		p.onDrop()
	}
}
func (*senderBenchmarkPacket) LocalAddr() net.Addr { return benchmarkSenderAddress }

var benchmarkSenderAddress = &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 12345}

type senderBenchmarkOwnedPacket struct{ *senderBenchmarkPacket }

func (p *senderBenchmarkOwnedPacket) TakeBuffer() *buf.Buffer {
	buffer := p.buffer
	p.buffer = nil
	return buffer
}

type senderBenchmarkConn struct{ C.PacketConn }

func (c *senderBenchmarkConn) CreatePacketBatchWriterWithCount() (SN.PacketBatchWriterWithCount, bool) {
	return c, true
}

func (*senderBenchmarkConn) FrontHeadroom() int                        { return 32 }
func (*senderBenchmarkConn) RearHeadroom() int                         { return 16 }
func (*senderBenchmarkConn) SetReadDeadline(time.Time) error           { return nil }
func (*senderBenchmarkConn) WriteTo(p []byte, _ net.Addr) (int, error) { return len(p), nil }
func (*senderBenchmarkConn) WritePacketBatchCount(buffers []*buf.Buffer, _ []M.Socksaddr) (int, error) {
	buf.ReleaseMulti(buffers)
	return len(buffers), nil
}

// Includes actual admission, worker wakeup and Send/Process ownership transfer.
// Waiting for all Drops keeps producer reuse legal even when scheduling splits
// the four packets into several writes. No timed input storage is allocated.
func BenchmarkPacketSenderOwnedQueue(b *testing.B) {
	s := newPacketSender().(*packetSender)
	metadata := &C.Metadata{NetWork: C.UDP, DstIP: netip.MustParseAddr("127.0.0.2"), DstPort: 53}
	s.AddMapping(metadata, metadata)
	var dropped sync.WaitGroup
	var inputs [packetSendBatchSize]*buf.Buffer
	var payloads [packetSendBatchSize]senderBenchmarkPacket
	var packets [packetSendBatchSize]C.PacketAdapter
	for i := range packets {
		inputs[i] = buf.With(make([]byte, 32+1200+16))
		payloads[i].onDrop = dropped.Done
		packets[i] = C.NewPacketAdapter(&senderBenchmarkOwnedPacket{&payloads[i]}, metadata)
	}
	proxy := nat.NewWriteBackProxy(packets[0])
	done := make(chan struct{})
	go func() { s.Process(&senderBenchmarkConn{}, proxy); close(done) }()
	b.Cleanup(func() { s.Close(); <-done })
	b.ReportAllocs()
	b.SetBytes(packetSendBatchSize * 1200)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		dropped.Add(len(packets))
		for j, buffer := range inputs {
			buffer.Resize(32, 1200)
			payloads[j].buffer = buffer
			s.Send(packets[j])
		}
		dropped.Wait()
	}
}
