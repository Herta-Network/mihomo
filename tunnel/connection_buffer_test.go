package tunnel

import (
	"io"
	"net/netip"
	"testing"

	C "github.com/metacubex/mihomo/constant"
	"github.com/metacubex/sing/common/buf"
	M "github.com/metacubex/sing/common/metadata"
)

type ownedSenderPacket struct {
	senderTestPacket
	buffer *buf.Buffer
}

func (p *ownedSenderPacket) Data() []byte { return p.buffer.Bytes() }
func (p *ownedSenderPacket) TakeBuffer() *buf.Buffer {
	b := p.buffer
	p.buffer = nil
	return b
}
func (p *ownedSenderPacket) Drop() {
	p.buffer.Release()
	p.buffer = nil
	p.senderTestPacket.Drop()
}

type ownedSenderConn struct {
	senderTestConn
	want []*buf.Buffer
	t    *testing.T
}

func (*ownedSenderConn) FrontHeadroom() int { return 16 }
func (*ownedSenderConn) RearHeadroom() int  { return 8 }
func (c *ownedSenderConn) WritePacketBatchCount(buffers []*buf.Buffer, destinations []M.Socksaddr) (int, error) {
	for i, buffer := range buffers {
		if buffer != c.want[i] {
			c.t.Fatal("owned packet copied despite sufficient headroom")
		}
		if destinations[i] != M.ParseSocksaddr("127.0.0.2:53") {
			c.t.Fatalf("destination=%v", destinations[i])
		}
		// Exercise both ends: the payload remains owned until this call, and
		// packet.Drop must not release it again after the writer consumes it.
		clear(buffer.ExtendHeader(16))
		clear(buffer.Extend(8))
	}
	return c.senderTestConn.WritePacketBatchCount(buffers, destinations)
}

func TestPacketSenderTransfersOwnedBuffers(t *testing.T) {
	for _, single := range []bool{false, true} {
		s := newPacketSender().(*packetSender)
		metadata := &C.Metadata{NetWork: C.UDP, DstIP: netip.MustParseAddr("127.0.0.2"), DstPort: 53}
		s.AddMapping(metadata, metadata)
		count := packetSendBatchSize
		if single {
			count = 1
		}
		conn := &ownedSenderConn{t: t, senderTestConn: senderTestConn{prefix: 1, failure: io.ErrClosedPipe}}
		packets := make([]C.PacketAdapter, count)
		owned := make([]*ownedSenderPacket, count)
		for i := range packets {
			buffer := buf.NewSize(16 + i + 8)
			buffer.Resize(16, i) // includes an empty datagram
			conn.want = append(conn.want, buffer)
			owned[i] = &ownedSenderPacket{buffer: buffer}
			packets[i] = C.NewPacketAdapter(owned[i], metadata)
		}
		if single {
			s.processPacket(conn, packets[0], conn)
		} else {
			s.processBatch(conn, nil, conn, packets)
		}
		s.Close()
		for i, packet := range owned {
			if packet.buffer != nil || packet.drops.Load() != 1 || conn.want[i].RawCap() != 0 {
				t.Fatalf("packet %d retained or released more than once", i)
			}
		}
		for i := range s.buffers {
			if s.buffers[i] != nil || s.destinations[i] != (M.Socksaddr{}) {
				t.Fatal("sender scratch retained packet state")
			}
		}
		if conn.batchCalls != 1 || conn.scalarCalls != 0 || conn.deadlines != 1 {
			t.Fatalf("batch=%d scalar=%d deadlines=%d", conn.batchCalls, conn.scalarCalls, conn.deadlines)
		}
	}
}

func TestPacketSenderCanceledOwnedPacketIsReleased(t *testing.T) {
	s := newPacketSender().(*packetSender)
	s.Close()
	buffer := buf.NewSize(16)
	p := &ownedSenderPacket{buffer: buffer}
	metadata := &C.Metadata{NetWork: C.UDP, DstIP: netip.MustParseAddr("127.0.0.2"), DstPort: 53}
	s.processBatch(&senderTestConn{}, nil, &senderTestConn{}, []C.PacketAdapter{C.NewPacketAdapter(p, metadata)})
	if p.drops.Load() != 1 || buffer.RawCap() != 0 {
		t.Fatal("canceled owned packet retained")
	}
}

func TestPacketSenderDestinationKeysPreserveMappings(t *testing.T) {
	s := newPacketSender().(*packetSender)
	defer s.Close()
	ip := netip.MustParseAddr("198.18.0.1")
	target := netip.MustParseAddr("127.0.0.2")
	for _, host := range []string{"", "one.example", "two.example"} {
		origin := &C.Metadata{NetWork: C.UDP, DstIP: ip, Host: host, DstPort: 53}
		s.AddMapping(origin, &C.Metadata{DstIP: target})
		// Ports are intentionally not part of the mapping, as before.
		origin.DstPort = 443
		got := s.packetAddress(&senderTestConn{}, C.NewPacketAdapter(&senderTestPacket{}, origin))
		if got != netip.AddrPortFrom(target, 443) || s.RestoreReadFrom(target) != ip {
			t.Fatalf("host=%s destination=%s", host, got)
		}
		// A later mapping must not replace an already valid resolution.
		s.AddMapping(origin, &C.Metadata{DstIP: netip.IPv6Loopback()})
		if got = s.packetAddress(&senderTestConn{}, C.NewPacketAdapter(&senderTestPacket{}, origin)); got.Addr() != target {
			t.Fatal("valid mapping replaced")
		}
		target = target.Next()
	}
}
