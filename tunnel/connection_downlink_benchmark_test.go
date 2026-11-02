package tunnel

import (
	"io"
	"net"
	"net/netip"
	"testing"

	"github.com/metacubex/mihomo/component/nat"
	C "github.com/metacubex/mihomo/constant"
	"github.com/metacubex/mihomo/tunnel/statistic"
	"github.com/metacubex/sing/common/buf"
	M "github.com/metacubex/sing/common/metadata"
)

type downlinkBenchmarkConn struct {
	senderTestConn
	buffer *buf.Buffer
	source M.Socksaddr
	left   int
}

func (c *downlinkBenchmarkConn) nextPacket() (*buf.Buffer, M.Socksaddr, error) {
	if c.left == 0 {
		return nil, M.Socksaddr{}, io.EOF
	}
	c.left--
	c.buffer.Resize(32, 1200)
	return c.buffer, c.source, nil
}
func (c *downlinkBenchmarkConn) WaitReadFrom() ([]byte, func(), net.Addr, error) {
	buffer, source, err := c.nextPacket()
	if err != nil {
		return nil, nil, nil, err
	}
	return buffer.Bytes(), buffer.Release, source.UDPAddr(), nil
}

type downlinkBenchmarkReply struct{ calls int }

func (r *downlinkBenchmarkReply) WriteBack(b []byte, _ net.Addr) (int, error) {
	r.calls++
	return len(b), nil
}

// This stage includes the actual downstream loop, traffic tracker, NAT mapping
// and reply proxy. The reader supplies an already-owned buffer; no socket I/O,
// ingress allocation or protocol encoding is included. One operation is a packet.
func BenchmarkPacketDownlink(b *testing.B) {
	metadata := &C.Metadata{DstIP: netip.MustParseAddr("198.18.0.1"), DstPort: 53}
	target := netip.MustParseAddr("127.0.0.2")
	sender := newPacketSender().(*packetSender)
	sender.AddMapping(metadata, &C.Metadata{DstIP: target})
	raw := &downlinkBenchmarkConn{buffer: buf.With(make([]byte, 32+1200+32)), source: M.SocksaddrFrom(target, 53), left: b.N}
	manager := &statistic.Manager{}
	tracker := statistic.NewUDPTracker(raw, manager, metadata, nil, 0, 0, true)
	reply := &downlinkBenchmarkReply{}
	proxy := nat.NewWriteBackProxy(reply)
	b.ReportAllocs()
	b.SetBytes(1200)
	b.ResetTimer()
	handleUDPToLocal(proxy, tracker, sender, "benchmark-owned-downlink", netip.AddrPortFrom(target, 53))
	b.StopTimer()
	_, download := manager.Total()
	if reply.calls != b.N || download != int64(b.N)*1200 {
		b.Fatalf("packets=%d/%d download=%d", reply.calls, b.N, download)
	}
}
