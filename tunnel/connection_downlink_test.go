package tunnel

import (
	"context"
	"errors"
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

type ownedDownlinkConn struct {
	senderTestConn
	buffers []*buf.Buffer
	sources []M.Socksaddr
	reads   int
	closed  bool
}

func (c *ownedDownlinkConn) ReadPacketBuffer() (*buf.Buffer, M.Socksaddr, error) {
	if c.reads == len(c.buffers) {
		return nil, M.Socksaddr{}, io.EOF
	}
	i := c.reads
	c.reads++
	return c.buffers[i], c.sources[i], nil
}
func (*ownedDownlinkConn) WaitReadFrom() ([]byte, func(), net.Addr, error) {
	panic("owned downstream capability bypassed")
}
func (c *ownedDownlinkConn) Close() error { c.closed = true; return nil }

type ownedDownlinkReply struct {
	t       *testing.T
	want    []*buf.Buffer
	sources []M.Socksaddr
	seen    int
	fail    bool
}

func (*ownedDownlinkReply) WriteBack([]byte, net.Addr) (int, error) {
	panic("owned reply capability lost")
}
func (r *ownedDownlinkReply) WriteBackBuffer(buffer *buf.Buffer, source M.Socksaddr) error {
	defer buffer.Release()
	if buffer != r.want[r.seen] || buffer.Start() != 32 {
		r.t.Fatal("downstream buffer identity/headroom lost")
	}
	if source != r.sources[r.seen] {
		r.t.Fatalf("source=%v want=%v", source, r.sources[r.seen])
	}
	r.seen++
	if r.fail {
		return io.ErrClosedPipe
	}
	return nil
}

func TestPacketOwnedDownlinkMappingAccountingAndRelease(t *testing.T) {
	for _, fail := range []bool{false, true} {
		metadata := &C.Metadata{DstIP: netip.MustParseAddr("198.18.0.1"), DstPort: 53}
		sender := newPacketSender().(*packetSender)
		target := netip.MustParseAddr("127.0.0.2")
		sender.AddMapping(metadata, &C.Metadata{DstIP: target})
		raw := &ownedDownlinkConn{}
		for _, payload := range []string{"", "reply", "fallback"} {
			buffer := buf.NewSize(64)
			buffer.Resize(32, 0)
			buffer.WriteString(payload)
			raw.buffers = append(raw.buffers, buffer)
		}
		raw.sources = []M.Socksaddr{M.SocksaddrFrom(target, 53), M.SocksaddrFrom(target, 5353), M.ParseSocksaddr("invalid.example:99")}
		manager := &statistic.Manager{}
		tracker := statistic.NewUDPTracker(raw, manager, metadata.Clone(), nil, 0, 0, true)
		reply := &ownedDownlinkReply{t: t, want: raw.buffers, fail: fail, sources: []M.Socksaddr{
			M.SocksaddrFrom(metadata.DstIP, 53), M.SocksaddrFrom(metadata.DstIP, 5353), M.SocksaddrFrom(metadata.DstIP, 53),
		}}
		handleUDPToLocal(nat.NewWriteBackProxy(reply), tracker, sender, "owned-downlink-test", netip.AddrPortFrom(target, 53))
		wantCount, wantBytes := 3, int64(len("replyfallback"))
		if fail {
			wantCount, wantBytes = 1, 0
		}
		_, download := manager.Total()
		if reply.seen != wantCount || download != wantBytes || tracker.DownloadTotal.Load() != wantBytes || !raw.closed || !errors.Is(sender.ctx.Err(), context.Canceled) {
			t.Fatalf("replies=%d download=%d total=%d closed=%v", reply.seen, download, tracker.DownloadTotal.Load(), raw.closed)
		}
		for i, buffer := range raw.buffers {
			if i < wantCount && buffer.RawCap() != 0 {
				t.Fatal("consumed reply buffer retained")
			}
			buffer.Release()
		}
	}
}
