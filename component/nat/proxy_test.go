package nat

import (
	"net"
	"net/netip"
	"sync"
	"testing"
	"time"

	C "github.com/metacubex/mihomo/constant"
	"github.com/metacubex/sing/common/buf"
	M "github.com/metacubex/sing/common/metadata"
)

type replyFunc func([]byte, net.Addr) (int, error)

func (f replyFunc) WriteBack(b []byte, addr net.Addr) (int, error) { return f(b, addr) }

func TestWriteBackProxyUpdatesDuringBlockedReply(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	done := make(chan struct{})
	proxy := NewWriteBackProxy(replyFunc(func(b []byte, _ net.Addr) (int, error) {
		close(started)
		<-release
		return len(b), nil
	}))
	go func() { _, _ = proxy.WriteBack(nil, nil); close(done) }()
	<-started
	updated := make(chan struct{})
	go func() {
		proxy.UpdateWriteBack(replyFunc(func(b []byte, _ net.Addr) (int, error) { return len(b), nil }))
		close(updated)
	}()
	select {
	case <-updated:
	case <-time.After(3 * time.Second):
		close(release)
		t.Fatal("blocked reply held update lock")
	}
	close(release)
	<-done
	if n, err := proxy.WriteBack([]byte("new"), nil); n != 3 || err != nil {
		t.Fatalf("new writer: n=%d err=%v", n, err)
	}
}

type detachedReplyPacket struct {
	C.UDPPacket
	target        C.WriteBack
	defaultSource netip.AddrPort
}

func (p *detachedReplyPacket) WriteBackTarget() C.WriteBack            { return p.target }
func (p *detachedReplyPacket) DefaultWriteBackAddress() netip.AddrPort { return p.defaultSource }
func (*detachedReplyPacket) WriteBack([]byte, net.Addr) (int, error) {
	panic("packet retained as reply target")
}
func (*detachedReplyPacket) LocalAddr() net.Addr { return &net.UDPAddr{} }
func (p *detachedReplyPacket) Drop()             { p.target = nil }

type consumingReply struct{ seen *buf.Buffer }

func (*consumingReply) WriteBack([]byte, net.Addr) (int, error) {
	panic("owned reply fell back to bytes")
}
func (r *consumingReply) WriteBackBuffer(b *buf.Buffer, _ M.Socksaddr) error {
	r.seen = b
	b.Release()
	return nil
}

func TestWriteBackProxyDetachesPacketAndMetadata(t *testing.T) {
	for _, update := range []bool{false, true} {
		reply := &consumingReply{}
		packet := &detachedReplyPacket{target: reply}
		adapter := C.NewPacketAdapter(packet, &C.Metadata{Host: "packet-only.example"})
		proxy := NewWriteBackProxy(adapter).(*writeBackProxy)
		if update {
			proxy.UpdateWriteBack(adapter)
		}
		if proxy.wb != reply {
			t.Fatal("proxy retained the packet/metadata envelope")
		}
		adapter.Drop()
		b := buf.NewSize(64)
		b.WriteString("reply")
		if err := C.WriteBackBuffer(proxy, b, M.ParseSocksaddr("127.0.0.1:53")); err != nil || reply.seen != b || b.RawCap() != 0 {
			t.Fatalf("reply failed after packet drop: %v", err)
		}
	}
}

func TestWriteBackProxySnapshotsNilAddressFallback(t *testing.T) {
	want := netip.MustParseAddrPort("127.0.0.9:5353")
	reply := replyFunc(func(b []byte, address net.Addr) (int, error) {
		if address.(*net.UDPAddr).AddrPort() != want {
			t.Fatalf("fallback source=%v", address)
		}
		return len(b), nil
	})
	p := &detachedReplyPacket{target: reply, defaultSource: want}
	proxy := NewWriteBackProxy(C.NewPacketAdapter(p, &C.Metadata{}))
	p.Drop()
	p.defaultSource = netip.AddrPort{}
	if _, err := proxy.WriteBack(nil, nil); err != nil {
		t.Fatal(err)
	}
	want = netip.MustParseAddrPort("[::1]:5354")
	proxy.UpdateWriteBack(C.NewPacketAdapter(&detachedReplyPacket{target: reply, defaultSource: want}, &C.Metadata{}))
	if _, err := proxy.WriteBack(nil, nil); err != nil {
		t.Fatal(err)
	}
}

func TestWriteBackProxyConcurrentUpdates(t *testing.T) {
	reply := replyFunc(func(b []byte, _ net.Addr) (int, error) { return len(b), nil })
	proxy := NewWriteBackProxy(reply)
	var workers sync.WaitGroup
	for i := 0; i < 4; i++ {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for j := 0; j < 1000; j++ {
				proxy.UpdateWriteBack(reply)
				_, _ = proxy.WriteBack(nil, nil)
			}
		}()
	}
	workers.Wait()
	if allocations := testing.AllocsPerRun(100, func() { proxy.UpdateWriteBack(reply) }); allocations != 0 {
		t.Fatalf("steady update allocates: %g", allocations)
	}
}
