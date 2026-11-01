package tunnel

import (
	"context"
	"errors"
	"io"
	"net"
	"net/netip"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	N "github.com/metacubex/mihomo/common/net"
	C "github.com/metacubex/mihomo/constant"
	"github.com/metacubex/mihomo/constant/features"
	"github.com/metacubex/mihomo/tunnel/statistic"
	"github.com/metacubex/sing/common/buf"
	M "github.com/metacubex/sing/common/metadata"
	SN "github.com/metacubex/sing/common/network"
)

type senderTestPacket struct {
	C.UDPPacket
	payload []byte
	drops   atomic.Int32
	dropped chan struct{}
	onDrop  func()
}

func (p *senderTestPacket) Data() []byte { return p.payload }
func (p *senderTestPacket) LocalAddr() net.Addr {
	return &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 12345}
}
func (p *senderTestPacket) Drop() {
	p.drops.Add(1)
	if p.onDrop != nil {
		p.onDrop()
	}
	if p.dropped != nil {
		p.dropped <- struct{}{}
	}
}

type senderTestConn struct {
	C.PacketConn
	prefix                             int
	failure                            error
	batchCalls, scalarCalls, deadlines int
	packets                            []string
	resolveStarted                     chan struct{}
}

func (c *senderTestConn) CreatePacketBatchWriterWithCount() (SN.PacketBatchWriterWithCount, bool) {
	return c, true
}
func (c *senderTestConn) WritePacketBatchCount(buffers []*buf.Buffer, _ []M.Socksaddr) (int, error) {
	defer buf.ReleaseMulti(buffers)
	c.batchCalls++
	for _, b := range buffers {
		c.packets = append(c.packets, string(b.Bytes()))
	}
	return c.prefix, c.failure
}
func (c *senderTestConn) WriteTo(p []byte, _ net.Addr) (int, error) {
	c.scalarCalls++
	c.packets = append(c.packets, string(p))
	return len(p), nil
}
func (c *senderTestConn) SetReadDeadline(time.Time) error { c.deadlines++; return nil }
func (c *senderTestConn) RemoteDestination() string       { return "" }
func (c *senderTestConn) Chains() C.Chain                 { return nil }
func (c *senderTestConn) ProviderChains() C.Chain         { return nil }
func (c *senderTestConn) Close() error                    { return nil }
func (c *senderTestConn) ResolveUDP(ctx context.Context, _ *C.Metadata) error {
	close(c.resolveStarted)
	<-ctx.Done()
	return ctx.Err()
}

type senderWriteBack struct {
	C.WriteBackProxy
	updates int
}

func (p *senderWriteBack) UpdateWriteBack(C.WriteBack) { p.updates++ }

func waitSender(t *testing.T, done <-chan struct{}) {
	t.Helper()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("sender did not stop")
	}
}
func receiveDrops(t *testing.T, ch <-chan struct{}, count int) {
	t.Helper()
	for i := 0; i < count; i++ {
		select {
		case <-ch:
		case <-time.After(3 * time.Second):
			t.Fatal("packet ownership retained")
		}
	}
}

func TestPacketSenderBatchCountsPrefix(t *testing.T) {
	for _, prefix := range []int{0, 1, 2, 4} {
		t.Run(string(rune('0'+prefix)), func(t *testing.T) {
			s := newPacketSender().(*packetSender)
			defer s.Close()
			metadata := &C.Metadata{NetWork: C.UDP, DstIP: netip.MustParseAddr("127.0.0.2"), DstPort: 53}
			s.AddMapping(metadata, metadata)
			raw := &senderTestConn{prefix: prefix}
			if prefix < 4 {
				raw.failure = io.ErrClosedPipe
			}
			manager := &statistic.Manager{}
			conn := statistic.NewUDPTracker(raw, manager, metadata, nil, 0, 0, true)
			defer conn.Close()
			dropped := make(chan struct{}, 5)
			var packets []*senderTestPacket
			for _, payload := range []string{"", "b", "cc", "ddd"} {
				p := &senderTestPacket{payload: []byte(payload), dropped: dropped}
				packets = append(packets, p)
				s.Send(C.NewPacketAdapter(p, metadata))
			}
			proxy := &senderWriteBack{}
			done := make(chan struct{})
			go func() { s.Process(conn, proxy); close(done) }()
			receiveDrops(t, dropped, 4)
			s.Close()
			waitSender(t, done)
			for _, p := range packets {
				if p.drops.Load() != 1 {
					t.Fatal("packet released more than once")
				}
			}
			wantBytes := int64(prefix * (prefix - 1) / 2)
			wantDeadlines := 0
			if prefix > 0 {
				wantDeadlines = 1
			}
			if features.WithLowMemory {
				wantBytes, wantDeadlines = 6, 4
				if raw.batchCalls != 0 || raw.scalarCalls != 4 {
					t.Fatal("low-memory path used batch")
				}
			} else if raw.batchCalls != 1 || raw.scalarCalls != 0 {
				t.Fatalf("batch=%d scalar=%d", raw.batchCalls, raw.scalarCalls)
			}
			uploaded, _ := manager.Total()
			if uploaded != wantBytes || conn.Info().UploadTotal.Load() != wantBytes || raw.deadlines != wantDeadlines || proxy.updates != 4 {
				t.Fatalf("bytes=%d deadlines=%d updates=%d", uploaded, raw.deadlines, proxy.updates)
			}
			if len(raw.packets) != 4 {
				t.Fatal("retried or lost queued input")
			}
			late := &senderTestPacket{}
			s.Send(C.NewPacketAdapter(late, metadata))
			if late.drops.Load() != 1 {
				t.Fatal("send after close retained ownership")
			}
		})
	}
}

func TestPacketSenderSendCloseRaceDropsExactlyOnce(t *testing.T) {
	s := newPacketSender().(*packetSender)
	metadata := &C.Metadata{NetWork: C.UDP, DstIP: netip.MustParseAddr("127.0.0.2"), DstPort: 53}
	packets := make([]senderTestPacket, 1000)
	start := make(chan struct{})
	var workers sync.WaitGroup
	for worker := 0; worker < 10; worker++ {
		workers.Add(1)
		go func(worker int) {
			defer workers.Done()
			<-start
			for i := worker; i < len(packets); i += 10 {
				s.Send(C.NewPacketAdapter(&packets[i], metadata))
			}
		}(worker)
	}
	workers.Add(1)
	go func() { defer workers.Done(); <-start; s.Close() }()
	close(start)
	workers.Wait()
	s.Close()
	for i := range packets {
		if packets[i].drops.Load() != 1 {
			t.Fatalf("packet %d drops=%d", i, packets[i].drops.Load())
		}
	}
	if len(s.ch) != 0 {
		t.Fatal("enqueue survived close drain")
	}
}

func TestPacketSenderCancellationDuringResolution(t *testing.T) {
	s := newPacketSender().(*packetSender)
	defer s.Close()
	raw := &senderTestConn{resolveStarted: make(chan struct{})}
	dropped := make(chan struct{}, 1)
	packet := &senderTestPacket{payload: []byte("pending"), dropped: dropped}
	s.Send(C.NewPacketAdapter(packet, &C.Metadata{NetWork: C.UDP, Host: "blocked.example", DstPort: 53}))
	done := make(chan struct{})
	go func() { s.Process(raw, nil); close(done) }()
	select {
	case <-raw.resolveStarted:
	case <-time.After(3 * time.Second):
		t.Fatal("resolve not started")
	}
	s.Close()
	waitSender(t, done)
	receiveDrops(t, dropped, 1)
	if raw.batchCalls != 0 || raw.scalarCalls != 0 || packet.drops.Load() != 1 {
		t.Fatal("canceled packet was sent or leaked")
	}
}

func TestPacketSenderNativeWrappers(t *testing.T) {
	output, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	defer output.Close()
	input, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	conn := &senderNativeConn{EnhancePacketConn: N.NewDeadlineEnhancePacketConn(N.NewRefPacketConn(N.NewThreadSafePacketConn(input), new(int)))}
	defer conn.Close()
	metadata := &C.Metadata{NetWork: C.UDP, DstIP: netip.MustParseAddr("127.0.0.1"), DstPort: uint16(output.LocalAddr().(*net.UDPAddr).Port)}
	tracked := statistic.NewUDPTracker(conn, &statistic.Manager{}, metadata, nil, 0, 0, true)
	defer tracked.Close()
	s := newPacketSender().(*packetSender)
	defer s.Close()
	s.AddMapping(metadata, metadata)
	dropped := make(chan struct{}, 4)
	for _, value := range []string{"", "one", "two", "three"} {
		s.Send(C.NewPacketAdapter(&senderTestPacket{payload: []byte(value), dropped: dropped}, metadata))
	}
	done := make(chan struct{})
	go func() { s.Process(tracked, nil); close(done) }()
	output.SetReadDeadline(time.Now().Add(3 * time.Second))
	for _, want := range []string{"", "one", "two", "three"} {
		p := make([]byte, 32)
		n, _, err := output.ReadFrom(p)
		if err != nil || string(p[:n]) != want {
			t.Fatalf("got %q err=%v", p[:n], err)
		}
	}
	receiveDrops(t, dropped, 4)
	s.Close()
	waitSender(t, done)
	if tracked.Info().UploadTotal.Load() != 11 {
		t.Fatalf("upload=%d", tracked.Info().UploadTotal.Load())
	}
	// No additional datagram may be emitted by retrying a completed prefix.
	output.SetReadDeadline(time.Now().Add(-time.Second))
	_, _, err = output.ReadFrom(make([]byte, 1))
	var timeout net.Error
	if !errors.As(err, &timeout) || !timeout.Timeout() {
		t.Fatal("unexpected extra packet")
	}
}

type senderNativeConn struct{ N.EnhancePacketConn }

func (*senderNativeConn) RemoteDestination() string                     { return "" }
func (*senderNativeConn) Chains() C.Chain                               { return nil }
func (*senderNativeConn) ProviderChains() C.Chain                       { return nil }
func (*senderNativeConn) AppendToChains(C.ProxyAdapter)                 {}
func (*senderNativeConn) ResolveUDP(context.Context, *C.Metadata) error { return nil }
func (c *senderNativeConn) CreatePacketBatchWriterWithCount() (SN.PacketBatchWriterWithCount, bool) {
	return N.CreatePacketBatchWriterWithCount(c.EnhancePacketConn)
}

func TestPacketSenderFlushesBeforeResolvingNextDestination(t *testing.T) {
	s := newPacketSender().(*packetSender)
	defer s.Close()
	known := &C.Metadata{NetWork: C.UDP, DstIP: netip.MustParseAddr("127.0.0.2"), DstPort: 53}
	s.AddMapping(known, known)
	raw := &senderTestConn{prefix: 1, resolveStarted: make(chan struct{})}
	dropped := make(chan struct{}, 2)
	s.Send(C.NewPacketAdapter(&senderTestPacket{payload: []byte("first"), dropped: dropped}, known))
	s.Send(C.NewPacketAdapter(&senderTestPacket{payload: []byte("second"), dropped: dropped}, &C.Metadata{NetWork: C.UDP, Host: "blocked.example", DstPort: 53}))
	done := make(chan struct{})
	go func() { s.Process(raw, nil); close(done) }()
	select {
	case <-raw.resolveStarted:
	case <-time.After(3 * time.Second):
		t.Fatal("resolve did not start")
	}
	if len(raw.packets) != 1 || raw.packets[0] != "first" {
		t.Fatal("held earlier packet behind DNS resolution")
	}
	s.Close()
	waitSender(t, done)
	receiveDrops(t, dropped, 2)
}

func TestPacketSenderDropCanReenterClose(t *testing.T) {
	for _, queued := range []bool{false, true} {
		s := newPacketSender().(*packetSender)
		if !queued {
			s.Close()
		}
		p := &senderTestPacket{onDrop: func() { s.Close() }}
		metadata := &C.Metadata{NetWork: C.UDP, DstIP: netip.MustParseAddr("127.0.0.2"), DstPort: 53}
		done := make(chan struct{})
		go func() { s.Send(C.NewPacketAdapter(p, metadata)); s.Close(); close(done) }()
		waitSender(t, done)
		if p.drops.Load() != 1 {
			t.Fatal("reentrant drop repeated")
		}
	}
}
