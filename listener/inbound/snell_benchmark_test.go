package inbound_test

import (
	"context"
	"fmt"
	"net/netip"
	"sync/atomic"
	"testing"
	"time"

	"github.com/metacubex/mihomo/adapter/outbound"
	N "github.com/metacubex/mihomo/common/net"
	C "github.com/metacubex/mihomo/constant"
	"github.com/metacubex/mihomo/listener/inbound"
	"github.com/metacubex/mihomo/tunnel/statistic"
	"github.com/metacubex/sing/common/buf"
	M "github.com/metacubex/sing/common/metadata"
	SN "github.com/metacubex/sing/common/network"
)

// Snell v6 derives its shaping profile from the PSK. Keep the workload
// identical across independent before/after benchmark processes.
const snellBenchmarkPSK = "mihomo-fixed-benchmark-psk-2026-11-01"

// BenchmarkSnellPacketWrite compares the same production adapter/traffic
// wrappers on revisions with and without counted batch capability. Each op is
// four datagrams, matching packetSender's queue-drain limit. The receiving
// listener consumes every packet before the timer stops, without an echo leg.
func BenchmarkSnellPacketWrite(b *testing.B) {
	for _, version := range []int{3, 4, 6} {
		for _, size := range []int{64, 1200} {
			b.Run(fmt.Sprintf("v%d/%d", version, size), func(b *testing.B) {
				in, err := inbound.NewSnell(&inbound.SnellOption{BaseOption: inbound.BaseOption{NameStr: "snell-benchmark", Listen: "127.0.0.1", Port: "0"}, Psk: snellBenchmarkPSK, Version: version, UDP: true})
				if err != nil {
					b.Fatal(err)
				}
				tunnel := NewHttpTestTunnel()
				defer tunnel.Close()
				var received atomic.Int64
				tunnel.HandleUDPPacketFn = func(p C.UDPPacket, _ *C.Metadata) { p.Drop(); received.Add(1) }
				if err = in.Listen(tunnel); err != nil {
					b.Fatal(err)
				}
				defer in.Close()
				addr, err := netip.ParseAddrPort(in.Address())
				if err != nil {
					b.Fatal(err)
				}
				out, err := outbound.NewSnell(outbound.SnellOption{BasicOption: outbound.BasicOption{DialerForAPI: tunnel.NewDialer()}, Name: "snell-benchmark", Server: addr.Addr().String(), Port: int(addr.Port()), Psk: snellBenchmarkPSK, Version: version, UDP: true})
				if err != nil {
					b.Fatal(err)
				}
				defer out.Close()
				metadata := &C.Metadata{NetWork: C.UDP, DstIP: remoteAddr, DstPort: 5353}
				conn, err := out.ListenPacketContext(context.Background(), metadata)
				if err != nil {
					b.Fatal(err)
				}
				manager := &statistic.Manager{}
				tracker := statistic.NewUDPTracker(conn, manager, metadata, nil, 0, 0, true)
				defer tracker.Close()
				payload := make([]byte, size)
				destination := M.ParseSocksaddr("127.0.0.9:5353")
				udpAddr := destination.UDPAddr()
				wait := func(want int64) {
					deadline := time.Now().Add(10 * time.Second)
					for received.Load() != want {
						if time.Now().After(deadline) {
							b.Fatalf("received %d/%d", received.Load(), want)
						}
						time.Sleep(50 * time.Microsecond)
					}
				}
				if _, err = tracker.WriteTo(payload, udpAddr); err != nil {
					b.Fatal(err)
				}
				wait(1)
				writer, batched := N.CreatePacketBatchWriterWithCount(tracker)
				options := SN.NewReadWaitOptions(nil, tracker)
				var buffers [4]*buf.Buffer
				destinations := [4]M.Socksaddr{destination, destination, destination, destination}
				b.ReportAllocs()
				b.SetBytes(int64(4 * size))
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					if batched {
						for j := range buffers {
							buffers[j] = options.Copy(buf.As(payload))
						}
						n, err := writer.WritePacketBatchCount(buffers[:], destinations[:])
						if err != nil || n != 4 {
							b.Fatalf("prefix=%d err=%v", n, err)
						}
					} else {
						for j := 0; j < 4; j++ {
							if _, err := tracker.WriteTo(payload, udpAddr); err != nil {
								b.Fatal(err)
							}
						}
					}
				}
				wait(1 + 4*int64(b.N))
				b.StopTimer()
				upload, _ := manager.Total()
				if upload != int64(size)*(1+4*int64(b.N)) {
					b.Fatalf("payload accounting=%d", upload)
				}
				b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(4*b.N), "ns/packet")
			})
		}
	}
}
