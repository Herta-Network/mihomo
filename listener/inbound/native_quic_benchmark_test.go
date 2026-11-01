package inbound_test

import (
	"bytes"
	"context"
	"net"
	"net/netip"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/metacubex/mihomo/adapter/outbound"
	N "github.com/metacubex/mihomo/common/net"
	C "github.com/metacubex/mihomo/constant"
	"github.com/metacubex/mihomo/listener/inbound"
	"github.com/metacubex/mihomo/log"
	"github.com/metacubex/mihomo/tunnel/statistic"
	"github.com/metacubex/sing/common/buf"
	M "github.com/metacubex/sing/common/metadata"
	SN "github.com/metacubex/sing/common/network"
)

// Exercises real TLS/QUIC endpoints and production packet wrappers over UDP
// loopback. Routing and sender queues are excluded; setup is warmed before timing.
func BenchmarkNativeQUICOwnedRoundTrip(b *testing.B) {
	previous := log.Level()
	log.SetLevel(log.ERROR)
	b.Cleanup(func() { log.SetLevel(previous) })
	for _, mode := range []string{"hysteria2", "tuic-v4", "tuic-v5"} {
		b.Run(mode, func(b *testing.B) {
			const benchmarkUUID = "00000000-0000-0000-0000-000000000001"
			base := inbound.BaseOption{NameStr: "quic-owned-roundtrip", Listen: "127.0.0.1", Port: "0"}
			var in C.InboundListener
			var err error
			if mode == "hysteria2" {
				in, err = inbound.NewHysteria2(&inbound.Hysteria2Option{BaseOption: base, Users: map[string]string{"benchmark": benchmarkUUID}, Certificate: tlsCertificate, PrivateKey: tlsPrivateKey})
			} else {
				in, err = inbound.NewTuic(&inbound.TuicOption{BaseOption: base, Token: []string{benchmarkUUID}, Users: map[string]string{benchmarkUUID: benchmarkUUID}, Certificate: tlsCertificate, PrivateKey: tlsPrivateKey, CongestionController: "cubic"})
			}
			if err != nil {
				b.Fatal(err)
			}
			tunnel := NewHttpTestTunnel()
			defer tunnel.Close()
			tunnel.HandleUDPPacketFn = func(p C.UDPPacket, metadata *C.Metadata) {
				var target C.WriteBack = p
				if provider, ok := p.(interface{ WriteBackTarget() C.WriteBack }); ok {
					target = provider.WriteBackTarget()
				}
				var buffer *buf.Buffer
				if owned, ok := p.(C.PacketBuffer); ok {
					buffer = owned.TakeBuffer()
				} else {
					buffer = buf.NewSize(len(p.Data()))
					buffer.Write(p.Data())
				}
				p.Drop()
				destination := M.SocksaddrFromNetIP(metadata.AddrPort())
				var err error
				if writer, ok := target.(interface {
					WriteBackBuffer(*buf.Buffer, M.Socksaddr) error
				}); ok {
					err = writer.WriteBackBuffer(buffer, destination)
				} else {
					_, err = target.WriteBack(buffer.Bytes(), destination.UDPAddr())
					buffer.Release()
				}
				if err != nil {
					b.Error(err)
				}
			}
			if err = in.Listen(tunnel); err != nil {
				b.Fatal(err)
			}
			defer in.Close()
			address, err := netip.ParseAddrPort(strings.Split(in.Address(), ",")[0])
			if err != nil {
				b.Fatal(err)
			}
			basic := outbound.BasicOption{DialerForAPI: tunnel.NewDialer(), TunnelForAPI: tunnel}
			var out outbound.ProxyAdapter
			if mode == "hysteria2" {
				out, err = outbound.NewHysteria2(outbound.Hysteria2Option{BasicOption: basic, Name: mode, Server: address.Addr().String(), Port: int(address.Port()), Password: benchmarkUUID, Fingerprint: tlsFingerprint})
			} else {
				option := outbound.TuicOption{BasicOption: basic, Name: mode, Server: address.Addr().String(), Port: int(address.Port()), Fingerprint: tlsFingerprint, CongestionController: "cubic"}
				if mode == "tuic-v4" {
					option.Token = benchmarkUUID
				} else {
					option.UUID = benchmarkUUID
					option.Password = benchmarkUUID
				}
				out, err = outbound.NewTuic(option)
			}
			if err != nil {
				b.Fatal(err)
			}
			defer out.Close()
			metadata := &C.Metadata{NetWork: C.UDP, DstIP: netip.MustParseAddr("127.0.0.9"), DstPort: 5353}
			conn, err := out.ListenPacketContext(context.Background(), metadata)
			if err != nil {
				b.Fatal(err)
			}
			manager := &statistic.Manager{}
			tracker := statistic.NewUDPTracker(conn, manager, metadata, nil, 0, 0, true)
			defer tracker.Close()
			writer, ok := N.CreatePacketBatchWriterWithCount(tracker)
			if !ok {
				b.Fatal("missing counted packet writer")
			}
			reader, hasOwned := any(tracker).(interface {
				ReadPacketBuffer() (*buf.Buffer, M.Socksaddr, error)
			})
			options := SN.NewReadWaitOptions(nil, tracker)
			payload := bytes.Repeat([]byte{7}, 1200)
			var buffers [4]*buf.Buffer
			destination := M.ParseSocksaddr("127.0.0.9:5353")
			destinations := [4]M.Socksaddr{destination, destination, destination, destination}
			roundTrip := func() {
				if err := tracker.SetReadDeadline(time.Now().Add(10 * time.Second)); err != nil {
					b.Fatal(err)
				}
				for i := range buffers {
					buffers[i] = options.Copy(buf.As(payload))
				}
				if n, err := writer.WritePacketBatchCount(buffers[:], destinations[:]); n != 4 || err != nil {
					b.Fatalf("write n=%d err=%v", n, err)
				}
				for i := 0; i < 4; i++ {
					if hasOwned {
						buffer, source, err := reader.ReadPacketBuffer()
						if err != nil {
							b.Fatal(err)
						}
						valid := bytes.Equal(payload, buffer.Bytes()) && source == destination
						buffer.Release()
						if !valid {
							b.Fatal("owned reply changed payload/source")
						}
					} else {
						data, put, source, err := tracker.WaitReadFrom()
						if err != nil {
							b.Fatal(err)
						}
						udp, ok := source.(*net.UDPAddr)
						valid := bytes.Equal(payload, data) && ok && udp.AddrPort() == destination.AddrPort()
						if put != nil {
							put()
						}
						if !valid {
							b.Fatal("reply changed payload/source")
						}
					}
				}
			}
			roundTrip()
			b.ReportAllocs()
			b.SetBytes(4 * 1200)
			samples := make([]int64, b.N)
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				start := time.Now()
				roundTrip()
				samples[i] = time.Since(start).Nanoseconds()
			}
			b.StopTimer()
			sort.Slice(samples, func(i, j int) bool { return samples[i] < samples[j] })
			b.ReportMetric(float64(samples[(len(samples)-1)*50/100]), "p50-ns")
			b.ReportMetric(float64(samples[(len(samples)-1)*95/100]), "p95-ns")
			b.ReportMetric(float64(samples[(len(samples)-1)*99/100]), "p99-ns")
			upload, download := manager.Total()
			if want := int64(b.N+1) * 4 * 1200; upload != want || download != want {
				b.Fatalf("upload=%d download=%d want=%d", upload, download, want)
			}
		})
	}
}
