package inbound_test

import (
	"bytes"
	"context"
	"fmt"
	"net"
	"net/netip"
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

// This fixture also compiles on the parent revision. Optional owned APIs keep
// the same four-packet echo workload while exercising each revision's actual
// outbound/deadline/ref/traffic wrappers and the listener's reply endpoint.
// It includes both network endpoints, but not the tunnel's routing/sender queue.
func BenchmarkSnellOwnedRoundTrip(b *testing.B) {
	for _, version := range []int{3, 4, 6} {
		b.Run(fmt.Sprintf("v%d", version), func(b *testing.B) {
			in, err := inbound.NewSnell(&inbound.SnellOption{BaseOption: inbound.BaseOption{NameStr: "owned-roundtrip", Listen: "127.0.0.1", Port: "0"}, Psk: snellBenchmarkPSK, Version: version, UDP: true})
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
				buffer := p.(C.PacketBuffer).TakeBuffer()
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
			address, err := netip.ParseAddrPort(in.Address())
			if err != nil {
				b.Fatal(err)
			}
			out, err := outbound.NewSnell(outbound.SnellOption{BasicOption: outbound.BasicOption{DialerForAPI: tunnel.NewDialer()}, Name: "owned-roundtrip", Server: address.Addr().String(), Port: int(address.Port()), Psk: snellBenchmarkPSK, Version: version, UDP: true})
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
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				roundTrip()
			}
			b.StopTimer()
			upload, download := manager.Total()
			if want := int64(b.N+1) * 4 * 1200; upload != want || download != want {
				b.Fatalf("upload=%d download=%d want=%d", upload, download, want)
			}
		})
	}
}
