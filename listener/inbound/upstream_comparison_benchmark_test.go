package inbound_test

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"net"
	"net/netip"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/metacubex/mihomo/adapter/outbound"
	C "github.com/metacubex/mihomo/constant"
	"github.com/metacubex/mihomo/listener/inbound"
	"github.com/metacubex/mihomo/log"
	"github.com/metacubex/mihomo/tunnel/statistic"
	"github.com/metacubex/sing/common/buf"
	M "github.com/metacubex/sing/common/metadata"
)

// This file is compiled unchanged on upstream v1.19.33 and the optimized fork.
// Optional interfaces select each version's actual production capability;
// reflection is used only once during setup, never in the measured loop.
type comparisonBatchWriter interface {
	WritePacketBatchCount([]*buf.Buffer, []M.Socksaddr) (int, error)
}
type comparisonOwnedReader interface {
	ReadPacketBuffer() (*buf.Buffer, M.Socksaddr, error)
}

func comparisonBatch(conn any) comparisonBatchWriter {
	method := reflect.ValueOf(conn).MethodByName("CreatePacketBatchWriterWithCount")
	if !method.IsValid() {
		return nil
	}
	values := method.Call(nil)
	if len(values) != 2 || !values[1].Bool() {
		return nil
	}
	writer, _ := values[0].Interface().(comparisonBatchWriter)
	return writer
}

func comparisonProxy(b *testing.B, mode string, tunnel *TestTunnel) outbound.ProxyAdapter {
	b.Helper()
	const id = "00000000-0000-0000-0000-000000000001"
	const psk = "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA="
	base := inbound.BaseOption{NameStr: "upstream-comparison", Listen: "127.0.0.1", Port: "0"}
	if strings.HasPrefix(mode, "ss-") {
		for {
			probe, e := net.Listen("tcp4", "127.0.0.1:0")
			if e != nil {
				b.Fatal(e)
			}
			port := probe.Addr().(*net.TCPAddr).Port
			udp, e := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: port})
			probe.Close()
			if e != nil {
				continue
			}
			udp.Close()
			base.Port = fmt.Sprint(port)
			break
		}
	}
	var in C.InboundListener
	var err error
	switch {
	case strings.HasPrefix(mode, "vmess"):
		in, err = inbound.NewVmess(&inbound.VmessOption{BaseOption: base, Users: []inbound.VmessUser{{Username: "bench", UUID: id}}})
	case mode == "vless" || strings.HasPrefix(mode, "mux-"):
		in, err = inbound.NewVless(&inbound.VlessOption{BaseOption: base, AllowInsecure: true, Users: []inbound.VlessUser{{Username: "bench", UUID: id}}})
	case strings.HasPrefix(mode, "ss-"):
		in, err = inbound.NewShadowSocks(&inbound.ShadowSocksOption{BaseOption: base, Cipher: "aes-128-gcm", Password: "benchmark-password", UDP: true})
	case mode == "snell3":
		in, err = inbound.NewSnell(&inbound.SnellOption{BaseOption: base, Psk: psk, Version: 3, UDP: true})
	case mode == "anytls-uot":
		in, err = inbound.NewAnyTLS(&inbound.AnyTLSOption{BaseOption: base, Users: map[string]string{"bench": id}, Certificate: tlsCertificate, PrivateKey: tlsPrivateKey})
	case mode == "hysteria2":
		in, err = inbound.NewHysteria2(&inbound.Hysteria2Option{BaseOption: base, Users: map[string]string{"bench": id}, Certificate: tlsCertificate, PrivateKey: tlsPrivateKey})
	default:
		in, err = inbound.NewTuic(&inbound.TuicOption{BaseOption: base, Token: []string{id}, Users: map[string]string{id: id}, Certificate: tlsCertificate, PrivateKey: tlsPrivateKey, CongestionController: "cubic"})
	}
	if err != nil {
		b.Fatal(err)
	}
	if err = in.Listen(tunnel); err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { in.Close() })
	address, err := netip.ParseAddrPort(strings.Split(in.Address(), ",")[0])
	if err != nil {
		b.Fatal(err)
	}
	basic := outbound.BasicOption{DialerForAPI: tunnel.NewDialer(), TunnelForAPI: tunnel}
	var out outbound.ProxyAdapter
	switch {
	case strings.HasPrefix(mode, "vmess"):
		out, err = outbound.NewVmess(outbound.VmessOption{BasicOption: basic, Name: mode, Server: address.Addr().String(), Port: int(address.Port()), UUID: id, Cipher: "aes-128-gcm", UDP: true, XUDP: mode == "vmess-xudp"})
	case mode == "vless" || strings.HasPrefix(mode, "mux-"):
		out, err = outbound.NewVless(outbound.VlessOption{BasicOption: basic, Name: mode, Server: address.Addr().String(), Port: int(address.Port()), UUID: id, UDP: true})
	case strings.HasPrefix(mode, "ss-"):
		out, err = outbound.NewShadowSocks(outbound.ShadowSocksOption{BasicOption: basic, Name: mode, Server: address.Addr().String(), Port: int(address.Port()), Cipher: "aes-128-gcm", Password: "benchmark-password", UDP: true, UDPOverTCP: mode == "ss-uot", UDPOverTCPVersion: 2})
	case mode == "snell3":
		out, err = outbound.NewSnell(outbound.SnellOption{BasicOption: basic, Name: mode, Server: address.Addr().String(), Port: int(address.Port()), Psk: psk, Version: 3, UDP: true})
	case mode == "anytls-uot":
		out, err = outbound.NewAnyTLS(outbound.AnyTLSOption{BasicOption: basic, Name: mode, Server: address.Addr().String(), Port: int(address.Port()), Password: id, Fingerprint: tlsFingerprint, UDP: true})
	case mode == "hysteria2":
		out, err = outbound.NewHysteria2(outbound.Hysteria2Option{BasicOption: basic, Name: mode, Server: address.Addr().String(), Port: int(address.Port()), Password: id, Fingerprint: tlsFingerprint})
	default:
		options := outbound.TuicOption{BasicOption: basic, Name: mode, Server: address.Addr().String(), Port: int(address.Port()), Fingerprint: tlsFingerprint, CongestionController: "cubic"}
		if mode == "tuic4" {
			options.Token = id
		} else {
			options.UUID = id
			options.Password = id
		}
		out, err = outbound.NewTuic(options)
	}
	if err != nil {
		b.Fatal(err)
	}
	original := out
	b.Cleanup(func() { original.Close() })
	if strings.HasPrefix(mode, "mux-") {
		out, err = outbound.NewSingMux(outbound.SingMuxOption{Enabled: true, Protocol: strings.TrimPrefix(mode, "mux-")}, &notCloseProxyAdapter{out})
		if err != nil {
			b.Fatal(err)
		}
		b.Cleanup(func() { out.Close() })
	}
	return out
}

func BenchmarkUpstreamUDP(b *testing.B) {
	previous := log.Level()
	log.SetLevel(log.ERROR)
	b.Cleanup(func() { log.SetLevel(previous) })
	for _, mode := range []string{"vmess-aes", "vmess-xudp", "vless", "ss-udp", "ss-uot", "snell3", "anytls-uot", "mux-smux", "mux-h2mux", "mux-yamux", "hysteria2", "tuic4", "tuic5"} {
		for _, size := range []int{64, 1200} {
			b.Run(fmt.Sprintf("%s/bytes=%d", mode, size), func(b *testing.B) {
				tunnel := NewHttpTestTunnel()
				b.Cleanup(func() { tunnel.Close() })
				tunnel.HandleUDPPacketFn = func(p C.UDPPacket, m *C.Metadata) {
					if owned, ok := p.(interface{ TakeBuffer() *buf.Buffer }); ok {
						var target C.WriteBack = p
						if provider, ok := p.(interface{ WriteBackTarget() C.WriteBack }); ok {
							target = provider.WriteBackTarget()
						}
						packet := owned.TakeBuffer()
						p.Drop()
						destination := M.SocksaddrFromNetIP(m.AddrPort())
						var err error
						if writer, ok := target.(interface {
							WriteBackBuffer(*buf.Buffer, M.Socksaddr) error
						}); ok {
							err = writer.WriteBackBuffer(packet, destination)
						} else {
							_, err = target.WriteBack(packet.Bytes(), destination.UDPAddr())
							packet.Release()
						}
						if err != nil {
							b.Error(err)
						}
					} else {
						defer p.Drop()
						if _, err := p.WriteBack(p.Data(), m.UDPAddr()); err != nil {
							b.Error(err)
						}
					}
				}
				out := comparisonProxy(b, mode, tunnel)
				metadata := &C.Metadata{NetWork: C.UDP, DstIP: netip.MustParseAddr("127.0.0.9"), DstPort: 5353}
				conn, err := out.ListenPacketContext(context.Background(), metadata)
				if err != nil {
					b.Fatal(err)
				}
				manager := &statistic.Manager{}
				tracker := statistic.NewUDPTracker(conn, manager, metadata, nil, 0, 0, true)
				b.Cleanup(func() { tracker.Close() })
				writer := comparisonBatch(tracker)
				reader, ownedRead := any(tracker).(comparisonOwnedReader)
				destination := M.ParseSocksaddr("127.0.0.9:5353")
				targetAddr := destination.UDPAddr()
				var packets [4]*buf.Buffer
				destinations := [4]M.Socksaddr{destination, destination, destination, destination}
				payload := bytes.Repeat([]byte{7}, size)
				var sequence uint64
				roundTrip := func() {
					if err := tracker.SetReadDeadline(time.Now().Add(3 * time.Second)); err != nil {
						b.Fatal(err)
					}
					start := sequence
					sequence += 4
					for i := range packets {
						p := buf.NewSize(512 + size + 1024)
						p.Resize(512, 0)
						p.Write(payload)
						binary.BigEndian.PutUint64(p.Bytes(), start+uint64(i))
						packets[i] = p
					}
					if writer != nil {
						if n, err := writer.WritePacketBatchCount(packets[:], destinations[:]); err != nil || n != 4 {
							b.Fatalf("write %d: %v", n, err)
						}
					} else {
						for _, p := range packets {
							n, err := tracker.WriteTo(p.Bytes(), targetAddr)
							p.Release()
							if err != nil || n != size {
								b.Fatalf("legacy write %d: %v", n, err)
							}
						}
					}
					var seen [4]bool
					for i := 0; i < 4; i++ {
						var data []byte
						var release func()
						var source M.Socksaddr
						if ownedRead {
							p, d, e := reader.ReadPacketBuffer()
							if e != nil {
								b.Fatal(e)
							}
							data, release, source = p.Bytes(), p.Release, d
						} else {
							var addr net.Addr
							var e error
							data, release, addr, e = tracker.WaitReadFrom()
							if e != nil {
								b.Fatal(e)
							}
							source = M.SocksaddrFromNet(addr)
						}
						valid := len(data) == size && source == destination
						if valid {
							id := binary.BigEndian.Uint64(data)
							valid = id >= start && id < start+4 && bytes.Equal(data[8:], payload[8:])
							if valid {
								index := int(id - start)
								valid = !seen[index]
								seen[index] = true
							}
						}
						if release != nil {
							release()
						}
						if !valid {
							b.Fatal("reply loss, corruption, duplication or wrong source")
						}
					}
				}
				const warm = 10
				for i := 0; i < warm; i++ {
					roundTrip()
				}
				samples := make([]int64, b.N)
				b.ReportAllocs()
				b.SetBytes(int64(4 * size))
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					start := time.Now()
					roundTrip()
					samples[i] = time.Since(start).Nanoseconds()
				}
				b.StopTimer()
				sort.Slice(samples, func(i, j int) bool { return samples[i] < samples[j] })
				b.ReportMetric(float64(samples[(b.N-1)*95/100]), "p95-ns")
				b.ReportMetric(float64(samples[(b.N-1)*99/100]), "p99-ns")
				if writer != nil {
					b.ReportMetric(1, "batch-api")
				} else {
					b.ReportMetric(0, "batch-api")
				}
				upload, download := manager.Total()
				want := int64(b.N+warm) * int64(4*size)
				if upload != want || download != want {
					b.Fatalf("accounting %d/%d want %d", upload, download, want)
				}
			})
		}
	}
}
