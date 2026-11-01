package inbound_test

import (
	"bytes"
	"context"
	"net"
	"net/netip"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/metacubex/mihomo/adapter/outbound"
	N "github.com/metacubex/mihomo/common/net"
	C "github.com/metacubex/mihomo/constant"
	"github.com/metacubex/mihomo/listener/inbound"
	"github.com/metacubex/mihomo/tunnel/statistic"
	"github.com/metacubex/sing/common/buf"
	M "github.com/metacubex/sing/common/metadata"
	"github.com/stretchr/testify/require"
)

func TestProtocolPacketBatchIntegration(t *testing.T) {
	for _, name := range []string{"socks5", "trojan", "vless", "vless-xudp", "vless-packetaddr", "vmess-auto", "vmess-none", "vmess-xudp", "vmess-packetaddr", "sudoku", "trust-h2", "trust-quic", "mux-h2mux", "mux-smux", "mux-yamux", "hysteria2", "tuic-v4", "tuic-v5", "vless-ws", "vless-ws-early", "vless-grpc", "vmess-ws", "vmess-ws-early", "vmess-grpc"} {
		t.Run(name, func(t *testing.T) {
			base := inbound.BaseOption{NameStr: "batch-" + name, Listen: "127.0.0.1", Port: "0"}
			var in C.InboundListener
			var err error
			switch {
			case name == "socks5":
				probe, probeErr := net.Listen("tcp4", "127.0.0.1:0")
				require.NoError(t, probeErr)
				base.Port = strconv.Itoa(probe.Addr().(*net.TCPAddr).Port)
				require.NoError(t, probe.Close())
				in, err = inbound.NewSocks(&inbound.SocksOption{BaseOption: base, UDP: true, Users: inbound.AuthUsers{}})
			case name == "trojan":
				in, err = inbound.NewTrojan(&inbound.TrojanOption{BaseOption: base, Users: []inbound.TrojanUser{{Username: "test", Password: userUUID}}, Certificate: tlsCertificate, PrivateKey: tlsPrivateKey})
			case strings.HasPrefix(name, "vless") || strings.HasPrefix(name, "mux-"):
				options := inbound.VlessOption{BaseOption: base, AllowInsecure: true, Users: []inbound.VlessUser{{Username: "test", UUID: userUUID}}}
				if strings.Contains(name, "-ws") {
					options.WsPath = "/batch"
				}
				if strings.Contains(name, "-grpc") {
					options.GrpcServiceName = "Batch"
					options.Certificate = tlsCertificate
					options.PrivateKey = tlsPrivateKey
				}
				in, err = inbound.NewVless(&options)
			case strings.HasPrefix(name, "vmess"):
				options := inbound.VmessOption{BaseOption: base, Users: []inbound.VmessUser{{Username: "test", UUID: userUUID}}}
				if strings.Contains(name, "-ws") {
					options.WsPath = "/batch"
				}
				if strings.Contains(name, "-grpc") {
					options.GrpcServiceName = "Batch"
					options.Certificate = tlsCertificate
					options.PrivateKey = tlsPrivateKey
				}
				in, err = inbound.NewVmess(&options)
			case name == "hysteria2":
				in, err = inbound.NewHysteria2(&inbound.Hysteria2Option{BaseOption: base, Users: map[string]string{"test": userUUID}, Certificate: tlsCertificate, PrivateKey: tlsPrivateKey})
			case strings.HasPrefix(name, "tuic-"):
				in, err = inbound.NewTuic(&inbound.TuicOption{BaseOption: base, Token: []string{userUUID}, Users: map[string]string{userUUID: userUUID}, Certificate: tlsCertificate, PrivateKey: tlsPrivateKey, CongestionController: "cubic"})
			case name == "sudoku":
				in, err = inbound.NewSudoku(&inbound.SudokuOption{BaseOption: base, Key: "packet-batch-test"})
			default:
				options := inbound.TrustTunnelOption{BaseOption: base, Users: []inbound.AuthUser{{Username: "test", Password: userUUID}}, Certificate: tlsCertificate, PrivateKey: tlsPrivateKey}
				if name == "trust-quic" {
					options.Network = []string{"udp"}
				}
				in, err = inbound.NewTrustTunnel(&options)
			}
			require.NoError(t, err)
			tunnel := NewHttpTestTunnel()
			defer tunnel.Close()
			echoResults := make(chan error, 64)
			tunnel.HandleUDPPacketFn = func(p C.UDPPacket, m *C.Metadata) {
				defer p.Drop()
				_, err := p.WriteBack(p.Data(), m.UDPAddr())
				select {
				case echoResults <- err:
				default:
				}
			}
			require.NoError(t, in.Listen(tunnel))
			defer in.Close()
			addr, err := netip.ParseAddrPort(strings.Split(in.Address(), ",")[0])
			require.NoError(t, err)
			basic := outbound.BasicOption{DialerForAPI: tunnel.NewDialer(), TunnelForAPI: tunnel}
			var out outbound.ProxyAdapter
			switch {
			case name == "socks5":
				out, err = outbound.NewSocks5(outbound.Socks5Option{BasicOption: basic, Name: name, Server: addr.Addr().String(), Port: int(addr.Port()), UDP: true})
			case name == "trojan":
				out, err = outbound.NewTrojan(outbound.TrojanOption{BasicOption: basic, Name: name, Server: addr.Addr().String(), Port: int(addr.Port()), Password: userUUID, Fingerprint: tlsFingerprint, UDP: true})
			case strings.HasPrefix(name, "vless") || strings.HasPrefix(name, "mux-"):
				options := outbound.VlessOption{BasicOption: basic, Name: name, Server: addr.Addr().String(), Port: int(addr.Port()), UUID: userUUID, UDP: true, XUDP: name == "vless-xudp", PacketAddr: name == "vless-packetaddr"}
				if strings.Contains(name, "-ws") {
					options.Network = "ws"
					options.WSOpts.Path = "/batch"
				}
				if strings.HasSuffix(name, "-early") {
					options.WSOpts.MaxEarlyData = 2048
					options.WSOpts.EarlyDataHeaderName = "Sec-WebSocket-Protocol"
				}
				if strings.Contains(name, "-grpc") {
					options.Network = "grpc"
					options.TLS = true
					options.Fingerprint = tlsFingerprint
					options.GrpcOpts.GrpcServiceName = "Batch"
				}
				out, err = outbound.NewVless(options)
			case strings.HasPrefix(name, "vmess"):
				cipher := "auto"
				if name == "vmess-none" {
					cipher = "none"
				}
				options := outbound.VmessOption{BasicOption: basic, Name: name, Server: addr.Addr().String(), Port: int(addr.Port()), UUID: userUUID, Cipher: cipher, UDP: true, XUDP: name == "vmess-xudp", PacketAddr: name == "vmess-packetaddr"}
				if strings.Contains(name, "-ws") {
					options.Network = "ws"
					options.WSOpts.Path = "/batch"
				}
				if strings.HasSuffix(name, "-early") {
					options.WSOpts.MaxEarlyData = 2048
					options.WSOpts.EarlyDataHeaderName = "Sec-WebSocket-Protocol"
				}
				if strings.Contains(name, "-grpc") {
					options.Network = "grpc"
					options.TLS = true
					options.Fingerprint = tlsFingerprint
					options.GrpcOpts.GrpcServiceName = "Batch"
				}
				out, err = outbound.NewVmess(options)
			case name == "hysteria2":
				out, err = outbound.NewHysteria2(outbound.Hysteria2Option{BasicOption: basic, Name: name, Server: addr.Addr().String(), Port: int(addr.Port()), Password: userUUID, Fingerprint: tlsFingerprint})
			case strings.HasPrefix(name, "tuic-"):
				options := outbound.TuicOption{BasicOption: basic, Name: name, Server: addr.Addr().String(), Port: int(addr.Port()), Fingerprint: tlsFingerprint, CongestionController: "cubic"}
				if name == "tuic-v4" {
					options.Token = userUUID
				} else {
					options.UUID = userUUID
					options.Password = userUUID
				}
				out, err = outbound.NewTuic(options)
			case name == "sudoku":
				out, err = outbound.NewSudoku(outbound.SudokuOption{BasicOption: basic, Name: name, Server: addr.Addr().String(), Port: int(addr.Port()), Key: "packet-batch-test"})
			default:
				out, err = outbound.NewTrustTunnel(outbound.TrustTunnelOption{BasicOption: basic, Name: name, Server: addr.Addr().String(), Port: int(addr.Port()), UserName: "test", Password: userUUID, Fingerprint: tlsFingerprint, Quic: name == "trust-quic", UDP: true})
			}
			require.NoError(t, err)
			defer out.Close()
			if strings.HasPrefix(name, "mux-") {
				out, err = outbound.NewSingMux(outbound.SingMuxOption{Enabled: true, Protocol: strings.TrimPrefix(name, "mux-")}, &notCloseProxyAdapter{out})
				require.NoError(t, err)
				defer out.Close()
			}
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			metadata := &C.Metadata{NetWork: C.UDP, DstIP: netip.MustParseAddr("127.0.0.9"), DstPort: 5353}
			conn, err := out.ListenPacketContext(ctx, metadata)
			require.NoError(t, err)
			manager := &statistic.Manager{}
			tracker := statistic.NewUDPTracker(conn, manager, metadata, nil, 0, 0, true)
			defer tracker.Close()
			require.NoError(t, tracker.SetReadDeadline(time.Now().Add(10*time.Second)))
			writer, ok := N.CreatePacketBatchWriterWithCount(tracker)
			require.True(t, ok, "capability missing through production wrappers")
			bound := (strings.HasPrefix(name, "vless") || strings.HasPrefix(name, "vmess")) && !strings.Contains(name, "xudp") && !strings.Contains(name, "packetaddr")
			payloads := [][]byte{nil, []byte("one"), bytes.Repeat([]byte{0x5a}, 60000), []byte("last")}
			// Darwin defaults to a small raw UDP send buffer; stream carriers test 60 KB.
			if name == "socks5" || name == "hysteria2" || name == "tuic-v5" {
				payloads[2] = bytes.Repeat([]byte{0x5a}, 4096)
			}
			if name == "tuic-v4" {
				payloads[2] = bytes.Repeat([]byte{0x5a}, 1200)
			}
			if name == "vmess-none" {
				n, err := writer.WritePacketBatchCount([]*buf.Buffer{buf.NewSize(1)}, []M.Socksaddr{M.SocksaddrFromNet(metadata.UDPAddr())})
				require.Error(t, err)
				require.Zero(t, n)
				payloads[0] = []byte("nonempty")
			}
			var total int64
			buffers := make([]*buf.Buffer, len(payloads))
			destinations := make([]M.Socksaddr, len(payloads))
			for i, payload := range payloads {
				buffers[i] = buf.As(payload)
				destinations[i] = M.SocksaddrFromNet(metadata.UDPAddr())
				if !bound && i%2 != 0 {
					destinations[i] = M.ParseSocksaddr("[::1]:5354")
				}
				total += int64(len(payload))
			}
			n, err := writer.WritePacketBatchCount(buffers, destinations)
			require.NoError(t, err)
			require.Equal(t, len(payloads), n)
			seen := make([]bool, len(payloads))
			for range payloads {
				got, release, from, err := tracker.WaitReadFrom()
				require.NoError(t, err)
				index := -1
				for i, payload := range payloads {
					if !seen[i] && bytes.Equal(payload, got) {
						index = i
						break
					}
				}
				if release != nil {
					defer release()
				}
				require.NotEqual(t, -1, index, "unexpected or fragmented packet, length %d", len(got))
				require.Equal(t, destinations[index].String(), from.String())
				seen[index] = true
			}
			for range payloads {
				select {
				case echoErr := <-echoResults:
					require.NoError(t, echoErr)
				case <-time.After(time.Second):
					t.Fatal("echo handler did not finish")
				}
			}
			up, down := manager.Total()
			require.Equal(t, total, up)
			require.Equal(t, total, down)
			if name == "hysteria2" || strings.HasPrefix(name, "tuic-") || name == "trust-quic" {
				started := make(chan struct{})
				done := make(chan error, 1)
				go func() {
					for i := 0; i < 128; i++ {
						_, writeErr := writer.WritePacketBatchCount([]*buf.Buffer{buf.As([]byte("close-check")), buf.As([]byte("second"))}, []M.Socksaddr{destinations[0], destinations[0]})
						if i == 0 {
							close(started)
						}
						if writeErr != nil {
							done <- writeErr
							return
						}
					}
					done <- nil
				}()
				select {
				case <-started:
				case <-time.After(time.Second):
					t.Fatal("write loop did not start")
				}
				require.NoError(t, tracker.Close())
				select {
				case <-done:
				case <-time.After(3 * time.Second):
					t.Fatal("Close did not unblock packet writer")
				}
			}

		})
	}
}
