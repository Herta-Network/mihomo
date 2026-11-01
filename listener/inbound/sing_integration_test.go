package inbound_test

import (
	"bytes"
	"context"
	"fmt"
	"net"
	"net/netip"
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
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Exercise the actual protocol, NAT, enhanced/deadline/ref wrappers and traffic
// tracker together. Prefetching must not bypass decryption or reorder sources.
func TestSingPacketIntegration(t *testing.T) {
	for _, test := range []struct {
		name, cipher, password string
		uot                    int
	}{
		{"none-udp", "none", "", 0},
		{"stream-udp", "chacha20-ietf", shadowsocksPassword32, 0},
		{"aead-udp", "aes-128-gcm", shadowsocksPassword16, 0},
		{"2022-udp", "2022-blake3-aes-128-gcm", shadowsocksPassword16, 0},
		{"uot-v1", "aes-128-gcm", shadowsocksPassword16, 1},
		{"uot-v2", "aes-128-gcm", shadowsocksPassword16, 2},
	} {
		t.Run(test.name, func(t *testing.T) {
			tunnel := &TestTunnel{
				HandleTCPConnFn: func(conn net.Conn, _ *C.Metadata) { conn.Close(); t.Error("unexpected TCP forwarding") },
				HandleUDPPacketFn: func(p C.UDPPacket, m *C.Metadata) {
					defer p.Drop()
					_, err := p.WriteBack(p.Data(), m.UDPAddr())
					assert.NoError(t, err)
				},
			}
			in, err := inbound.NewShadowSocks(&inbound.ShadowSocksOption{
				BaseOption: inbound.BaseOption{NameStr: "sing-integration", Listen: "127.0.0.1", Port: "0"},
				Cipher:     test.cipher, Password: test.password, UDP: test.uot == 0,
			})
			require.NoError(t, err)
			require.NoError(t, in.Listen(tunnel))
			defer in.Close()
			addresses := strings.Split(in.Address(), ",")
			address := addresses[0]
			if test.uot == 0 {
				address = addresses[len(addresses)-1]
			}
			server, err := netip.ParseAddrPort(address)
			require.NoError(t, err)
			out, err := outbound.NewShadowSocks(outbound.ShadowSocksOption{
				Name: "sing-integration", Server: server.Addr().String(), Port: int(server.Port()),
				Cipher: test.cipher, Password: test.password, UDP: true, UDPOverTCP: test.uot != 0, UDPOverTCPVersion: test.uot,
			})
			require.NoError(t, err)
			defer out.Close()
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			metadata := &C.Metadata{NetWork: C.UDP, DstIP: netip.MustParseAddr("127.0.0.9"), DstPort: 5353}
			conn, err := out.ListenPacketContext(ctx, metadata)
			require.NoError(t, err)
			manager := &statistic.Manager{}
			tracker := statistic.NewUDPTracker(conn, manager, metadata, nil, 0, 0, true)
			defer tracker.Close()
			require.NoError(t, tracker.SetReadDeadline(time.Now().Add(5*time.Second)))
			payloads := [][]byte{nil, []byte("one"), bytes.Repeat([]byte{0x7a}, 4096), []byte("three"), nil, []byte("five")}
			var total int64
			targets := make([]*net.UDPAddr, len(payloads))
			batchWriter, canBatch := N.CreatePacketBatchWriterWithCount(tracker)
			require.True(t, canBatch, "packet batch capability lost in wrapper chain")
			buffers := make([]*buf.Buffer, 0, len(payloads))
			destinations := make([]M.Socksaddr, 0, len(payloads))
			for i, payload := range payloads {
				targets[i] = net.UDPAddrFromAddrPort(netip.MustParseAddrPort(fmt.Sprintf("127.0.0.9:%d", 5353+i)))
				total += int64(len(payload))
				if canBatch {
					buffers = append(buffers, buf.As(payload))
					destinations = append(destinations, M.SocksaddrFromNet(targets[i]))
				} else {
					n, err := tracker.WriteTo(payload, targets[i])
					require.NoError(t, err)
					require.Equal(t, len(payload), n)
				}
			}
			if canBatch {
				n, err := batchWriter.WritePacketBatchCount(buffers, destinations)
				require.NoError(t, err)
				require.Equal(t, len(payloads), n)
			}

			for i, want := range payloads {
				var got []byte
				var addr net.Addr
				if i%2 == 0 {
					var put func()
					data, release, from, err := tracker.WaitReadFrom()
					require.NoError(t, err)
					got, put, addr = bytes.Clone(data), release, from
					if put != nil {
						put()
					}
				} else {
					data := make([]byte, 65535)
					n, from, err := tracker.ReadFrom(data)
					require.NoError(t, err)
					got, addr = data[:n], from
				}
				require.True(t, bytes.Equal(want, got), "packet %d: got %d bytes", i, len(got))
				require.Equal(t, targets[i].String(), addr.String())
			}
			require.Equal(t, total, tracker.Info().UploadTotal.Load())
			require.Equal(t, total, tracker.Info().DownloadTotal.Load())
			upload, download := manager.Total()
			require.Equal(t, total, upload)
			require.Equal(t, total, download)
		})
	}
}
