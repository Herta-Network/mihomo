package inbound_test

import (
	"bytes"
	"context"
	"fmt"
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
	"github.com/stretchr/testify/require"
)

func TestInboundSnellBatch(t *testing.T) {
	for _, version := range []int{3, 4, 5, 6} {
		modes := []string{""}
		if version == 6 {
			modes = []string{"default", "unshaped", "unsafe-raw"}
		}
		for _, mode := range modes {
			obfsModes := []string{""}
			if version != 6 {
				obfsModes = []string{"", "http", "tls"}
			}
			for _, obfs := range obfsModes {
				for _, owned := range []bool{false, true} {
					t.Run(fmt.Sprintf("v%d/%s/%s/owned=%v", version, mode, obfs, owned), func(t *testing.T) {
						in, err := inbound.NewSnell(&inbound.SnellOption{BaseOption: inbound.BaseOption{NameStr: "snell-batch", Listen: "127.0.0.1", Port: "0"}, Psk: userUUID, Version: version, Version6Mode: mode, UDP: true, ObfsOpts: inbound.SnellObfsOption{Mode: obfs}})
						require.NoError(t, err)
						tunnel := NewHttpTestTunnel()
						defer tunnel.Close()
						tunnel.HandleUDPPacketFn = func(p C.UDPPacket, m *C.Metadata) {
							if owned {
								target := C.GetWriteBackTarget(C.NewPacketAdapter(p, m))
								buffer := p.(C.PacketBuffer).TakeBuffer()
								p.Drop()
								_, writeErr := target.WriteBack(buffer.Bytes(), m.UDPAddr())
								buffer.Release()
								if writeErr != nil {
									t.Error(writeErr)
								}
								return
							}
							defer p.Drop()
							if _, err := p.WriteBack(p.Data(), m.UDPAddr()); err != nil {
								t.Error(err)
							}
						}
						require.NoError(t, in.Listen(tunnel))
						defer in.Close()
						addr, err := netip.ParseAddrPort(in.Address())
						require.NoError(t, err)
						out, err := outbound.NewSnell(outbound.SnellOption{BasicOption: outbound.BasicOption{DialerForAPI: tunnel.NewDialer()}, Name: "snell-batch", Server: addr.Addr().String(), Port: int(addr.Port()), Psk: userUUID, Version: version, Version6Mode: mode, UDP: true, ObfsOpts: map[string]any{"mode": obfs}})
						require.NoError(t, err)
						defer out.Close()
						ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
						defer cancel()
						metadata := &C.Metadata{NetWork: C.UDP, DstIP: remoteAddr, DstPort: 5353}
						conn, err := out.ListenPacketContext(ctx, metadata)
						require.NoError(t, err)
						manager := &statistic.Manager{}
						tracker := statistic.NewUDPTracker(conn, manager, metadata, nil, 0, 0, true)
						defer tracker.Close()
						require.NoError(t, tracker.SetReadDeadline(time.Now().Add(5*time.Second)))
						writer, ok := N.CreatePacketBatchWriterWithCount(tracker)
						require.True(t, ok, "missing capability through outbound/deadline/ref/traffic wrappers")
						payloads := [][]byte{nil, []byte("one"), bytes.Repeat([]byte{9}, 1200), []byte("tail")}
						if version == 3 {
							payloads[3] = bytes.Repeat([]byte{7}, 16000)
						} else if version == 6 {
							payloads[3] = bytes.Repeat([]byte{7}, 60000)
						}
						var total int64
						buffers := make([]*buf.Buffer, 4)
						dests := make([]M.Socksaddr, 4)
						for i, p := range payloads {
							buffers[i] = buf.As(p)
							dests[i] = M.ParseSocksaddr("127.0.0.9:5353")
							if i%2 != 0 {
								dests[i] = M.ParseSocksaddr("[::1]:5354")
							}
							total += int64(len(p))
						}
						n, err := writer.WritePacketBatchCount(buffers, dests)
						require.NoError(t, err)
						require.Equal(t, 4, n)
						for i, want := range payloads {

							got, put, addr, err := tracker.WaitReadFrom()
							require.NoError(t, err)
							require.True(t, bytes.Equal(want, got))
							require.Equal(t, dests[i].String(), addr.String())
							if put != nil {
								put()
							}
						}
						upload, download := manager.Total()
						require.Equal(t, total, upload)
						require.Equal(t, total, download)
					})
				}
			}
		}
	}
}
