package packetbatch

import (
	"fmt"
	"net"
	"testing"

	vmess "github.com/metacubex/sing-vmess"
	"github.com/metacubex/sing-vmess/packetaddr"
	"github.com/metacubex/sing/common/buf"
	M "github.com/metacubex/sing/common/metadata"
	N "github.com/metacubex/sing/common/network"
)

type discardVMessStream struct{ net.Conn }

func (*discardVMessStream) Write(p []byte) (int, error) { return len(p), nil }
func (*discardVMessStream) RemoteAddr() net.Addr        { return M.ParseSocksaddr("127.0.0.1:443") }
func (*discardVMessStream) Close() error                { return nil }

// Uses the same StreamEncoder and codec stack as the VMess outbound, with a
// discard transport to isolate encoding CPU and allocation from network noise.
func BenchmarkVMessOwnedFrames(b *testing.B) {
	for _, mode := range []string{"udp", "xudp", "packetaddr", "legacy"} {
		for _, size := range []int{1200, 60000} {
			for _, count := range []int{1, 8} {
				b.Run(fmt.Sprintf("%s/size=%d/batch=%d", mode, size, count), func(b *testing.B) {
					target := M.ParseSocksaddr("1.2.3.4:5353")
					cipher := "aes-128-gcm"
					if mode == "legacy" {
						cipher = "aes-128-cfb"
					}
					client, err := vmess.NewClient("00000000-0000-0000-0000-000000000001", cipher, 0)
					if err != nil {
						b.Fatal(err)
					}
					conn, err := NewStreamEncoder(&discardVMessStream{}, func(raw net.Conn) (net.Conn, error) {
						if mode == "xudp" {
							return client.DialXUDPPacketConn(raw, [8]byte{}, target)
						}
						if mode == "packetaddr" {
							inner, err := client.DialPacketConn(raw, M.ParseSocksaddrHostPort(packetaddr.SeqPacketMagicAddress, 443))
							if err != nil {
								return nil, err
							}
							return packetaddr.NewBindConn(inner), nil
						}
						return client.DialPacketConn(raw, target)
					}, StreamEncoderOptions{Destination: target})
					if err != nil {
						b.Fatal(err)
					}
					defer conn.Close()
					options := N.NewReadWaitOptions(nil, conn)
					buffers := make([]*buf.Buffer, count)
					destinations := make([]M.Socksaddr, count)
					for i := range destinations {
						destinations[i] = target
					}
					b.SetBytes(int64(size * count))
					b.ReportAllocs()
					b.ResetTimer()
					for i := 0; i < b.N; i++ {
						for j := range buffers {
							buffers[j] = options.NewBufferSize(size)
							buffers[j].Extend(size)
							options.PostReturn(buffers[j])
						}
						if count == 1 {
							if err = conn.WritePacket(buffers[0], target); err != nil {
								b.Fatal(err)
							}
						} else if _, err = conn.WritePacketBatchCount(buffers, destinations); err != nil {
							b.Fatal(err)
						}
					}
				})
			}
		}
	}
}
