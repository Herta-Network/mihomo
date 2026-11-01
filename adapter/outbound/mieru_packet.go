package outbound

import (
	"fmt"
	"io"
	"net"

	mierucommon "github.com/enfein/mieru/v3/apis/common"
	"github.com/metacubex/mihomo/common/pool"
	"github.com/metacubex/mihomo/transport/socks5"
)

// Keep Mieru's packet-over-stream framing and use the existing SOCKS UDP codec.
// The SDK UDP wrapper returns io.EOF for an empty decoded payload; a valid
// empty datagram must remain distinct from EOF before the next stream frame.
type mieruPacketConn struct{ net.PacketConn }

func newMieruPacketConn(conn net.Conn) net.PacketConn {
	return &mieruPacketConn{PacketConn: mierucommon.NewPacketOverStreamTunnel(conn)}
}
func (c *mieruPacketConn) WriteTo(payload []byte, addr net.Addr) (int, error) {
	packet, err := socks5.EncodeUDPPacket(socks5.ParseAddrToSocksAddr(addr), payload)
	if err != nil {
		return 0, err
	}
	n, err := c.PacketConn.WriteTo(packet, addr)
	if n != len(packet) && err == nil {
		err = io.ErrShortWrite
	}
	if err != nil {
		return 0, err
	}
	return len(payload), nil
}
func (c *mieruPacketConn) WaitReadFrom() (data []byte, release func(), addr net.Addr, err error) {
	buffer := pool.Get(65535)
	n, _, err := c.PacketConn.ReadFrom(buffer)
	if err != nil {
		_ = pool.Put(buffer)
		return nil, nil, nil, err
	}
	target, payload, err := socks5.DecodeUDPPacket(buffer[:n])
	if err != nil {
		_ = pool.Put(buffer)
		return nil, nil, nil, err
	}
	udpAddr := target.UDPAddr()
	if udpAddr == nil {
		_ = pool.Put(buffer)
		return nil, nil, nil, fmt.Errorf("Mieru UDP response requires an IP address")
	}
	return payload, func() { _ = pool.Put(buffer) }, udpAddr, nil
}
func (c *mieruPacketConn) ReadFrom(p []byte) (int, net.Addr, error) {
	data, release, addr, err := c.WaitReadFrom()
	if release != nil {
		defer release()
	}
	if err != nil {
		return 0, nil, err
	}
	return copy(p, data), addr, nil
}
func (c *mieruPacketConn) Upstream() any           { return c.PacketConn }
func (c *mieruPacketConn) ReaderReplaceable() bool { return false }
func (c *mieruPacketConn) WriterReplaceable() bool { return false }
