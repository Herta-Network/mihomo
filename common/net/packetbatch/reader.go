package packetbatch

import (
	"io"
	"net"

	P "github.com/metacubex/mihomo/common/net/packet"
	"github.com/metacubex/sing/common/buf"
	M "github.com/metacubex/sing/common/metadata"
	N "github.com/metacubex/sing/common/network"
)

// Keep the original wait-reader and one owner for any prefetched packets when
// adding a write adapter. This also preserves SSR/QUIC zero-copy WaitReadFrom.
type packetReader struct{ P.EnhancePacketConn }

func newPacketReader(conn net.PacketConn) packetReader {
	return packetReader{P.NewEnhancePacketConn(conn)}
}
func (r packetReader) readPacket(buffer *buf.Buffer) (M.Socksaddr, error) {
	data, release, addr, err := r.WaitReadFrom()
	if release != nil {
		defer release()
	}
	if err != nil {
		return M.Socksaddr{}, err
	}
	_, err = buffer.Write(data)
	return M.SocksaddrFromNet(addr), err
}

// The generic sing net.PacketConn adapter converts every target to UDPAddr.
// Preserve domain destinations for codecs such as SOCKS5 and Mieru instead.
func newPacketCodec(conn net.PacketConn) N.NetPacketConn {
	if packetConn, ok := conn.(N.NetPacketConn); ok {
		return packetConn
	}
	return &addressPacketConn{PacketConn: conn}
}

type addressPacketConn struct{ net.PacketConn }

func (c *addressPacketConn) ReadPacket(buffer *buf.Buffer) (M.Socksaddr, error) {
	_, addr, err := buffer.ReadPacketFrom(c.PacketConn)
	return M.SocksaddrFromNet(addr), err
}
func (c *addressPacketConn) WritePacket(buffer *buf.Buffer, dest M.Socksaddr) error {
	defer buffer.Release()
	var addr net.Addr = dest
	if dest.IsIP() {
		addr = dest.UDPAddr()
	}
	n, err := c.PacketConn.WriteTo(buffer.Bytes(), addr)
	if n < 0 || n > buffer.Len() {
		return io.ErrShortWrite
	}
	if n != buffer.Len() && err == nil {
		err = io.ErrShortWrite
	}
	return err
}
func (c *addressPacketConn) Upstream() any           { return c.PacketConn }
func (c *addressPacketConn) ReaderReplaceable() bool { return true }
func (c *addressPacketConn) WriterReplaceable() bool { return false }
