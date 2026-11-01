package trojan

import (
	"encoding/binary"
	"io"
	"net"

	"github.com/metacubex/mihomo/transport/socks5"
	"github.com/metacubex/sing/common/buf"
	M "github.com/metacubex/sing/common/metadata"
	N "github.com/metacubex/sing/common/network"
)

func (pc *PacketConn) FrontHeadroom() int { return M.MaxSocksaddrLength + 4 }
func (pc *PacketConn) WritePacketBatchCount(buffers []*buf.Buffer, destinations []M.Socksaddr) (int, error) {
	return pc.writes.WriteStream(pc.Conn, pc.Conn, buffers, destinations, preparePacket)
}
func preparePacket(buffer *buf.Buffer, destination M.Socksaddr) (*buf.Buffer, error) {
	if buffer.Len() > maxLength {
		return nil, io.ErrShortBuffer
	}
	var addr net.Addr = destination
	if destination.IsIP() {
		addr = destination.UDPAddr()
	}
	address := socks5.ParseAddrToSocksAddr(addr)
	if len(address) == 0 {
		return nil, io.ErrUnexpectedEOF
	}
	size := buffer.Len()
	buffer = (N.ReadWaitOptions{FrontHeadroom: len(address) + 4}).Copy(buffer)
	header := buffer.ExtendHeader(len(address) + 4)
	copy(header, address)
	binary.BigEndian.PutUint16(header[len(address):], uint16(size))
	copy(header[len(address)+2:], crlf)
	return buffer, nil
}
func (pc *PacketConn) writePacketCount(p []byte, addr net.Addr) (int, error) {
	n, err := pc.WritePacketBatchCount([]*buf.Buffer{buf.As(p)}, []M.Socksaddr{M.SocksaddrFromNet(addr)})
	if n == 1 {
		return len(p), err
	}
	return 0, err
}
func (pc *PacketConn) Close() error { return pc.writes.Close(pc.Conn) }
