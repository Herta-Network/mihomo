package vless

import (
	"encoding/binary"
	"io"
	"net"

	"github.com/metacubex/mihomo/common/net/packetbatch"
	"github.com/metacubex/mihomo/common/pool"
	"github.com/metacubex/sing/common/buf"
	M "github.com/metacubex/sing/common/metadata"
)

type PacketConn struct {
	net.Conn
	rAddr  net.Addr
	writes packetbatch.State
}

func (c *PacketConn) WriteTo(p []byte, addr net.Addr) (int, error) {
	n, err := c.WritePacketBatchCount([]*buf.Buffer{buf.As(p)}, []M.Socksaddr{M.SocksaddrFromNet(addr)})
	if n == 1 {
		return len(p), err
	}
	return 0, err
}
func (c *PacketConn) WritePacketBatchCount(buffers []*buf.Buffer, destinations []M.Socksaddr) (int, error) {
	return c.writes.WriteStream(c.Conn, c.Conn, buffers, destinations, func(b *buf.Buffer, dest M.Socksaddr) (*buf.Buffer, error) {
		if err := packetbatch.CheckDestination(dest, M.SocksaddrFromNet(c.rAddr)); err != nil {
			return nil, err
		}
		return packetbatch.PrepareLengthPacket(b, dest)
	})
}
func (c *PacketConn) FrontHeadroom() int { return 2 }
func (c *PacketConn) Close() error       { return c.writes.Close(c.Conn) }

func (c *PacketConn) ReadFrom(b []byte) (int, net.Addr, error) {
	var length uint16
	err := binary.Read(c.Conn, binary.BigEndian, &length)
	if err != nil {
		return 0, nil, err
	}
	if len(b) < int(length) {
		return 0, nil, io.ErrShortBuffer
	}
	n, err := io.ReadFull(c.Conn, b[:length])
	return n, c.rAddr, err
}

func (c *PacketConn) WaitReadFrom() (data []byte, put func(), addr net.Addr, err error) {
	var length uint16
	err = binary.Read(c.Conn, binary.BigEndian, &length)
	if err != nil {
		return
	}
	readBuf := pool.Get(int(length))
	put = func() {
		_ = pool.Put(readBuf)
	}
	n, err := io.ReadFull(c.Conn, readBuf)
	if err != nil {
		put()
		put = nil
		return
	}
	data = readBuf[:n]
	addr = c.rAddr
	return
}
