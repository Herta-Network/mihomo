package packet

import (
	"net"

	"github.com/metacubex/sing/common/buf"
	M "github.com/metacubex/sing/common/metadata"
	N "github.com/metacubex/sing/common/network"
)

type SingPacketConn = N.NetPacketConn

type EnhanceSingPacketConn interface {
	SingPacketConn
	EnhancePacketConn
}

type enhanceSingPacketConn struct {
	SingPacketConn
	readState
}

func (c *enhanceSingPacketConn) WaitReadFrom() ([]byte, func(), net.Addr, error) {
	return c.readState.waitReadFrom(c.SingPacketConn)
}

func (c *enhanceSingPacketConn) ReadFrom(p []byte) (int, net.Addr, error) {
	return c.readState.readFrom(c.SingPacketConn, p)
}

func (c *enhanceSingPacketConn) ReadPacket(buffer *buf.Buffer) (M.Socksaddr, error) {
	packet, destination, err := c.readState.read(c.SingPacketConn)
	if err != nil {
		return destination, err
	}
	defer packet.Release()
	_, err = buffer.Write(packet.Bytes())
	return destination, err
}

func (c *enhanceSingPacketConn) Close() error {
	err := c.SingPacketConn.Close()
	c.readState.close()
	return err
}

func (c *enhanceSingPacketConn) Upstream() any {
	return c.SingPacketConn
}

func (c *enhanceSingPacketConn) WriterReplaceable() bool {
	return true
}

func (c *enhanceSingPacketConn) ReaderReplaceable() bool {
	return false
}

func newEnhanceSingPacketConn(conn SingPacketConn) *enhanceSingPacketConn {
	return &enhanceSingPacketConn{SingPacketConn: conn}
}
