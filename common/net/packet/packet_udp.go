package packet

import (
	"net"

	"github.com/metacubex/sing/common/bufio"
	N "github.com/metacubex/sing/common/network"
)

type enhanceUDPConn struct {
	net.PacketConn
	source N.PacketReader
	readState
}

func (c *enhanceUDPConn) WaitReadFrom() ([]byte, func(), net.Addr, error) {
	return c.readState.waitReadFrom(c.source)
}

func (c *enhanceUDPConn) ReadFrom(p []byte) (int, net.Addr, error) {
	return c.readState.readFrom(c.source, p)
}

func (c *enhanceUDPConn) Close() error {
	err := c.PacketConn.Close()
	c.readState.close()
	return err
}

func (c *enhanceUDPConn) Upstream() any           { return c.PacketConn }
func (c *enhanceUDPConn) WriterReplaceable() bool { return true }

// Reads must consume the pending batch before accessing the underlying socket.
func (c *enhanceUDPConn) ReaderReplaceable() bool { return false }

func newEnhanceUDPConn(conn *net.UDPConn) *enhanceUDPConn {
	return &enhanceUDPConn{PacketConn: conn, source: bufio.NewPacketConn(conn)}
}
