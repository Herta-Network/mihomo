package outbound

import (
	N "github.com/metacubex/mihomo/common/net"
	"github.com/metacubex/sing/common/buf"
	M "github.com/metacubex/sing/common/metadata"
)

func (c *packetConn) ReadPacketBuffer() (*buf.Buffer, M.Socksaddr, error) {
	return N.ReadPacketBuffer(c.EnhancePacketConn)
}
