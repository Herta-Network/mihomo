package deadline

import (
	"github.com/metacubex/mihomo/common/net/packet"
	N "github.com/metacubex/sing/common/network"
)

func (c *NetPacketConn) CreatePacketBatchWriterWithCount() (N.PacketBatchWriterWithCount, bool) {
	if c.isClosed() {
		return nil, false
	}
	return packet.CreatePacketBatchWriterWithCount(c.PacketConn)
}
