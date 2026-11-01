package v5

import (
	"encoding/binary"
	"fmt"
	"io"
)

// parsePacketBytes borrows address and payload from one immutable QUIC datagram.
// The caller must retain the datagram while consuming or reassembling the packet.
func parsePacketBytes(data []byte) (c Packet, err error) {
	if len(data) < 11 {
		return c, io.ErrUnexpectedEOF
	}
	c.CommandHead = CommandHead{VER: data[0], TYPE: CommandType(data[1])}
	if c.VER != VER || c.TYPE != PacketType {
		return Packet{}, fmt.Errorf("invalid packet command")
	}
	c.ASSOC_ID = binary.BigEndian.Uint16(data[2:])
	c.PKT_ID = binary.BigEndian.Uint16(data[4:])
	c.FRAG_TOTAL, c.FRAG_ID = data[6], data[7]
	c.SIZE = binary.BigEndian.Uint16(data[8:])
	if c.FRAG_TOTAL == 0 || c.FRAG_ID >= c.FRAG_TOTAL {
		return Packet{}, fmt.Errorf("invalid packet fragments")
	}
	size := int(c.SIZE)
	offset := 10
	c.ADDR.TYPE = data[offset]
	offset++
	addressSize := 0
	switch c.ADDR.TYPE {
	case AtypIPv4:
		addressSize = 4
	case AtypIPv6:
		addressSize = 16
	case AtypDomainName:
		if offset >= len(data) {
			return Packet{}, io.ErrUnexpectedEOF
		}
		addressSize = 1 + int(data[offset])
	case AtypNone:
		break
	default:
		return Packet{}, fmt.Errorf("invalid packet address type")
	}
	if len(data)-offset < addressSize {
		return Packet{}, io.ErrUnexpectedEOF
	}
	c.ADDR.ADDR = data[offset : offset+addressSize]
	offset += addressSize
	if c.ADDR.TYPE != AtypNone {
		if len(data)-offset < 2 {
			return Packet{}, io.ErrUnexpectedEOF
		}
		c.ADDR.PORT = binary.BigEndian.Uint16(data[offset:])
		offset += 2
	}
	if len(data)-offset < size {
		return Packet{}, io.ErrUnexpectedEOF
	}
	if len(data)-offset != size {
		return Packet{}, fmt.Errorf("trailing packet data")
	}
	c.DATA = data[offset:]
	return c, nil
}
