package packetbatch

import (
	"encoding/binary"
	"errors"
	"io"
	"strings"

	"github.com/metacubex/sing/common/buf"
	M "github.com/metacubex/sing/common/metadata"
	N "github.com/metacubex/sing/common/network"
)

var ErrDestinationMismatch = errors.New("UDP packet dropped due to mismatched remote address")

func CheckDestination(destination, bound M.Socksaddr) error {
	if destination.Port == bound.Port {
		if destination.IsIP() && bound.IsIP() && destination.Addr.Unmap() == bound.Addr.Unmap() {
			return nil
		}
		if destination.IsFqdn() && bound.IsFqdn() && strings.EqualFold(destination.Fqdn, bound.Fqdn) {
			return nil
		}
	}
	return ErrDestinationMismatch
}
func PrepareLengthPacket(buffer *buf.Buffer, _ M.Socksaddr) (*buf.Buffer, error) {
	if buffer.Len() > 65535 {
		return nil, io.ErrShortBuffer
	}
	size := buffer.Len()
	buffer = (N.ReadWaitOptions{FrontHeadroom: 2}).Copy(buffer)
	binary.BigEndian.PutUint16(buffer.ExtendHeader(2), uint16(size))
	return buffer, nil
}
