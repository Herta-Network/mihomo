package deadline

import (
	"net"

	"github.com/metacubex/sing/common/buf"
	M "github.com/metacubex/sing/common/metadata"
)

// The result channel is shared by every read API. A timed-out read may publish
// after the caller switches APIs; consume that result instead of waiting for a
// second reader of its original type to arrive.
func packetResultBuffer(result any) (buffer *buf.Buffer, destination M.Socksaddr, err error) {
	switch result := result.(type) {
	case *singReadResult:
		buffer, destination, err = result.buffer, result.destination, result.err
	case *singWaitReadResult:
		buffer, destination, err = result.buffer, result.destination, result.err
	case *readResult:
		err = result.err
		if err == nil {
			// pipeReadFrom allocated these bytes exclusively for this result.
			buffer, destination = buf.As(result.data), cachedPacketSource(result.addr)
		}
	case *enhanceReadResult:
		if result.put != nil {
			defer result.put()
		}
		err = result.err
		if err == nil {
			buffer = buf.NewSize(len(result.data))
			_, _ = buffer.Write(result.data)
			destination = cachedPacketSource(result.addr)
		}
	default:
		panic("invalid cached packet result")
	}
	if err != nil {
		buffer.Release()
		return nil, M.Socksaddr{}, err
	}
	return
}

func cachedPacketSource(addr net.Addr) M.Socksaddr {
	if udp, ok := addr.(*net.UDPAddr); ok {
		if udp == nil {
			return M.Socksaddr{}
		}
		return M.SocksaddrFromNetIP(udp.AddrPort())
	}
	return M.SocksaddrFromNet(addr)
}

func packetResultData(result any) (data []byte, put func(), addr net.Addr, err error) {
	switch result := result.(type) {
	case *readResult:
		return result.data, nil, result.addr, result.err
	case *enhanceReadResult:
		return result.data, result.put, result.addr, result.err
	default:
		buffer, destination, readErr := packetResultBuffer(result)
		if readErr != nil {
			return nil, nil, nil, readErr
		}
		if destination.IsFqdn() {
			addr = destination
		} else {
			addr = destination.UDPAddr()
		}
		return buffer.Bytes(), buffer.Release, addr, nil
	}
}
