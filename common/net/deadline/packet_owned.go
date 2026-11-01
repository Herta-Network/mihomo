package deadline

import (
	"net"
	"os"

	"github.com/metacubex/mihomo/common/net/packet"
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

func (c *enhancePacketConn) ReadPacketBuffer() (*buf.Buffer, M.Socksaddr, error) {
	c.netPacketConn.readMutex.Lock()
	defer c.netPacketConn.readMutex.Unlock()
	return c.readPacketBuffer()
}

func (c *enhancePacketConn) readPacketBuffer() (*buf.Buffer, M.Socksaddr, error) {
	owner := c.netPacketConn
	if owner.isClosed() {
		return nil, M.Socksaddr{}, net.ErrClosed
	}
	select {
	case result := <-owner.resultCh:
		if result != nil {
			buffer, destination, err := packetResultBuffer(result)
			owner.publishResult(nil)
			return buffer, destination, err
		}
		owner.publishResult(nil)
	case <-owner.done:
		return nil, M.Socksaddr{}, net.ErrClosed
	case <-owner.pipeDeadline.Wait():
		return nil, M.Socksaddr{}, os.ErrDeadlineExceeded
	}
	if owner.disablePipe.Load() {
		return packet.ReadPacketBuffer(c.enhancePacketConn)
	} else if !owner.deadlineSet.Load() {
		owner.inRead.Store(true)
		defer owner.inRead.Store(false)
		return packet.ReadPacketBuffer(c.enhancePacketConn)
	}
	select {
	case <-owner.done:
		return nil, M.Socksaddr{}, net.ErrClosed
	case <-owner.resultCh:
	}
	go c.pipeReadPacketBuffer()
	return c.readPacketBuffer()
}

func (c *enhancePacketConn) pipeReadPacketBuffer() {
	buffer, destination, err := packet.ReadPacketBuffer(c.enhancePacketConn)
	c.netPacketConn.publishResult(&singWaitReadResult{buffer: buffer, destination: destination, err: err})
}
