package deadline

import (
	"net"
	"os"

	"github.com/metacubex/mihomo/common/net/packet"
)

type EnhancePacketConn struct {
	*NetPacketConn
	enhancePacketConn
}

var _ packet.EnhancePacketConn = (*EnhancePacketConn)(nil)

func NewEnhancePacketConn(pc packet.EnhancePacketConn) packet.EnhancePacketConn {
	return NewNetPacketConn(pc).(packet.EnhancePacketConn)
}

type enhanceReadResult struct {
	data []byte
	put  func()
	addr net.Addr
	err  error
}

type enhancePacketConn struct {
	netPacketConn     *NetPacketConn
	enhancePacketConn packet.EnhancePacketConn
}

func (c *enhancePacketConn) WaitReadFrom() (data []byte, put func(), addr net.Addr, err error) {
	c.netPacketConn.readMutex.Lock()
	defer c.netPacketConn.readMutex.Unlock()
	return c.waitReadFrom()
}

func (c *enhancePacketConn) waitReadFrom() (data []byte, put func(), addr net.Addr, err error) {
	if c.netPacketConn.isClosed() {
		return nil, nil, nil, net.ErrClosed
	}
FOR:
	for {
		select {
		case result := <-c.netPacketConn.resultCh:
			if result != nil {
				data, put, addr, err = packetResultData(result)
				c.netPacketConn.publishResult(nil) // finish cache read
				return
			} else {
				c.netPacketConn.publishResult(nil)
				break FOR
			}
		case <-c.netPacketConn.done:
			return nil, nil, nil, net.ErrClosed
		case <-c.netPacketConn.pipeDeadline.Wait():
			return nil, nil, nil, os.ErrDeadlineExceeded
		}
	}

	if c.netPacketConn.disablePipe.Load() {
		return c.enhancePacketConn.WaitReadFrom()
	} else if !c.netPacketConn.deadlineSet.Load() {
		c.netPacketConn.inRead.Store(true)
		defer c.netPacketConn.inRead.Store(false)
		data, put, addr, err = c.enhancePacketConn.WaitReadFrom()
		return
	}

	select {
	case <-c.netPacketConn.done:
		return nil, nil, nil, net.ErrClosed
	case <-c.netPacketConn.resultCh:
	}
	go c.pipeWaitReadFrom()

	return c.waitReadFrom()
}

func (c *enhancePacketConn) pipeWaitReadFrom() {
	data, put, addr, err := c.enhancePacketConn.WaitReadFrom()
	result := &enhanceReadResult{}
	result.data = data
	result.put = put
	result.addr = addr
	result.err = err
	c.netPacketConn.publishResult(result)
}
