package deadline

import (
	"net"
	"os"
	"runtime"
	"sync"
	"time"

	"github.com/metacubex/mihomo/common/atomic"
	"github.com/metacubex/mihomo/common/net/packet"
)

type readResult struct {
	data []byte
	addr net.Addr
	err  error
}

type NetPacketConn struct {
	net.PacketConn
	deadline     atomic.TypedValue[time.Time]
	pipeDeadline PipeDeadline
	disablePipe  atomic.Bool
	inRead       atomic.Bool
	resultCh     chan any
	done         chan struct{}
	closeOnce    sync.Once
	publishMutex sync.Mutex
	closeErr     error
}

func NewNetPacketConn(pc net.PacketConn) net.PacketConn {
	npc := &NetPacketConn{
		PacketConn:   pc,
		pipeDeadline: MakePipeDeadline(),
		resultCh:     make(chan any, 1),
		done:         make(chan struct{}),
	}
	npc.publishResult(nil)
	if enhancePC, isEnhance := pc.(packet.EnhancePacketConn); isEnhance {
		epc := &EnhancePacketConn{
			NetPacketConn: npc,
			enhancePacketConn: enhancePacketConn{
				netPacketConn:     npc,
				enhancePacketConn: enhancePC,
			},
		}
		if singPC, isSingPC := pc.(packet.SingPacketConn); isSingPC {
			return &EnhanceSingPacketConn{
				EnhancePacketConn: epc,
				singPacketConn: singPacketConn{
					netPacketConn:  npc,
					singPacketConn: singPC,
				},
			}
		}
		return epc
	}
	if singPC, isSingPC := pc.(packet.SingPacketConn); isSingPC {
		return &SingPacketConn{
			NetPacketConn: npc,
			singPacketConn: singPacketConn{
				netPacketConn:  npc,
				singPacketConn: singPC,
			},
		}
	}
	return npc
}

func (c *NetPacketConn) ReadFrom(p []byte) (n int, addr net.Addr, err error) {
	if c.isClosed() {
		return 0, nil, net.ErrClosed
	}
FOR:
	for {
		select {
		case result := <-c.resultCh:
			if result != nil {
				if result, ok := result.(*readResult); ok {
					n = copy(p, result.data)
					addr = result.addr
					err = result.err
					c.publishResult(nil) // finish cache read
					return
				}
				c.publishResult(result) // another type of read
				runtime.Gosched()       // allowing other goroutines to run
				continue FOR
			} else {
				c.publishResult(nil)
				break FOR
			}
		case <-c.done:
			return 0, nil, net.ErrClosed
		case <-c.pipeDeadline.Wait():
			return 0, nil, os.ErrDeadlineExceeded
		}
	}

	if c.disablePipe.Load() {
		return c.PacketConn.ReadFrom(p)
	} else if c.deadline.Load().IsZero() {
		c.inRead.Store(true)
		defer c.inRead.Store(false)
		n, addr, err = c.PacketConn.ReadFrom(p)
		return
	}

	select {
	case <-c.done:
		return 0, nil, net.ErrClosed
	case <-c.resultCh:
	}
	go c.pipeReadFrom(len(p))

	return c.ReadFrom(p)
}

func (c *NetPacketConn) pipeReadFrom(size int) {
	buffer := make([]byte, size)
	n, addr, err := c.PacketConn.ReadFrom(buffer)
	buffer = buffer[:n]
	result := &readResult{}
	result.data = buffer
	result.addr = addr
	result.err = err
	c.publishResult(result)
}

func (c *NetPacketConn) SetReadDeadline(t time.Time) error {
	if c.disablePipe.Load() {
		return c.PacketConn.SetReadDeadline(t)
	} else if c.inRead.Load() {
		c.disablePipe.Store(true)
		return c.PacketConn.SetReadDeadline(t)
	}
	c.deadline.Store(t)
	c.pipeDeadline.Set(t)
	return nil
}

func (c *NetPacketConn) ReaderReplaceable() bool {
	if c.isClosed() {
		return false
	}
	select {
	case result := <-c.resultCh:
		c.publishResult(result)
		if result != nil {
			return false // cache reading
		} else {
			break
		}
	default:
		return false // pipe reading
	}
	return c.disablePipe.Load() || c.deadline.Load().IsZero()
}

func (c *NetPacketConn) WriterReplaceable() bool {
	return true
}

func (c *NetPacketConn) Upstream() any {
	return c.PacketConn
}

func (c *NetPacketConn) NeedAdditionalReadDeadline() bool {
	return false
}

// Close interrupts pending reads before discarding cached owned packets. A
// result arriving after close is released by its producer, never left in a
// channel whose consumer has timed out.
func (c *NetPacketConn) Close() error {
	c.closeOnce.Do(func() {
		close(c.done)
		c.publishMutex.Lock()
		select {
		case result := <-c.resultCh:
			releasePacketResult(result)
		default:
		}
		c.publishMutex.Unlock()
		c.closeErr = c.PacketConn.Close()
	})
	return c.closeErr
}

func (c *NetPacketConn) publishResult(result any) {
	c.publishMutex.Lock()
	defer c.publishMutex.Unlock()
	if c.isClosed() {
		releasePacketResult(result)
		return
	}
	select {
	case <-c.done:
		releasePacketResult(result)
	case c.resultCh <- result:
	}
}

func releasePacketResult(result any) {
	switch result := result.(type) {
	case *enhanceReadResult:
		if result.put != nil {
			result.put()
		}
	case *singReadResult:
		result.buffer.Release()
	case *singWaitReadResult:
		result.buffer.Release()
	}
}

func (c *NetPacketConn) isClosed() bool {
	select {
	case <-c.done:
		return true
	default:
		return false
	}
}
