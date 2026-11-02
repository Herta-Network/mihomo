package deadline

import (
	"encoding/binary"
	"errors"
	"io"
	"net"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/metacubex/sing/common/buf"
	M "github.com/metacubex/sing/common/metadata"
	N "github.com/metacubex/sing/common/network"
)

type concurrentOwnedSource struct {
	net.PacketConn
	packets chan uint16
	closed  chan struct{}
	once    sync.Once
	mutex   sync.Mutex
	buffers []*buf.Buffer
}

func (c *concurrentOwnedSource) ReadPacketBuffer() (*buf.Buffer, M.Socksaddr, error) {
	select {
	case value := <-c.packets:
		buffer := buf.NewSize(8)
		binary.BigEndian.PutUint16(buffer.Extend(2), value)
		c.mutex.Lock()
		c.buffers = append(c.buffers, buffer)
		c.mutex.Unlock()
		return buffer, M.ParseSocksaddr("127.0.0.1:53"), nil
	case <-c.closed:
		return nil, M.Socksaddr{}, net.ErrClosed
	}
}
func (c *concurrentOwnedSource) WaitReadFrom() ([]byte, func(), net.Addr, error) {
	b, destination, err := c.ReadPacketBuffer()
	if err != nil {
		return nil, nil, nil, err
	}
	return b.Bytes(), b.Release, destination.UDPAddr(), nil
}
func (c *concurrentOwnedSource) ReadFrom(p []byte) (int, net.Addr, error) {
	data, put, addr, err := c.WaitReadFrom()
	if put != nil {
		defer put()
	}
	return copy(p, data), addr, err
}
func (c *concurrentOwnedSource) Close() error { c.once.Do(func() { close(c.closed) }); return nil }

func TestPacketDeadlineConcurrentReadAPIsDoNotStealResults(t *testing.T) {
	const perReader = 150
	raw := &concurrentOwnedSource{packets: make(chan uint16, 2*perReader), closed: make(chan struct{})}
	for i := 0; i < 2*perReader; i++ {
		raw.packets <- uint16(i)
	}
	conn := NewEnhancePacketConn(raw).(*EnhancePacketConn)
	defer conn.Close()
	conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	seen := make(chan uint16, 2*perReader)
	var readers sync.WaitGroup
	for method := 0; method < 2; method++ {
		readers.Add(1)
		go func(method int) {
			defer readers.Done()
			for i := 0; i < perReader; i++ {
				var data []byte
				var release func()
				var err error
				switch method {

				case 0:
					data, release, _, err = conn.WaitReadFrom()
				case 1:
					var p [2]byte
					var n int
					n, _, err = conn.ReadFrom(p[:])
					data = p[:n]
				}
				if err != nil || len(data) != 2 {
					if release != nil {
						release()
					}
					t.Errorf("reader %d: bytes=%d err=%v", method, len(data), err)
					return
				}
				value := binary.BigEndian.Uint16(data)
				if release != nil {
					release()
				}
				seen <- value
			}
		}(method)
	}
	readers.Wait()
	close(seen)
	var received [2 * perReader]bool
	for value := range seen {
		if int(value) >= len(received) || received[value] {
			t.Fatalf("duplicate/corrupt packet %d", value)
		}
		received[value] = true
	}
	for value, ok := range received {
		if !ok {
			t.Fatalf("lost packet %d", value)
		}
	}
	raw.mutex.Lock()
	defer raw.mutex.Unlock()
	for _, buffer := range raw.buffers {
		if buffer.RawCap() != 0 {
			t.Fatal("concurrent read leaked payload")
		}
	}
}

func TestPacketDeadlineCachedReadAPISwitch(t *testing.T) {
	for _, source := range []string{"read-from", "wait-read-from", "read-packet", "wait-read-packet"} {
		for _, target := range []string{"read-from", "wait-read-from", "read-packet", "wait-read-packet"} {
			t.Run(source+"/"+target, func(t *testing.T) {
				raw := &delayedEnhanceConn{&delayedPacketConn{closed: make(chan struct{})}}
				conn := NewEnhancePacketConn(raw).(*EnhanceSingPacketConn)
				defer conn.Close()
				conn.SetReadDeadline(time.Now().Add(time.Second))
				destination := M.ParseSocksaddr("[::1]:5353")
				owned := buf.NewSize(64)
				owned.Resize(16, 0)
				owned.WriteString("cached")
				var result any
				switch source {
				case "read-from":
					result = &readResult{data: []byte("cached"), addr: destination.UDPAddr()}
					owned.Release()
				case "wait-read-from":
					result = &enhanceReadResult{data: owned.Bytes(), put: owned.Release, addr: destination.UDPAddr()}
				case "read-packet":
					result = &singReadResult{buffer: owned, destination: destination}
				case "wait-read-packet":
					result = &singWaitReadResult{buffer: owned, destination: destination}
				}
				<-conn.resultCh
				conn.publishResult(result)
				var data string
				var got M.Socksaddr
				var err error
				switch target {
				case "read-from":
					var p [32]byte
					var n int
					var addr net.Addr
					n, addr, err = conn.ReadFrom(p[:])
					data, got = string(p[:n]), M.SocksaddrFromNet(addr)
				case "wait-read-from":
					p, put, addr, readErr := conn.WaitReadFrom()
					data, got, err = string(p), M.SocksaddrFromNet(addr), readErr
					if put != nil {
						put()
					}
				case "read-packet":
					buffer := buf.NewSize(64)
					got, err = conn.ReadPacket(buffer)
					data = string(buffer.Bytes())
					buffer.Release()
				default:
					var buffer *buf.Buffer

					waiter := &singPacketReadWaiter{netPacketConn: conn.NetPacketConn, options: N.ReadWaitOptions{FrontHeadroom: 8, RearHeadroom: 4}}
					buffer, got, err = waiter.WaitReadPacket()
					if buffer != nil && (buffer.Start() < 8 || buffer.FreeLen() < 4) {
						t.Fatal("cached result lost waiter headroom")
					}
					if buffer != nil {
						data = string(buffer.Bytes())
						buffer.Release()
					}
				}
				if err != nil || data != "cached" || got != destination || owned.RawCap() != 0 {
					t.Fatalf("data=%q destination=%v err=%v retained=%d", data, got, err, owned.RawCap())
				}
			})
		}
	}
}

type delayedOwnedConn struct{ *delayedEnhanceConn }

func (*delayedOwnedConn) InitializeReadWaiter(N.ReadWaitOptions) bool { return false }

func (c *delayedOwnedConn) WaitReadPacket() (*buf.Buffer, M.Socksaddr, error) {
	close(c.started)
	<-c.deliver
	close(c.finished)
	return c.buffer, M.ParseSocksaddr("127.0.0.1:53"), nil
}

func TestPacketDeadlineOwnedTimeoutSwitchAndClose(t *testing.T) {
	for _, mode := range []string{"switch", "close-cached", "close-late"} {
		t.Run(mode, func(t *testing.T) {
			owned := buf.NewSize(64)
			owned.Resize(16, 0)
			owned.WriteString("late owned")
			raw := &delayedOwnedConn{&delayedEnhanceConn{&delayedPacketConn{
				started: make(chan struct{}), deliver: make(chan struct{}), finished: make(chan struct{}), closed: make(chan struct{}), buffer: owned,
			}}}
			conn := NewEnhancePacketConn(raw).(*EnhanceSingPacketConn)
			defer conn.Close()
			conn.SetReadDeadline(time.Now().Add(10 * time.Millisecond))
			waiter, ok := conn.CreateReadWaiter()
			if !ok {
				t.Fatal("missing read waiter")
			}
			if buffer, _, err := waiter.WaitReadPacket(); buffer != nil || !errors.Is(err, os.ErrDeadlineExceeded) {
				t.Fatalf("timeout buffer=%v err=%v", buffer, err)
			}
			<-raw.started
			if mode == "close-late" {
				conn.Close()
			}
			close(raw.deliver)
			<-raw.finished
			if mode == "close-late" {
				deadline := time.Now().Add(time.Second)
				for {
					conn.publishMutex.Lock()
					released := owned.RawCap() == 0
					conn.publishMutex.Unlock()
					if released {
						break
					}
					if time.Now().After(deadline) {
						t.Fatal("late owned result leaked")
					}
					time.Sleep(time.Millisecond)
				}
			} else if mode == "close-cached" {
				deadline := time.Now().Add(time.Second)
				for len(conn.resultCh) == 0 {
					if time.Now().After(deadline) {
						t.Fatal("result was not published")
					}
					time.Sleep(time.Millisecond)
				}
				conn.Close()
			} else {
				conn.SetReadDeadline(time.Now().Add(time.Second))
				data, put, addr, err := conn.WaitReadFrom()
				if err != nil || string(data) != "late owned" || addr.String() != "127.0.0.1:53" || put == nil {
					t.Fatalf("switched read %q %v %v", data, addr, err)
				}
				put()
			}
			if owned.RawCap() != 0 {
				t.Fatal("owned result retained")
			}
		})
	}
}

func TestPacketDeadlineOwnedFailureReleasesBuffer(t *testing.T) {
	for _, kind := range []string{"sing", "waiter", "legacy"} {
		t.Run(kind, func(t *testing.T) {
			owned := buf.NewSize(64)
			owned.WriteString("failed")
			var result any
			switch kind {
			case "sing":
				result = &singReadResult{buffer: owned, err: io.ErrUnexpectedEOF}
			case "waiter":
				result = &singWaitReadResult{buffer: owned, err: io.ErrUnexpectedEOF}
			case "legacy":
				result = &enhanceReadResult{data: owned.Bytes(), put: owned.Release, err: io.ErrUnexpectedEOF}
			}
			b, _, err := packetResultBuffer(result)
			if b != nil || !errors.Is(err, io.ErrUnexpectedEOF) || owned.RawCap() != 0 {
				t.Fatalf("buffer=%v err=%v retained=%d", b, err, owned.RawCap())
			}
		})
	}
}
