package deadline

import (
	"errors"
	"net"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/metacubex/mihomo/common/net/packet"
	"github.com/metacubex/sing/common/buf"
	M "github.com/metacubex/sing/common/metadata"
	N "github.com/metacubex/sing/common/network"
)

type delayedPacketConn struct {
	N.NetPacketConn
	started  chan struct{}
	deliver  chan struct{}
	finished chan struct{}
	closed   chan struct{}
	once     sync.Once
	buffer   *buf.Buffer
}

func (c *delayedPacketConn) ReadPacket(b *buf.Buffer) (M.Socksaddr, error) {
	close(c.started)
	<-c.deliver
	_, err := b.Write([]byte("late packet"))
	close(c.finished)
	return M.ParseSocksaddr("127.0.0.1:53"), err
}
func (c *delayedPacketConn) Close() error { c.once.Do(func() { close(c.closed) }); return nil }

func TestPacketDeadlineReadRetainsBuffer(t *testing.T) {
	raw := &delayedPacketConn{started: make(chan struct{}), deliver: make(chan struct{}), finished: make(chan struct{}), closed: make(chan struct{})}
	conn := NewSingPacketConn(raw)
	defer conn.Close()
	conn.SetReadDeadline(time.Now().Add(10 * time.Millisecond))
	buffer := buf.NewSize(64)
	defer buffer.Release()
	if _, err := conn.ReadPacket(buffer); !errors.Is(err, os.ErrDeadlineExceeded) {
		t.Fatal(err)
	}
	<-raw.started
	close(raw.deliver)
	<-raw.finished
	conn.SetReadDeadline(time.Now().Add(time.Second))
	dest, err := conn.ReadPacket(buffer)
	if err != nil || string(buffer.Bytes()) != "late packet" || dest.Port != 53 {
		t.Fatalf("%q %v %v", buffer.Bytes(), dest, err)
	}
}

type delayedEnhanceConn struct{ *delayedPacketConn }

func (c *delayedEnhanceConn) WaitReadFrom() ([]byte, func(), net.Addr, error) {
	close(c.started)
	<-c.deliver
	close(c.finished)
	return c.buffer.Bytes(), c.buffer.Release, &net.UDPAddr{}, nil
}

func TestPacketDeadlineCloseReleasesLateBuffer(t *testing.T) {
	for _, late := range []bool{false, true} {
		name := "cached"
		if late {
			name = "after-close"
		}
		t.Run(name, func(t *testing.T) {
			owned := buf.NewSize(64)
			owned.WriteString("owned")
			raw := &delayedEnhanceConn{&delayedPacketConn{started: make(chan struct{}), deliver: make(chan struct{}), finished: make(chan struct{}), closed: make(chan struct{}), buffer: owned}}
			conn := NewEnhancePacketConn(raw).(*EnhanceSingPacketConn)
			conn.SetReadDeadline(time.Now().Add(10 * time.Millisecond))
			if _, _, _, err := conn.WaitReadFrom(); !errors.Is(err, os.ErrDeadlineExceeded) {
				t.Fatal(err)
			}
			<-raw.started
			if late {
				conn.Close()
			}
			close(raw.deliver)
			<-raw.finished
			// Synchronize with the publishing goroutine without racing buffer inspection.
			if !late {
				deadline := time.After(time.Second)
				for len(conn.resultCh) == 0 {
					select {
					case <-deadline:
						t.Fatal("no cached result")
					default:
						time.Sleep(time.Millisecond)
					}
				}
				conn.Close()
			} else {
				deadline := time.After(time.Second)
				for {
					conn.publishMutex.Lock()
					released := owned.Cap() == 0
					conn.publishMutex.Unlock()
					if released {
						break
					}
					select {
					case <-deadline:
						t.Fatal("late buffer leaked")
					default:
						time.Sleep(time.Millisecond)
					}
				}
			}
			if owned.Cap() != 0 {
				t.Fatal("buffer leaked")
			}
			conn.Close()
			if _, _, _, err := conn.WaitReadFrom(); !errors.Is(err, net.ErrClosed) {
				t.Fatal(err)
			}
		})
	}
}

var _ packet.EnhanceSingPacketConn = (*delayedEnhanceConn)(nil)

func TestPacketDeadlineCloseUnblocksResultPublisher(t *testing.T) {
	raw := &delayedPacketConn{closed: make(chan struct{})}
	conn := NewNetPacketConn(raw).(*SingPacketConn).NetPacketConn
	owned := buf.NewSize(64)
	owned.WriteString("pending publication")
	published := make(chan struct{})
	go func() {
		conn.publishResult(&singReadResult{buffer: owned})
		close(published)
	}()
	// The initial cache sentinel occupies resultCh. Wait until the producer
	// owns its publication lock, then close while its channel send is blocked.
	deadline := time.After(time.Second)
	for conn.publishMutex.TryLock() {
		conn.publishMutex.Unlock()
		select {
		case <-deadline:
			t.Fatal("publisher did not start")
		default:
			time.Sleep(time.Millisecond)
		}
	}
	closed := make(chan struct{})
	go func() { conn.Close(); close(closed) }()
	select {
	case <-closed:
	case <-time.After(time.Second):
		t.Fatal("close blocked behind result publication")
	}
	select {
	case <-published:
	case <-time.After(time.Second):
		t.Fatal("publisher did not stop")
	}
	if owned.Cap() != 0 {
		t.Fatal("unpublished packet leaked")
	}
}
