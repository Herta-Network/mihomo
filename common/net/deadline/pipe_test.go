package deadline

import (
	"errors"
	"net"
	"runtime"
	"sync"
	"testing"
	"time"
)

func TestPipeDeadlineReuseAfterClearAndExpiry(t *testing.T) {
	d := MakePipeDeadline()
	defer d.stop()
	future := time.Now().Add(time.Hour)
	d.Set(future)
	timer, open := d.timer, d.Wait()
	for _, next := range []time.Time{{}, {}, future, future, {}} {
		d.Set(next)
		if d.timer != timer || d.Wait() != open || isClosedChan(open) {
			t.Fatal("refresh or clear replaced active timer/channel")
		}
	}
	d.Set(time.Now().Add(time.Millisecond))
	select {
	case <-open:
	case <-time.After(3 * time.Second):
		t.Fatal("reused timer did not expire")
	}
	d.Set(future)
	refreshed := d.Wait()
	if d.timer != timer || refreshed == open || isClosedChan(refreshed) {
		t.Fatal("expired deadline did not start a fresh wait epoch")
	}
	d.Set(time.Now().Add(-time.Second))
	if !isClosedChan(refreshed) {
		t.Fatal("past deadline did not cancel immediately")
	}
	d.Set(time.Time{})
	if isClosedChan(d.Wait()) {
		t.Fatal("cleared deadline is still expired")
	}
	d.Set(future)
	if n := testing.AllocsPerRun(100, func() { d.Set(future) }); n != 0 {
		t.Fatalf("steady deadline refresh allocates: %g", n)
	}
}

func TestPipeDeadlineConcurrentSetAndStop(t *testing.T) {
	d := MakePipeDeadline()
	var workers sync.WaitGroup
	for i := 0; i < 6; i++ {
		workers.Add(1)
		go func(worker int) {
			defer workers.Done()
			for j := 0; j < 500; j++ {
				switch (worker + j) % 3 {
				case 0:
					d.Set(time.Now().Add(time.Microsecond))
				case 1:
					d.Set(time.Time{})
				case 2:
					d.Set(time.Now().Add(-time.Second))
				}
				_ = isClosedChan(d.Wait())
				runtime.Gosched()
			}
		}(i)
	}
	workers.Wait()
	d.stop()
	d.Set(time.Now().Add(time.Hour))
	if d.timer != nil || d.timerActive || !d.stopped {
		t.Fatal("closed owner retained or rearmed timer")
	}
}

func TestPacketDeadlineClosePreventsRefresh(t *testing.T) {
	c := NewNetPacketConn(&benchmarkPacketConn{}).(*NetPacketConn)
	var workers sync.WaitGroup
	ready := make(chan struct{}, 4)
	start := make(chan struct{})
	for i := 0; i < 4; i++ {
		workers.Add(1)
		go func() {
			defer workers.Done()
			_ = c.SetReadDeadline(time.Now().Add(time.Hour))
			ready <- struct{}{}
			<-start
			for j := 0; j < 500; j++ {
				_ = c.SetReadDeadline(time.Now().Add(time.Hour))
			}
		}()
	}
	for i := 0; i < 4; i++ {
		<-ready
	}
	close(start)
	c.Close()
	workers.Wait()
	if err := c.SetReadDeadline(time.Time{}); !errors.Is(err, net.ErrClosed) {
		t.Fatalf("deadline after close: %v", err)
	}
	if c.pipeDeadline.timer != nil || c.pipeDeadline.timerActive || !c.pipeDeadline.stopped {
		t.Fatal("closed packet connection retained timer")
	}
}

type notifiedReadConn struct {
	net.Conn
	started chan struct{}
	once    sync.Once
}

func (c *notifiedReadConn) Read(p []byte) (int, error) {
	c.once.Do(func() { close(c.started) })
	return c.Conn.Read(p)
}

func TestConnDeadlineCloseUnblocksReader(t *testing.T) {
	left, right := net.Pipe()
	defer right.Close()
	raw := &notifiedReadConn{Conn: left, started: make(chan struct{})}
	c := NewConn(raw)
	defer c.Close()
	c.SetReadDeadline(time.Now().Add(time.Hour))
	done := make(chan error, 1)
	go func() { _, err := c.Read(make([]byte, 1)); done <- err }()
	select {
	case <-raw.started:
	case <-time.After(3 * time.Second):
		t.Fatal("read did not reach carrier")
	}
	c.Close()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("closed read succeeded")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("closing the timer stranded a pipe read")
	}
	if c.pipeDeadline.timer != nil || !c.pipeDeadline.stopped {
		t.Fatal("closed stream connection retained timer")
	}
}
