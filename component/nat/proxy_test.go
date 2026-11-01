package nat

import (
	"net"
	"sync"
	"testing"
	"time"
)

type replyFunc func([]byte, net.Addr) (int, error)

func (f replyFunc) WriteBack(b []byte, addr net.Addr) (int, error) { return f(b, addr) }

func TestWriteBackProxyUpdatesDuringBlockedReply(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	done := make(chan struct{})
	proxy := NewWriteBackProxy(replyFunc(func(b []byte, _ net.Addr) (int, error) {
		close(started)
		<-release
		return len(b), nil
	}))
	go func() { _, _ = proxy.WriteBack(nil, nil); close(done) }()
	<-started
	updated := make(chan struct{})
	go func() {
		proxy.UpdateWriteBack(replyFunc(func(b []byte, _ net.Addr) (int, error) { return len(b), nil }))
		close(updated)
	}()
	select {
	case <-updated:
	case <-time.After(3 * time.Second):
		close(release)
		t.Fatal("blocked reply held update lock")
	}
	close(release)
	<-done
	if n, err := proxy.WriteBack([]byte("new"), nil); n != 3 || err != nil {
		t.Fatalf("new writer: n=%d err=%v", n, err)
	}
}

func TestWriteBackProxyConcurrentUpdates(t *testing.T) {
	reply := replyFunc(func(b []byte, _ net.Addr) (int, error) { return len(b), nil })
	proxy := NewWriteBackProxy(reply)
	var workers sync.WaitGroup
	for i := 0; i < 4; i++ {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for j := 0; j < 1000; j++ {
				proxy.UpdateWriteBack(reply)
				_, _ = proxy.WriteBack(nil, nil)
			}
		}()
	}
	workers.Wait()
	if allocations := testing.AllocsPerRun(100, func() { proxy.UpdateWriteBack(reply) }); allocations != 0 {
		t.Fatalf("steady update allocates: %g", allocations)
	}
}
