package singleflight

import (
	"sync"
	"sync/atomic"
	"testing"
)

func TestStoredResultConcurrentCalls(t *testing.T) {
	for round := 0; round < 32; round++ {
		group := Group[int]{StoreResult: true}
		var called atomic.Int64
		var workers sync.WaitGroup
		start := make(chan struct{})
		for i := 0; i < 16; i++ {
			workers.Add(1)
			go func() {
				defer workers.Done()
				<-start
				for j := 0; j < 32; j++ {
					value, err, _ := group.Do("same", func() (int, error) { called.Add(1); return 42, nil })
					if err != nil || value != 42 {
						t.Errorf("stored result: %d %v", value, err)
						return
					}
				}
			}()
		}
		close(start)
		workers.Wait()
		if called.Load() != 1 {
			t.Fatalf("stored callback ran %d times", called.Load())
		}
	}
}
