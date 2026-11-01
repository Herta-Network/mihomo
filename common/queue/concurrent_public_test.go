package queue

import (
	"sync"
	"testing"
)

func TestQueuePublicOperationsConcurrent(t *testing.T) {
	q := New[int](32)
	var workers sync.WaitGroup
	start := make(chan struct{})
	for worker := 0; worker < 8; worker++ {
		workers.Add(1)
		go func(worker int) {
			defer workers.Done()
			<-start
			for i := 0; i < 2000; i++ {
				switch worker % 4 {
				case 0:
					q.Put(i + 1)
				case 1:
					_ = q.Pop()
				case 2:
					_ = q.Last()
				case 3:
					_ = q.Copy()
					_ = q.Len()
				}
			}
		}(worker)
	}
	close(start)
	workers.Wait()
}
