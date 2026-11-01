package log

import (
	"sync"
	"testing"
)

func TestLogLevelConcurrentReloadAndBackgroundLogging(t *testing.T) {
	previous := Level()
	t.Cleanup(func() { SetLevel(previous) })
	SetLevel(INFO)
	var workers sync.WaitGroup
	start := make(chan struct{})
	for worker := 0; worker < 4; worker++ {
		workers.Add(1)
		go func(writer bool) {
			defer workers.Done()
			<-start
			for i := 0; i < 10000; i++ {
				if writer {
					SetLevel(INFO)
					SetLevel(ERROR)
				} else {
					_ = Level()
					print(Event{LogLevel: DEBUG, Payload: "suppressed concurrent event"})
				}
			}
		}(worker == 0)
	}
	close(start)
	workers.Wait()
}
