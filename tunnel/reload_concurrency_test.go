package tunnel

import (
	"sync"
	"testing"

	"github.com/metacubex/mihomo/component/sniffer"
)

func TestSnifferAndModeConcurrentReload(t *testing.T) {
	dispatcher, err := sniffer.NewDispatcher(&sniffer.Config{Enable: true})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { UpdateSniffer(nil); SetMode(Rule) })
	var group sync.WaitGroup
	start := make(chan struct{})
	for worker := 0; worker < 4; worker++ {
		group.Add(1)
		go func() {
			defer group.Done()
			<-start
			for i := 0; i < 1000; i++ {
				IsSniffing()
				Mode()
				SetSniffing(i%2 == 0)
			}
		}()
	}
	close(start)
	for i := 0; i < 1000; i++ {
		UpdateSniffer(dispatcher)
		UpdateSniffer(nil)
		SetMode(TunnelMode(i % 3))
	}
	group.Wait()
}
