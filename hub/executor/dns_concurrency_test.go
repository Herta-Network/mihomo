package executor

import (
	"context"
	"sync"
	"testing"

	"github.com/metacubex/mihomo/component/resolver"
	"github.com/metacubex/mihomo/component/trie"
	"github.com/metacubex/mihomo/config"
)

func TestDNSReloadDuringQueries(t *testing.T) {
	oldDefault, oldProxy, oldDirect := resolver.DefaultResolver.Load(), resolver.ProxyServerHostResolver.Load(), resolver.DirectHostResolver.Load()
	oldMapper, oldService, oldHosts := resolver.DefaultHostMapper.Load(), resolver.DefaultService.Load(), resolver.DefaultHosts.Load()
	oldSystemHosts := resolver.UseSystemHosts.Load()
	t.Cleanup(func() {
		resolver.DefaultResolver.Store(oldDefault)
		resolver.ProxyServerHostResolver.Store(oldProxy)
		resolver.DirectHostResolver.Store(oldDirect)
		resolver.DefaultHostMapper.Store(oldMapper)
		resolver.DefaultService.Store(oldService)
		resolver.DefaultHosts.Store(oldHosts)
		resolver.UseSystemHosts.Store(oldSystemHosts)
	})
	tree := trie.New[resolver.HostValue]()
	value, _ := resolver.NewHostValue([]string{"127.0.0.1"})
	if err := tree.Insert("reload.example", value); err != nil {
		t.Fatal(err)
	}
	var group sync.WaitGroup
	start := make(chan struct{})
	for worker := 0; worker < 4; worker++ {
		group.Add(1)
		go func() {
			defer group.Done()
			<-start
			for i := 0; i < 500; i++ {
				resolver.MappingEnabled()
				resolver.FakeIPEnabled()
				resolver.DefaultHosts.Load().Search("reload.example", false)
				resolver.DefaultHosts.Load().Search("missing.invalid", false)
				if _, err := resolver.LookupIP(context.Background(), "127.0.0.1"); err != nil {
					t.Error(err)
					return
				}
			}
		}()
	}
	close(start)
	for i := 0; i < 500; i++ {
		updateHosts(tree)
		updateDNS(&config.DNS{Enable: i%2 == 0, UseSystemHosts: i%3 == 0}, false)
	}
	group.Wait()
}
