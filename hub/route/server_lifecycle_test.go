package route

import (
	"net"
	"net/http"
	"sync"
	"testing"
	"time"
)

func reserveControllerAddresses(t *testing.T, count int) []string {
	t.Helper()
	listeners := make([]net.Listener, 0, count)
	addresses := make([]string, 0, count)
	defer func() {
		for _, listener := range listeners {
			listener.Close()
		}
	}()
	for i := 0; i < count; i++ {
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		listeners = append(listeners, listener)
		addresses = append(addresses, listener.Addr().String())
	}
	return addresses
}

func TestControllerConcurrentRecreationClosesAllListeners(t *testing.T) {
	addresses := reserveControllerAddresses(t, 8)
	t.Cleanup(func() { ReCreateServer(&Config{}); SetEmbedMode(false); SetUIPath("") })
	start := make(chan struct{})
	var workers sync.WaitGroup
	for _, address := range addresses {
		workers.Add(1)
		go func(address string) {
			defer workers.Done()
			<-start
			for i := 0; i < 4; i++ {
				cfg := &Config{Addr: address, Secret: "before-return", Cors: Cors{AllowOrigins: []string{"https://before.example"}}}
				ReCreateServer(cfg)
				// Callers may reuse their config after ReCreateServer returns.
				cfg.Secret = "after-return"
				cfg.Cors.AllowOrigins[0] = "https://after.example"
			}
		}(address)
	}
	close(start)
	workers.Wait()
	ReCreateServer(&Config{})
	for _, address := range addresses {
		listener, err := net.Listen("tcp", address)
		if err != nil {
			t.Errorf("stop returned with a live listener at %s: %v", address, err)
			continue
		}
		listener.Close()
	}
}

func TestControllerSettingsConcurrentRecreation(t *testing.T) {
	t.Cleanup(func() { ReCreateServer(&Config{}); SetEmbedMode(false); SetUIPath("") })
	address := reserveControllerAddresses(t, 1)[0]
	path := t.TempDir()
	start := make(chan struct{})
	var workers sync.WaitGroup
	workers.Add(2)
	go func() {
		defer workers.Done()
		<-start
		for i := 0; i < 30; i++ {
			SetEmbedMode(i%2 == 0)
			SetUIPath(path)
		}
	}()
	go func() {
		defer workers.Done()
		<-start
		for i := 0; i < 30; i++ {
			ReCreateServer(&Config{Addr: address})
		}
	}()
	close(start)
	workers.Wait()
	ReCreateServer(&Config{})
}

func TestControllerTLSValidationDoesNotLeakListener(t *testing.T) {
	address := reserveControllerAddresses(t, 1)[0]
	t.Cleanup(func() { ReCreateServer(&Config{}) })
	ReCreateServer(&Config{TLSAddr: address, Certificate: "invalid certificate", PrivateKey: "invalid key"})
	listener, err := net.Listen("tcp", address)
	if err != nil {
		t.Fatalf("invalid TLS configuration leaked listener: %v", err)
	}
	listener.Close()
}

func TestControllerUsesConfigSnapshot(t *testing.T) {
	address := reserveControllerAddresses(t, 1)[0]
	t.Cleanup(func() { ReCreateServer(&Config{}) })
	cfg := &Config{Addr: address, Secret: "original", Cors: Cors{AllowOrigins: []string{"https://original.example"}}}
	ReCreateServer(cfg)
	cfg.Secret = "changed"
	cfg.Cors.AllowOrigins[0] = "https://changed.example"
	client := &http.Client{Timeout: time.Second}
	defer client.CloseIdleConnections()
	for _, item := range []struct {
		token  string
		status int
	}{{"original", http.StatusOK}, {"changed", http.StatusUnauthorized}} {
		req, _ := http.NewRequest(http.MethodGet, "http://"+address+"/version", nil)
		req.Header.Set("Authorization", "Bearer "+item.token)
		req.Header.Set("Origin", "https://original.example")
		resp, err := client.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		if resp.StatusCode != item.status {
			t.Errorf("token %s status %d, want %d", item.token, resp.StatusCode, item.status)
		}
		if resp.Header.Get("Access-Control-Allow-Origin") != "https://original.example" {
			t.Error("caller mutated live CORS settings")
		}
	}
}
