package mmdb

import (
	"context"
	"errors"
	"net"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	C "github.com/metacubex/mihomo/constant"
)

func installTestFiles(t *testing.T, asnFixture string) {
	t.Helper()
	home := C.Path.HomeDir()
	C.SetHomeDir(t.TempDir())
	t.Cleanup(func() { Close(); C.SetHomeDir(home) })
	for name, fixture := range map[string]string{"geoip.metadb": "country-a", "ASN.mmdb": asnFixture} {
		data, err := os.ReadFile(filepath.Join("testdata", fixture+".mmdb"))
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(C.Path.HomeDir(), name), data, 0600); err != nil {
			t.Fatal(err)
		}
	}
	ReloadIP()
	ReloadASN()
}

func TestMMDBReloadConcurrentLookup(t *testing.T) {
	installTestFiles(t, "asn-a")
	ip := net.ParseIP("1.2.3.4")
	var wg sync.WaitGroup
	start := make(chan struct{})
	for worker := 0; worker < 6; worker++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			for i := 0; i < 200; i++ {
				codes := IPInstance().LookupCode(ip)
				if len(codes) != 1 || codes[0] != "aa" {
					t.Errorf("incomplete country lookup: %v", codes)
					return
				}
				asn, org := ASNInstance().LookupASN(ip)
				if asn != "64512" || org != "Test A" {
					t.Errorf("incomplete ASN lookup: %q %q", asn, org)
					return
				}
			}
		}()
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		<-start
		for i := 0; i < 200; i++ {
			ReloadIP()
			ReloadASN()
		}
	}()
	close(start)
	wg.Wait()
}

func TestMMDBIPInfoMissingRecord(t *testing.T) {
	installTestFiles(t, "ipinfo")
	asn, org := ASNInstance().LookupASN(net.ParseIP("5.6.7.8"))
	if asn != "" || org != "" {
		t.Fatalf("missing record: %q %q", asn, org)
	}
}

func TestMMDBInstallWaitsForQueryAndPreservesOnFailure(t *testing.T) {
	installTestFiles(t, "asn-a")
	path := C.Path.MMDB()
	next, err := os.ReadFile(filepath.Join("testdata", "country-b.mmdb"))
	if err != nil {
		t.Fatal(err)
	}
	handle := IPInstance()
	reader, release := ipDatabase.acquire()
	done := make(chan error, 1)
	attempted, committed := make(chan struct{}), make(chan struct{})
	go func() {
		close(attempted)
		done <- InstallIP(path, C.Path.Generation(), next, func() error {
			close(committed)
			staged := path + ".new"
			if err := os.WriteFile(staged, next, 0600); err != nil {
				return err
			}
			return os.Rename(staged, path)
		})
	}()
	<-attempted
	select {
	case <-committed:
		t.Error("replaced database while query held a lease")
	case <-time.After(20 * time.Millisecond):
	}
	var record geoip2Country
	if err := reader.Lookup(net.ParseIP("1.2.3.4"), &record); err != nil || record.Country.IsoCode != "AA" {
		t.Errorf("leased reader invalid: %v %+v", err, record)
	}
	release()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	codes := handle.LookupCode(net.ParseIP("1.2.3.4"))
	if len(codes) != 1 || codes[0] != "bb" {
		t.Fatalf("retained handle did not switch: %v", codes)
	}
	rejected := errors.New("rename rejected")
	if err := InstallIP(path, C.Path.Generation(), next, func() error { return rejected }); !errors.Is(err, rejected) {
		t.Fatal(err)
	}
	codes = handle.LookupCode(net.ParseIP("1.2.3.4"))
	if len(codes) != 1 || codes[0] != "bb" {
		t.Fatalf("failed commit lost reader: %v", codes)
	}
}

func TestMMDBInstallFinishesAfterCommitCancellation(t *testing.T) {
	installTestFiles(t, "asn-a")
	next, err := os.ReadFile(filepath.Join("testdata", "country-b.mmdb"))
	if err != nil {
		t.Fatal(err)
	}
	path := C.Path.MMDB()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	err = InstallIP(path, C.Path.Generation(), next, func() error {
		staged := path + ".new"
		if err := os.WriteFile(staged, next, 0600); err != nil {
			return err
		}
		if err := os.Rename(staged, path); err != nil {
			return err
		}
		cancel() // cancellation after the irreversible publication boundary
		return nil
	})
	if err != nil || ctx.Err() == nil {
		t.Fatalf("commit: %v canceled=%v", err, ctx.Err())
	}
	codes := IPInstance().LookupCode(net.ParseIP("1.2.3.4"))
	if len(codes) != 1 || codes[0] != "bb" {
		t.Fatalf("committed file left old reader: %v", codes)
	}
}
