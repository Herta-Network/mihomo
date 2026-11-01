package geodata

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

func TestGeoSettingsConcurrentParseAndUpdate(t *testing.T) {
	mode, loader, matcher := GeodataMode(), LoaderName(), SiteMatcherName()
	ip, site, mmdb, asn := GeoIpUrl(), GeoSiteUrl(), MmdbUrl(), ASNUrl()
	t.Cleanup(func() {
		SetGeodataMode(mode)
		SetLoader(loader)
		SetSiteMatcher(matcher)
		SetGeoIpUrl(ip)
		SetGeoSiteUrl(site)
		SetMmdbUrl(mmdb)
		SetASNUrl(asn)
	})
	var workers sync.WaitGroup
	start := make(chan struct{})
	workers.Add(2)
	go func() {
		defer workers.Done()
		<-start
		for i := 0; i < 1000; i++ {
			SetGeodataMode(i%2 == 0)
			SetLoader("standard")
			SetSiteMatcher("mph")
			SetGeoIpUrl("http://127.0.0.1/geoip")
			SetGeoSiteUrl("http://127.0.0.1/geosite")
			SetMmdbUrl("http://127.0.0.1/mmdb")
			SetASNUrl("http://127.0.0.1/asn")
		}
	}()
	go func() {
		defer workers.Done()
		<-start
		for i := 0; i < 1000; i++ {
			_ = GeodataMode()
			_ = LoaderName()
			_ = SiteMatcherName()
			_ = GeoIpUrl()
			_ = GeoSiteUrl()
			_ = MmdbUrl()
			_ = ASNUrl()
		}
	}()
	close(start)
	workers.Wait()
}

func TestGeoSettingsSnapshotAndPublication(t *testing.T) {
	previous := GetSettings()
	t.Cleanup(func() { SetSettings(previous) })
	a := Settings{Mode: true, Loader: "standard", SiteMatcher: "mph", GeoIPURL: "http://a/geoip", GeoSiteURL: "http://a/geosite", MMDBURL: "http://a/mmdb", ASNURL: "http://a/asn"}
	b := Settings{Mode: false, Loader: "memconservative", SiteMatcher: "succinct", GeoIPURL: "http://b/geoip", GeoSiteURL: "http://b/geosite", MMDBURL: "http://b/mmdb", ASNURL: "http://b/asn"}
	SetSettings(a)
	var workers sync.WaitGroup
	workers.Add(2)
	go func() {
		defer workers.Done()
		for i := 0; i < 1000; i++ {
			SetSettings(a)
			SetSettings(b)
		}
	}()
	go func() {
		defer workers.Done()
		for i := 0; i < 2000; i++ {
			if snapshot := GetSettings(); snapshot != a && snapshot != b {
				t.Error("mixed GEO settings versions")
				return
			}
		}
	}()
	workers.Wait()
	SetSettings(b)
	called := false
	if err := Publish(context.Background(), a, func() error { called = true; return nil }); !errors.Is(err, ErrSettingsChanged) || called {
		t.Fatalf("stale publication: %v called=%v", err, called)
	}
	entered, release, done := make(chan struct{}), make(chan struct{}), make(chan struct{})
	go func() {
		_ = Publish(context.Background(), b, func() error { close(entered); <-release; return nil })
		close(done)
	}()
	<-entered
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	result := make(chan error, 1)
	go func() { result <- Publish(ctx, b, func() error { t.Error("canceled waiter committed"); return nil }) }()
	select {
	case err := <-result:
		if !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Error("canceled publication waited behind another writer")
	}
	close(release)
	<-done
}
