package geodata_test

import (
	"context"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/metacubex/mihomo/component/geodata"
	"github.com/metacubex/mihomo/component/geodata/router"
	_ "github.com/metacubex/mihomo/component/geodata/standard"
	"github.com/metacubex/mihomo/component/mmdb"
	C "github.com/metacubex/mihomo/constant"
	"google.golang.org/protobuf/proto"
)

func testGeoHome(t *testing.T) {
	t.Helper()
	home, settings := C.Path.HomeDir(), geodata.GetSettings()
	C.SetHomeDir(t.TempDir())
	t.Cleanup(func() { _ = mmdb.Close(); C.SetHomeDir(home); geodata.SetSettings(settings) })
	geodata.SetLoader("standard")
}
func TestInitialGeoDownloadKeepsConcurrentPublication(t *testing.T) {
	testGeoHome(t)
	geodata.SetGeodataMode(false)
	a, err := os.ReadFile(filepath.Join("..", "mmdb", "testdata", "country-a.mmdb"))
	if err != nil {
		t.Fatal(err)
	}
	b, err := os.ReadFile(filepath.Join("..", "mmdb", "testdata", "country-b.mmdb"))
	if err != nil {
		t.Fatal(err)
	}
	entered, release := make(chan struct{}), make(chan struct{})
	var releaseOnce sync.Once
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { close(entered); <-release; _, _ = w.Write(a) }))
	defer server.Close()
	defer releaseOnce.Do(func() { close(release) })
	geodata.SetMmdbUrl(server.URL)
	done := make(chan error, 1)
	go func() { done <- geodata.InitGeoIP() }()
	<-entered
	if err := geodata.InstallDatabase(context.Background(), geodata.MMDB, geodata.GetSettings(), C.Path.GeoPaths(), b); err != nil {
		t.Fatal(err)
	}
	releaseOnce.Do(func() { close(release) })
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("initialization stalled")
	}
	codes := mmdb.IPInstance().LookupCode(net.ParseIP("1.2.3.4"))
	if len(codes) != 1 || codes[0] != "bb" {
		t.Fatalf("initializer replaced newer update: %v", codes)
	}
}

func TestGeoSitePublicationRefreshesNewMatchers(t *testing.T) {
	testGeoHome(t)
	makeData := func(name string) []byte {
		data, err := proto.Marshal(&router.GeoSiteList{Entry: []*router.GeoSite{{CountryCode: "CN", Domain: []*router.Domain{{Type: router.Domain_Full, Value: name}}}}})
		if err != nil {
			t.Fatal(err)
		}
		return data
	}
	paths, settings := C.Path.GeoPaths(), geodata.GetSettings()
	if err := geodata.InstallDatabase(context.Background(), geodata.GeoSite, settings, paths, makeData("old.example")); err != nil {
		t.Fatal(err)
	}
	previous, err := geodata.LoadGeoSiteMatcher("CN")
	if err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 100; i++ {
			matcher, err := geodata.LoadGeoSiteMatcher("CN")
			if err != nil {
				t.Error(err)
				return
			}
			if !matcher.ApplyDomain("old.example") && !matcher.ApplyDomain("new.example") {
				t.Error("incomplete matcher")
				return
			}
		}
	}()
	if err := geodata.InstallDatabase(context.Background(), geodata.GeoSite, settings, paths, makeData("new.example")); err != nil {
		t.Fatal(err)
	}
	wg.Wait()
	current, err := geodata.LoadGeoSiteMatcher("CN")
	if err != nil {
		t.Fatal(err)
	}
	if !current.ApplyDomain("new.example") || current.ApplyDomain("old.example") {
		t.Fatal("new matcher retained old GEO data")
	}
	if !previous.ApplyDomain("old.example") {
		t.Fatal("active rule's immutable matcher changed")
	}
}
