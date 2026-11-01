package updater_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/metacubex/mihomo/component/geodata"
	"github.com/metacubex/mihomo/component/mmdb"
	"github.com/metacubex/mihomo/component/updater"
	"github.com/metacubex/mihomo/config"
	C "github.com/metacubex/mihomo/constant"
	_ "github.com/metacubex/mihomo/hub/executor"
)

func geoFixture(t *testing.T, name string) []byte {
	t.Helper()
	data, err := os.ReadFile(filepath.Join("..", "mmdb", "testdata", name+".mmdb"))
	if err != nil {
		t.Fatal(err)
	}
	return data
}
func setupGeoUpdate(t *testing.T) ([]byte, []byte) {
	t.Helper()
	home, settings := C.Path.HomeDir(), geodata.GetSettings()
	C.SetHomeDir(t.TempDir())
	t.Cleanup(func() { _ = mmdb.Close(); C.SetHomeDir(home); geodata.SetSettings(settings) })
	old, next := geoFixture(t, "country-a"), geoFixture(t, "country-b")
	if err := geodata.ValidateDatabase(geodata.MMDB, old); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(C.Path.MMDB(), old, 0600); err != nil {
		t.Fatal(err)
	}
	geodata.SetGeodataMode(false)
	if err := geodata.InitGeoIP(); err != nil {
		t.Fatal(err)
	}
	if err := mmdb.ReloadIP(); err != nil {
		t.Fatal(err)
	}
	return old, next
}
func receiveGeo(t *testing.T, done <-chan error) error {
	t.Helper()
	select {
	case err := <-done:
		return err
	case <-time.After(3 * time.Second):
		t.Fatal("GEO update did not finish")
		return nil
	}
}
func assertGeoCode(t *testing.T, want string) {
	t.Helper()
	codes := mmdb.IPInstance().LookupCode(net.ParseIP("1.2.3.4"))
	if len(codes) != 1 || codes[0] != want {
		t.Fatalf("codes %v, want %s", codes, want)
	}
}

func TestGeoUpdateAdmissionAndDownloadCancellation(t *testing.T) {
	old, _ := setupGeoUpdate(t)
	entered := make(chan struct{}, 256)
	var requests atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		entered <- struct{}{}
		w.WriteHeader(http.StatusOK)
		w.(http.Flusher).Flush()
		<-r.Context().Done()
	}))
	defer server.Close()
	geodata.SetMmdbUrl(server.URL)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 128)
	start := make(chan struct{})
	for i := 0; i < 128; i++ {
		go func() { <-start; done <- updater.UpdateGeoDatabasesContext(ctx) }()
	}
	close(start)
	select {
	case <-entered:
	case <-time.After(3 * time.Second):
		t.Fatal("download not entered")
	}
	for i := 0; i < 127; i++ {
		if err := receiveGeo(t, done); !errors.Is(err, updater.ErrGetDatabaseUpdateSkip) {
			t.Fatalf("duplicate admission: %v", err)
		}
	}
	if requests.Load() != 1 {
		t.Fatalf("concurrent downloads: %d", requests.Load())
	}
	cancel()
	if err := receiveGeo(t, done); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancellation: %v", err)
	}
	data, err := os.ReadFile(C.Path.MMDB())
	if err != nil || !bytes.Equal(data, old) {
		t.Fatalf("canceled download changed file: %v", err)
	}
	assertGeoCode(t, "aa")
	// Admission is released even on failure, so another canceled operation does
	// not see a permanently stuck updating flag.
	if err := updater.UpdateMMDBContext(ctx); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
}

func TestGeoUpdateRejectsStaleSettingsAndInvalidData(t *testing.T) {
	for _, stale := range []string{"settings", "path", "invalid"} {
		t.Run(stale, func(t *testing.T) {
			old, next := setupGeoUpdate(t)
			originalPath := C.Path.MMDB()
			entered, release := make(chan struct{}), make(chan struct{})
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				close(entered)
				<-release
				if stale == "invalid" {
					_, _ = w.Write([]byte("invalid"))
				} else {
					_, _ = w.Write(next)
				}
			}))
			defer server.Close()
			geodata.SetMmdbUrl(server.URL)
			done := make(chan error, 1)
			go func() { done <- updater.UpdateMMDB() }()
			<-entered
			switch stale {
			case "settings":
				geodata.SetMmdbUrl(server.URL + "/changed")
			case "path":
				C.SetHomeDir(t.TempDir())
			}
			close(release)
			err := receiveGeo(t, done)
			if err == nil {
				t.Fatal("stale/invalid update accepted")
			}
			if stale == "settings" && !errors.Is(err, geodata.ErrSettingsChanged) {
				t.Fatal(err)
			}
			if stale == "path" && !errors.Is(err, C.ErrGeoPathsChanged) {
				t.Fatal(err)
			}
			data, err := os.ReadFile(originalPath)
			if err != nil || !bytes.Equal(old, data) {
				t.Fatalf("rejected update changed file: %v", err)
			}
			temporary, _ := filepath.Glob(filepath.Join(filepath.Dir(originalPath), ".geo-update-*"))
			if len(temporary) != 0 {
				t.Fatalf("temporary files leaked: %v", temporary)
			}
		})
	}
}

func TestGeoRealUpdateConcurrentConfigParseAndQueries(t *testing.T) {
	_, next := setupGeoUpdate(t)
	entered, release := make(chan struct{}, 1), make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { entered <- struct{}{}; <-release; _, _ = w.Write(next) }))
	defer server.Close()
	geodata.SetMmdbUrl(server.URL)
	done := make(chan error, 1)
	go func() { done <- updater.UpdateMMDB() }()
	<-entered
	// Parse applies and rolls back real kernel settings while the actual HTTP
	// update is in flight. Query loops run through mmap replacement too.
	stop := make(chan struct{})
	var readers sync.WaitGroup
	for i := 0; i < 4; i++ {
		readers.Add(1)
		go func() {
			defer readers.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				codes := mmdb.IPInstance().LookupCode(net.ParseIP("1.2.3.4"))
				if len(codes) != 1 || (codes[0] != "aa" && codes[0] != "bb") {
					t.Errorf("partial query: %v", codes)
					return
				}
			}
		}()
	}
	for i := 0; i < 20; i++ {
		yaml := fmt.Sprintf("ipv6: %t\ngeodata-loader: standard\ngeodata-mode: false\ngeo-auto-update: true\ngeox-url:\n  mmdb: %q\nrules:\n  - GEOIP,CN,DIRECT\n  - MATCH,DIRECT\n", i%2 == 0, server.URL)
		if _, err := config.Parse([]byte(yaml)); err != nil {
			t.Errorf("parse: %v", err)
			break
		}
	}
	close(release)
	err := receiveGeo(t, done)
	close(stop)
	readers.Wait()
	if err != nil {
		t.Fatal(err)
	}
	assertGeoCode(t, "bb")
	data, err := os.ReadFile(C.Path.MMDB())
	if err != nil || !bytes.Equal(data, next) {
		t.Fatalf("published file mismatch: %v", err)
	}
}

func TestGeoDownloadsOverlapConfigParsing(t *testing.T) {
	_, next := setupGeoUpdate(t)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { _, _ = w.Write(next) }))
	defer server.Close()
	url := strings.Replace(server.URL, "127.0.0.1", "localhost", 1)
	geodata.SetMmdbUrl(url)
	start := make(chan struct{})
	done := make(chan error, 1)
	go func() {
		<-start
		for i := 0; i < 100; i++ {
			if err := updater.UpdateMMDB(); err != nil && !errors.Is(err, geodata.ErrSettingsChanged) {
				done <- err
				return
			}
		}
		done <- nil
	}()
	close(start)
	for i := 0; i < 100; i++ {
		yaml := fmt.Sprintf("ipv6: %t\ngeodata-loader: standard\ngeox-url:\n  mmdb: %q\nrules:\n  - GEOIP,CN,DIRECT\n  - MATCH,DIRECT\n", i%2 == 0, url)
		if _, err := config.Parse([]byte(yaml)); err != nil {
			t.Errorf("parse: %v", err)
			break
		}
	}
	if err := receiveGeo(t, done); err != nil {
		t.Fatal(err)
	}
}
