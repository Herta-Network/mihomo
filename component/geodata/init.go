package geodata

import (
	"context"
	"fmt"
	"io"
	"os"
	"sync"
	"time"

	"github.com/metacubex/mihomo/common/atomic"
	mihomoHttp "github.com/metacubex/mihomo/component/http"
	C "github.com/metacubex/mihomo/constant"
	"github.com/metacubex/mihomo/log"

	"github.com/metacubex/http"
)

var (
	geoIpEnable   atomic.Bool
	geoSiteEnable atomic.Bool
	asnEnable     atomic.Bool
	initialFiles  [4]struct {
		sync.Mutex
		info os.FileInfo
		path string
	}
)

func GeoIpUrl() string {
	return GetSettings().GeoIPURL
}

func SetGeoIpUrl(url string) {
	changeSettings(func(next *Settings) { next.GeoIPURL = url })
}

func MmdbUrl() string {
	return GetSettings().MMDBURL
}

func SetMmdbUrl(url string) {
	changeSettings(func(next *Settings) { next.MMDBURL = url })
}

func GeoSiteUrl() string {
	return GetSettings().GeoSiteURL
}

func SetGeoSiteUrl(url string) {
	changeSettings(func(next *Settings) { next.GeoSiteURL = url })
}

func ASNUrl() string {
	return GetSettings().ASNURL
}

func SetASNUrl(url string) {
	changeSettings(func(next *Settings) { next.ASNURL = url })
}

func initializeDatabase(kind Database, settings Settings) error {
	paths := C.Path.GeoPaths()
	filename := kind.Path(paths)
	cached := &initialFiles[kind]
	cached.Lock()
	defer cached.Unlock()
	info, statErr := os.Stat(filename)
	if statErr == nil && cached.path == filename && cached.info != nil && os.SameFile(info, cached.info) && info.Size() == cached.info.Size() && info.ModTime() == cached.info.ModTime() {
		return nil
	}
	if data, err := os.ReadFile(filename); err == nil && ValidateDatabase(kind, data) == nil {
		cached.path, cached.info = filename, info
		return nil
	}
	log.Infoln("GEO database missing or invalid, downloading: %s", filename)
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	resp, err := mihomoHttp.HttpRequest(ctx, kind.URL(settings), http.MethodGet, nil, nil)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		return fmt.Errorf("GEO download: %s", resp.Status)
	}
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}
	if err = installDatabase(ctx, kind, settings, paths, data, true); err != nil {
		return fmt.Errorf("install GEO database: %w", err)
	}
	cached.path = filename
	cached.info, _ = os.Stat(filename)
	return nil
}

func InitGeoSite() error {
	geoSiteEnable.Store(true)
	return initializeDatabase(GeoSite, GetSettings())
}
func InitGeoIP() error {
	geoIpEnable.Store(true)
	settings := GetSettings()
	if settings.Mode {
		return initializeDatabase(GeoIP, settings)
	}
	return initializeDatabase(MMDB, settings)
}
func InitASN() error {
	asnEnable.Store(true)
	return initializeDatabase(ASN, GetSettings())
}

func GeoIpEnable() bool {
	return geoIpEnable.Load()
}

func GeoSiteEnable() bool {
	return geoSiteEnable.Load()
}

func ASNEnable() bool {
	return asnEnable.Load()
}
