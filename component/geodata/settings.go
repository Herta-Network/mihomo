package geodata

import (
	"context"
	"errors"
	"sync/atomic"
)

type Settings struct {
	Mode        bool
	Loader      string
	SiteMatcher string
	GeoIPURL    string
	GeoSiteURL  string
	MMDBURL     string
	ASNURL      string
}

type settingsVersion struct {
	Settings
	version uint64
}

var geoSettings = func() *atomic.Pointer[settingsVersion] {
	value := new(atomic.Pointer[settingsVersion])
	value.Store(&settingsVersion{Settings: Settings{Loader: "memconservative", SiteMatcher: "succinct"}})
	return value
}()

// Publication and settings changes are short transactions. Downloads never hold
// this gate, and a canceled update can stop while waiting to publish its files.
var geoPublication = make(chan struct{}, 1)

func GetSettings() Settings { return geoSettings.Load().Settings }

func SetSettings(settings Settings) {
	changeSettings(func(next *Settings) { *next = settings })
}

func changeSettings(change func(*Settings)) {
	geoPublication <- struct{}{}
	defer func() { <-geoPublication }()
	previous := geoSettings.Load()
	next := previous.Settings
	change(&next)
	if next.Loader == "memc" {
		next.Loader = "memconservative"
	}
	switch next.SiteMatcher {
	case "mph", "hybrid":
		next.SiteMatcher = "mph"
	default:
		next.SiteMatcher = "succinct"
	}
	if next == previous.Settings {
		return
	}
	geoSettings.Store(&settingsVersion{Settings: next, version: previous.version + 1})
	ClearGeoIPCache()
	ClearGeoSiteCache()
}

var ErrSettingsChanged = errors.New("GEO settings changed during update")

// Publish runs a validated local-file commit against the settings used to fetch
// it. The callback must not download, acquire an engine lifecycle lock, or change
// GEO settings. Once it commits, it must finish cache/reader publication even if
// cancellation arrives during that commit.
func Publish(ctx context.Context, settings Settings, commit func() error) error {
	select {
	case geoPublication <- struct{}{}:
	case <-ctx.Done():
		return ctx.Err()
	}
	defer func() { <-geoPublication }()
	if err := ctx.Err(); err != nil {
		return err
	}
	if GetSettings() != settings {
		return ErrSettingsChanged
	}
	return commit()
}
