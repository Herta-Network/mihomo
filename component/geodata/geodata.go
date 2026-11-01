package geodata

import (
	"fmt"
	"sync"

	"github.com/metacubex/mihomo/component/geodata/router"
	C "github.com/metacubex/mihomo/constant"
)

type loader struct {
	LoaderImplementation
}

func (l *loader) LoadGeoSite(list string) ([]*router.Domain, error) {
	return l.LoadSiteByPath(C.Path.GeoSite(), list)
}

func (l *loader) LoadGeoIP(country string) ([]*router.CIDR, error) {
	return l.LoadIPByPath(C.Path.GeoIP(), country)
}

var loaders map[string]func() LoaderImplementation
var loadersMutex sync.RWMutex

func RegisterGeoDataLoaderImplementationCreator(name string, loader func() LoaderImplementation) {
	loadersMutex.Lock()
	defer loadersMutex.Unlock()
	if loaders == nil {
		loaders = map[string]func() LoaderImplementation{}
	}
	loaders[name] = loader
}

func getGeoDataLoaderImplementation(name string) (LoaderImplementation, error) {
	loadersMutex.RLock()
	geoLoader, ok := loaders[name]
	loadersMutex.RUnlock()
	if ok {
		return geoLoader(), nil
	}
	return nil, fmt.Errorf("unable to locate GeoData loader %s", name)
}

func GetGeoDataLoader(name string) (Loader, error) {
	loadImpl, err := getGeoDataLoaderImplementation(name)
	if err == nil {
		return &loader{loadImpl}, nil
	}
	return nil, err
}
