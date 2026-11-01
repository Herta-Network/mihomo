package geodata

import (
	"fmt"
	"strings"

	"github.com/metacubex/mihomo/common/singleflight"
	"github.com/metacubex/mihomo/component/geodata/router"
	C "github.com/metacubex/mihomo/constant"
	"github.com/metacubex/mihomo/log"
)

func GeodataMode() bool        { return GetSettings().Mode }
func LoaderName() string       { return GetSettings().Loader }
func SiteMatcherName() string  { return GetSettings().SiteMatcher }
func SetGeodataMode(mode bool) { changeSettings(func(next *Settings) { next.Mode = mode }) }
func SetLoader(loader string)  { changeSettings(func(next *Settings) { next.Loader = loader }) }
func SetSiteMatcher(matcher string) {
	changeSettings(func(next *Settings) { next.SiteMatcher = matcher })
}

func Verify(name string) error {
	switch name {
	case C.GeositeName:
		_, err := LoadGeoSiteMatcher("CN")
		return err
	case C.GeoipName:
		_, err := LoadGeoIPMatcher("CN")
		return err
	default:
		return fmt.Errorf("not support name")
	}
}

var loadGeoSiteMatcherListSF = singleflight.Group[[]*router.Domain]{StoreResult: true}
var loadGeoSiteMatcherSF = singleflight.Group[router.DomainMatcher]{StoreResult: true}

func LoadGeoSiteMatcher(countryCode string) (router.DomainMatcher, error) {
	settings := geoSettings.Load()
	paths := C.Path.GeoPaths()
	if countryCode == "" {
		return nil, fmt.Errorf("country code could not be empty")
	}

	not := false
	if countryCode[0] == '!' {
		not = true
		countryCode = countryCode[1:]
		if countryCode == "" {
			return nil, fmt.Errorf("country code could not be empty")
		}
	}
	countryCode = strings.ToLower(countryCode)

	parts := strings.Split(countryCode, "@")
	listName := strings.TrimSpace(parts[0])
	attrVal := parts[1:]
	attrs := parseAttrs(attrVal)

	if listName == "" {
		return nil, fmt.Errorf("empty listname in rule: %s", countryCode)
	}

	matcherName := listName
	if !attrs.IsEmpty() {
		matcherName += "@" + attrs.String()
	}
	cacheKey := fmt.Sprintf("%d:%s:%s", settings.version, paths.GeoSite, matcherName)
	listKey := fmt.Sprintf("%d:%s:%s", settings.version, paths.GeoSite, listName)
	matcher, err, shared := loadGeoSiteMatcherSF.Do(cacheKey, func() (router.DomainMatcher, error) {
		log.Infoln("Load GeoSite rule: %s", matcherName)
		domains, err, shared := loadGeoSiteMatcherListSF.Do(listKey, func() ([]*router.Domain, error) {
			geoLoader, err := GetGeoDataLoader(settings.Loader)
			if err != nil {
				return nil, err
			}
			return geoLoader.LoadSiteByPath(paths.GeoSite, listName)
		})
		if err != nil {
			if !shared {
				loadGeoSiteMatcherListSF.Forget(listKey) // don't store the error result
			}
			return nil, err
		}

		if attrs.IsEmpty() {
			if strings.Contains(countryCode, "@") {
				log.Warnln("empty attribute list: %s", countryCode)
			}
		} else {
			filteredDomains := make([]*router.Domain, 0, len(domains))
			hasAttrMatched := false
			for _, domain := range domains {
				if attrs.Match(domain) {
					hasAttrMatched = true
					filteredDomains = append(filteredDomains, domain)
				}
			}
			if !hasAttrMatched {
				log.Warnln("attribute match no rule: geosite: %s", countryCode)
			}
			domains = filteredDomains
		}

		/**
		linear: linear algorithm
		matcher, err := router.NewDomainMatcher(domains)
		mph：minimal perfect hash algorithm
		*/
		if settings.SiteMatcher == "mph" {
			return router.NewMphMatcherGroup(domains)
		} else {
			return router.NewSuccinctMatcherGroup(domains)
		}
	})
	if err != nil {
		if !shared {
			loadGeoSiteMatcherSF.Forget(cacheKey) // don't store the error result
		}
		return nil, err
	}
	if not {
		matcher = router.NewNotDomainMatcherGroup(matcher)
	}

	return matcher, nil
}

var loadGeoIPMatcherSF = singleflight.Group[router.IPMatcher]{StoreResult: true}

func LoadGeoIPMatcher(country string) (router.IPMatcher, error) {
	settings := geoSettings.Load()
	paths := C.Path.GeoPaths()
	if len(country) == 0 {
		return nil, fmt.Errorf("country code could not be empty")
	}

	not := false
	if country[0] == '!' {
		not = true
		country = country[1:]
	}
	country = strings.ToLower(country)

	cacheKey := fmt.Sprintf("%d:%s:%s", settings.version, paths.GeoIP, country)
	matcher, err, shared := loadGeoIPMatcherSF.Do(cacheKey, func() (router.IPMatcher, error) {
		log.Infoln("Load GeoIP rule: %s", country)
		geoLoader, err := GetGeoDataLoader(settings.Loader)
		if err != nil {
			return nil, err
		}
		cidrList, err := geoLoader.LoadIPByPath(paths.GeoIP, country)
		if err != nil {
			return nil, err
		}
		return router.NewGeoIPMatcher(cidrList)
	})
	if err != nil {
		if !shared {
			loadGeoIPMatcherSF.Forget(cacheKey) // don't store the error result
			log.Warnln("Load GeoIP rule: %s", country)
		}
		return nil, err
	}
	if not {
		matcher = router.NewNotIpMatcherGroup(matcher)
	}
	return matcher, nil
}

func ClearGeoSiteCache() {
	loadGeoSiteMatcherListSF.Reset()
	loadGeoSiteMatcherSF.Reset()
}

func ClearGeoIPCache() {
	loadGeoIPMatcherSF.Reset()
}
