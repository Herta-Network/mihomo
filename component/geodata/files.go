package geodata

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/metacubex/mihomo/component/geodata/router"
	"github.com/metacubex/mihomo/component/mmdb"
	C "github.com/metacubex/mihomo/constant"
	"github.com/oschwald/maxminddb-golang"
	"google.golang.org/protobuf/proto"
)

type Database uint8

const (
	MMDB Database = iota
	ASN
	GeoIP
	GeoSite
)

func (kind Database) Path(paths C.GeoPaths) string {
	switch kind {
	case MMDB:
		return paths.MMDB
	case ASN:
		return paths.ASN
	case GeoIP:
		return paths.GeoIP
	case GeoSite:
		return paths.GeoSite
	}
	return ""
}
func (kind Database) URL(settings Settings) string {
	switch kind {
	case MMDB:
		return settings.MMDBURL
	case ASN:
		return settings.ASNURL
	case GeoIP:
		return settings.GeoIPURL
	case GeoSite:
		return settings.GeoSiteURL
	}
	return ""
}

func ValidateDatabase(kind Database, data []byte) error {
	if len(data) == 0 {
		return fmt.Errorf("empty GEO database")
	}
	switch kind {
	case MMDB, ASN:
		reader, err := maxminddb.FromBytes(data)
		if err != nil {
			return err
		}
		defer reader.Close()
		return nil
	case GeoIP:
		var list router.GeoIPList
		if err := proto.Unmarshal(data, &list); err != nil {
			return err
		}
		for _, entry := range list.Entry {
			if strings.EqualFold(entry.CountryCode, "cn") {
				_, err := router.NewGeoIPMatcher(entry.Cidr)
				return err
			}
		}
	case GeoSite:
		var list router.GeoSiteList
		if err := proto.Unmarshal(data, &list); err != nil {
			return err
		}
		for _, entry := range list.Entry {
			if strings.EqualFold(entry.CountryCode, "cn") {
				_, err := router.NewSuccinctMatcherGroup(entry.Domain)
				return err
			}
		}
	default:
		return fmt.Errorf("unknown GEO database kind: %d", kind)
	}
	return fmt.Errorf("CN entry missing from GEO database")
}

// InstallDatabase stages and validates without holding publication locks. The
// final rename and reader/cache switch form a short, non-cancellable commit.
// data is owned by this call and must not be modified afterward.
func InstallDatabase(ctx context.Context, kind Database, settings Settings, paths C.GeoPaths, data []byte) error {
	return installDatabase(ctx, kind, settings, paths, data, false)
}

func installDatabase(ctx context.Context, kind Database, settings Settings, paths C.GeoPaths, data []byte, onlyIfInvalid bool) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := ValidateDatabase(kind, data); err != nil {
		return err
	}
	target := kind.Path(paths)
	if err := os.MkdirAll(filepath.Dir(target), 0755); err != nil {
		return err
	}
	file, err := os.CreateTemp(filepath.Dir(target), ".geo-update-*")
	if err != nil {
		return err
	}
	staged := file.Name()
	defer os.Remove(staged)
	if err = file.Chmod(0644); err == nil {
		_, err = file.Write(data)
	}
	if err == nil {
		err = file.Sync()
	}
	closeErr := file.Close()
	if err != nil {
		return err
	}
	if closeErr != nil {
		return closeErr
	}
	return Publish(ctx, settings, func() error {
		return C.Path.WithGeoPaths(paths, func() error {
			// Another initializer or updater may have published a valid file
			// while this initial download was in flight. Keep that publication.
			if onlyIfInvalid {
				current, err := os.ReadFile(target)
				if err == nil && ValidateDatabase(kind, current) == nil {
					return nil
				}
			}
			commit := func() error {
				if err := ctx.Err(); err != nil {
					return err
				}
				return os.Rename(staged, target)
			}
			switch kind {
			case MMDB:
				return mmdb.InstallIP(target, paths.Generation, data, commit)
			case ASN:
				return mmdb.InstallASN(target, paths.Generation, data, commit)
			default:
				if err := commit(); err != nil {
					return err
				}
				if kind == GeoIP {
					ClearGeoIPCache()
				} else {
					ClearGeoSiteCache()
				}
				return nil
			}
		})
	})
}
