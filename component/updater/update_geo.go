package updater

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync/atomic"
	"time"

	"github.com/metacubex/mihomo/common/utils"
	"github.com/metacubex/mihomo/component/geodata"
	"github.com/metacubex/mihomo/component/resource"
	C "github.com/metacubex/mihomo/constant"
	"github.com/metacubex/mihomo/log"
	"golang.org/x/sync/errgroup"
)

var (
	autoUpdate     atomic.Bool
	updateInterval atomic.Int64
	updatingGeo    atomic.Bool
)

func GeoAutoUpdate() bool            { return autoUpdate.Load() }
func GeoUpdateInterval() int         { return int(updateInterval.Load()) }
func SetGeoAutoUpdate(value bool)    { autoUpdate.Store(value) }
func SetGeoUpdateInterval(value int) { updateInterval.Store(int64(value)) }

var ErrGetDatabaseUpdateSkip = errors.New("GEO database is updating, skip")

func withGeoUpdate(ctx context.Context, update func(geodata.Settings, C.GeoPaths) error) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if !updatingGeo.CompareAndSwap(false, true) {
		return ErrGetDatabaseUpdateSkip
	}
	defer updatingGeo.Store(false)
	return update(geodata.GetSettings(), C.Path.GeoPaths())
}

func updateDatabase(ctx context.Context, kind geodata.Database, settings geodata.Settings, paths C.GeoPaths) error {
	vehicle := resource.NewHTTPVehicle(kind.URL(settings), kind.Path(paths), "", nil, defaultHttpTimeout, 0)
	var oldHash utils.HashType
	if data, err := os.ReadFile(vehicle.Path()); err == nil {
		oldHash = utils.MakeHash(data)
	}
	data, hash, err := vehicle.Read(ctx, oldHash)
	if err != nil {
		return fmt.Errorf("download %s: %w", vehicle.Path(), err)
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if oldHash.Equal(hash) {
		return nil
	}
	if err := geodata.InstallDatabase(ctx, kind, settings, paths, data); err != nil {
		return fmt.Errorf("install %s: %w", vehicle.Path(), err)
	}
	return nil
}

func updateOne(ctx context.Context, kind geodata.Database) error {
	return withGeoUpdate(ctx, func(settings geodata.Settings, paths C.GeoPaths) error {
		return updateDatabase(ctx, kind, settings, paths)
	})
}
func UpdateMMDB() error                              { return UpdateMMDBContext(context.Background()) }
func UpdateASN() error                               { return UpdateASNContext(context.Background()) }
func UpdateGeoIp() error                             { return UpdateGeoIpContext(context.Background()) }
func UpdateGeoSite() error                           { return UpdateGeoSiteContext(context.Background()) }
func UpdateMMDBContext(ctx context.Context) error    { return updateOne(ctx, geodata.MMDB) }
func UpdateASNContext(ctx context.Context) error     { return updateOne(ctx, geodata.ASN) }
func UpdateGeoIpContext(ctx context.Context) error   { return updateOne(ctx, geodata.GeoIP) }
func UpdateGeoSiteContext(ctx context.Context) error { return updateOne(ctx, geodata.GeoSite) }

func UpdateGeoDatabases() error { return UpdateGeoDatabasesContext(context.Background()) }

// Downloads inherit cancellation. Publication never acquires an embedding
// engine's lifecycle lock, so Stop may cancel and join while holding that lock.
func UpdateGeoDatabasesContext(ctx context.Context) error {
	return withGeoUpdate(ctx, func(settings geodata.Settings, paths C.GeoPaths) error {
		log.Infoln("[GEO] Updating GEO database")
		group, ctx := errgroup.WithContext(ctx)
		kinds := make([]geodata.Database, 0, 3)
		if geodata.GeoIpEnable() {
			kind := geodata.MMDB
			if settings.Mode {
				kind = geodata.GeoIP
			}
			kinds = append(kinds, kind)
		}
		if geodata.ASNEnable() {
			kinds = append(kinds, geodata.ASN)
		}
		if geodata.GeoSiteEnable() {
			kinds = append(kinds, geodata.GeoSite)
		}
		for _, kind := range kinds {
			group.Go(func() error { return updateDatabase(ctx, kind, settings, paths) })
		}
		return group.Wait()
	})
}

func getUpdateTime() (time.Time, error) {
	paths := C.Path.GeoPaths()
	var err error
	for _, file := range []string{paths.GeoIP, paths.MMDB, paths.ASN, paths.GeoSite} {
		var info os.FileInfo
		info, err = os.Stat(file)
		if err == nil {
			return info.ModTime(), nil
		}
	}
	return time.Time{}, err
}

func RegisterGeoUpdater() {
	interval := GeoUpdateInterval()
	if interval <= 0 {
		log.Errorln("[GEO] Invalid update interval: %d", interval)
		return
	}
	go func() {
		ticker := time.NewTicker(time.Duration(interval) * time.Hour)
		defer ticker.Stop()
		last, err := getUpdateTime()
		if err != nil {
			log.Errorln("[GEO] Get update time: %s", err)
			return
		}
		if last.Add(time.Duration(interval) * time.Hour).Before(time.Now()) {
			if err := UpdateGeoDatabases(); err != nil {
				log.Errorln("[GEO] Update: %s", err)
			}
		}
		for range ticker.C {
			if err := UpdateGeoDatabases(); err != nil {
				log.Errorln("[GEO] Update: %s", err)
			}
		}
	}()
}
