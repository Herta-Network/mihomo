package constant

import (
	"os"
	"path/filepath"
	"sync"
	"testing"
)

func TestGeoPathQueriesAreReadOnly(t *testing.T) {
	dir := t.TempDir()
	for _, name := range []string{"Country.mmdb", "gEoIp.DaT", "gEoSiTe.DaT", "AsN.mMdB", "BuNdLeMrS.7z"} {
		if err := os.WriteFile(filepath.Join(dir, name), nil, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	p := &path{homeDir: dir}
	_ = p.MMDB()
	_ = p.GeoIP()
	_ = p.GeoSite()
	_ = p.ASN()
	_ = p.BundleMRS()
	if GeoipName != "GeoIP.dat" || GeositeName != "GeoSite.dat" || ASNName != "ASN.mmdb" || BundleMRSName != "BundleMRS.7z" {
		t.Fatal("path lookup rewrote global canonical asset names")
	}
}

func TestGeoPathQueriesConcurrent(t *testing.T) {
	dir := t.TempDir()
	for _, name := range []string{"Country.mmdb", "GeoIP.dat", "GeoSite.dat"} {
		if err := os.WriteFile(filepath.Join(dir, name), nil, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	p := &path{homeDir: dir}
	var workers sync.WaitGroup
	for i := 0; i < 3; i++ {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for j := 0; j < 100; j++ {
				_ = p.MMDB()
				_ = p.GeoIP()
				_ = p.GeoSite()
				_ = GeoipName
				_ = GeositeName
			}
		}()
	}
	workers.Wait()
}

func TestHomeAndConfigConcurrentSnapshots(t *testing.T) {
	previousHome, previousConfig := Path.HomeDir(), Path.Config()
	t.Cleanup(func() { SetHomeDir(previousHome); SetConfig(previousConfig) })
	first, second := t.TempDir(), t.TempDir()
	SetHomeDir(first)
	var workers sync.WaitGroup
	workers.Add(2)
	go func() {
		defer workers.Done()
		for i := 0; i < 500; i++ {
			SetHomeDir(first)
			SetConfig("first.yaml")
			SetHomeDir(second)
			SetConfig("second.yaml")
		}
	}()
	go func() {
		defer workers.Done()
		for i := 0; i < 500; i++ {
			_ = Path.HomeDir()
			_ = Path.Config()
			_ = Path.Resolve("GeoIP.dat")
			_ = Path.SafePaths()
			_ = Path.GetAssetLocation("GeoSite.dat")
		}
	}()
	workers.Wait()
}
