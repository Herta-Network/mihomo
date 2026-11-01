package mmdb

import (
	"errors"
	"os"
	"runtime"
	"slices"
	"sync"

	C "github.com/metacubex/mihomo/constant"
	"github.com/metacubex/mihomo/log"
	"github.com/oschwald/maxminddb-golang"
)

// A handle never exposes the mmap reader. The read lock leases the mapping for
// an entire lookup; replacement waits for these leases before closing it.
type database struct {
	mu         sync.RWMutex
	reader     *maxminddb.Reader
	path       string
	generation uint64
	mapped     bool
	asn        bool
}

var ipDatabase database
var asnDatabase = database{asn: true}

func (d *database) filename(paths C.GeoPaths) string {
	if d.asn {
		return paths.ASN
	}
	return paths.MMDB
}

func (d *database) acquire() (*maxminddb.Reader, func()) {
	for {
		generation := C.Path.Generation()
		d.mu.RLock()
		if d.reader != nil && d.generation == generation {
			return d.reader, d.mu.RUnlock
		}
		d.mu.RUnlock()
		paths := C.Path.GeoPaths()
		err := C.Path.WithGeoPaths(paths, func() error {
			d.mu.Lock()
			defer d.mu.Unlock()
			if d.reader == nil || d.generation != paths.Generation {
				reader, err := maxminddb.Open(d.filename(paths))
				if err != nil {
					return err
				}
				d.swap(reader, d.filename(paths), paths.Generation, true)
			}
			return nil
		})
		if errors.Is(err, C.ErrGeoPathsChanged) {
			continue
		}
		if err != nil {
			log.Errorln("Can't load MMDB %s: %s", d.filename(paths), err)
			return nil, func() {}
		}
		// A replacement may have happened meanwhile; either complete reader is valid.
		d.mu.RLock()
		return d.reader, d.mu.RUnlock
	}
}

// swap requires the write lock, so no lookup can still use the old mapping.
func (d *database) swap(next *maxminddb.Reader, path string, generation uint64, mapped bool) {
	old := d.reader
	d.reader, d.path, d.generation, d.mapped = next, path, generation, mapped
	if old != nil {
		_ = old.Close()
	}
}

func LoadFromBytes(buffer []byte) {
	generation := C.Path.Generation()
	ipDatabase.mu.Lock()
	defer ipDatabase.mu.Unlock()
	if ipDatabase.reader != nil {
		return
	}
	reader, err := maxminddb.FromBytes(slices.Clone(buffer))
	if err != nil {
		log.Errorln("Can't load mmdb: %s", err)
		return
	}
	ipDatabase.swap(reader, "", generation, false)
}

func Verify(path string) bool {
	instance, err := maxminddb.Open(path)
	if err == nil {
		_ = instance.Close()
	}
	return err == nil
}

func IPInstance() IPReader   { return IPReader{database: &ipDatabase} }
func ASNInstance() ASNReader { return ASNReader{database: &asnDatabase} }

func (d *database) reload() error {
	paths := C.Path.GeoPaths()
	return C.Path.WithGeoPaths(paths, func() error {
		// Open under the same lock as install: an earlier open must never replace a
		// database committed by a concurrent installer with its now-stale mapping.
		d.mu.Lock()
		defer d.mu.Unlock()
		reader, err := maxminddb.Open(d.filename(paths))
		if err != nil {
			return err
		}
		d.swap(reader, d.filename(paths), paths.Generation, true)
		return nil
	})
}
func ReloadIP() error  { return ipDatabase.reload() }
func ReloadASN() error { return asnDatabase.reload() }

// InstallIP/ASN take ownership of a validated byte slice on success.
// commit atomically replaces the file and must not call MMDB lookup functions.
// A successful commit always publishes its reader, even if cancellation arrives
// after the rename. The caller checks cancellation before entering commit.
func InstallIP(path string, generation uint64, data []byte, commit func() error) error {
	return ipDatabase.install(path, generation, data, commit)
}
func InstallASN(path string, generation uint64, data []byte, commit func() error) error {
	return asnDatabase.install(path, generation, data, commit)
}
func (d *database) install(path string, generation uint64, data []byte, commit func() error) error {
	next, err := maxminddb.FromBytes(data)
	if err != nil {
		return err
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	// Windows cannot replace a mapped file. Keep a validated heap-backed fallback
	// before unmapping so failed publication still leaves a usable old database.
	if runtime.GOOS == "windows" && d.reader != nil && d.mapped && d.path == path {
		oldData, readErr := os.ReadFile(d.path)
		if readErr != nil {
			_ = next.Close()
			return readErr
		}
		rollback, readErr := maxminddb.FromBytes(oldData)
		if readErr != nil {
			_ = next.Close()
			return readErr
		}
		d.swap(rollback, d.path, d.generation, false)
	}
	if err := commit(); err != nil {
		_ = next.Close()
		return err
	}
	mapped := true
	reader, err := maxminddb.Open(path)
	if err != nil {
		// The committed data was already validated. Do not leave disk and lookup
		// state inconsistent if reopening the mmap fails after publication.
		reader, mapped = next, false
		log.Warnln("MMDB mmap unavailable after update, using memory: %s", err)
	} else {
		_ = next.Close()
	}
	d.swap(reader, path, generation, mapped)
	return nil
}

// Close releases mappings after in-flight lookups finish. Existing handles can
// lazily open the configured database again on their next query.
func Close() error {
	return errors.Join(ipDatabase.close(), asnDatabase.close())
}
func (d *database) close() error {
	d.mu.Lock()
	defer d.mu.Unlock()
	old := d.reader
	d.reader, d.path, d.mapped = nil, "", false
	if old != nil {
		return old.Close()
	}
	return nil
}
