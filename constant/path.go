package constant

import (
	"errors"
	"fmt"
	"os"
	P "path"
	"path/filepath"
	"strconv"
	"strings"
	"sync"

	"github.com/metacubex/mihomo/common/utils"
	"github.com/metacubex/mihomo/constant/features"
)

const Name = "mihomo"

const (
	GeositeName   = "GeoSite.dat"
	GeoipName     = "GeoIP.dat"
	ASNName       = "ASN.mmdb"
	BundleMRSName = "BundleMRS.7z"
)

// Path is used to get the configuration path
//
// on Unix systems, `$HOME/.config/mihomo`.
// on Windows, `%USERPROFILE%/.config/mihomo`.
var Path = func() *path {
	homeDir, err := os.UserHomeDir()
	if err != nil {
		homeDir, _ = os.Getwd()
	}
	allowUnsafePath, _ := strconv.ParseBool(os.Getenv("SKIP_SAFE_PATH_CHECK"))
	homeDir = P.Join(homeDir, ".config", Name)

	if _, err = os.Stat(homeDir); err != nil {
		if configHome, ok := os.LookupEnv("XDG_CONFIG_HOME"); ok {
			homeDir = P.Join(configHome, Name)
		}
	}

	var safePaths []string
	for _, safePath := range filepath.SplitList(os.Getenv("SAFE_PATHS")) {
		safePath = strings.TrimSpace(safePath)
		if len(safePath) == 0 {
			continue
		}
		safePaths = append(safePaths, safePath)
	}

	return &path{homeDir: homeDir, configFile: "config.yaml", allowUnsafePath: allowUnsafePath, safePaths: safePaths}
}()

type path struct {
	mu              sync.RWMutex
	generation      uint64
	homeDir         string
	configFile      string
	allowUnsafePath bool
	safePaths       []string
}

// SetHomeDir is used to set the configuration path
func SetHomeDir(root string) {
	Path.mu.Lock()
	defer Path.mu.Unlock()
	if Path.homeDir != root {
		Path.homeDir = root
		Path.generation++
	}
}

// SetConfig is used to set the configuration file
func SetConfig(file string) {
	Path.mu.Lock()
	defer Path.mu.Unlock()
	Path.configFile = file
}

func (p *path) HomeDir() string {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.homeDir
}

func (p *path) Config() string {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.configFile
}

// Resolve return a absolute path or a relative path with homedir
func (p *path) Resolve(path string) string {
	if !filepath.IsAbs(path) {
		return filepath.Join(p.HomeDir(), path)
	}
	return path
}

// IsSafePath return true if path is a subpath of homedir (or in the SAFE_PATHS environment variable)
func (p *path) IsSafePath(path string) bool {
	if p.allowUnsafePath || features.CMFA {
		return true
	}
	safePaths := p.SafePaths()
	if !filepath.IsAbs(path) {
		path = filepath.Join(safePaths[0], path)
	}
	for _, safePath := range safePaths {
		if rel, err := filepath.Rel(safePath, path); err == nil {
			if filepath.IsLocal(rel) {
				return true
			}
		}
	}
	return false
}

func (p *path) SafePaths() []string {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return append([]string{p.homeDir}, p.safePaths...) // add homedir to safePaths
}

func (p *path) ErrNotSafePath(path string) error {
	return ErrNotSafePath{Path: path, SafePaths: p.SafePaths()}
}

type ErrNotSafePath struct {
	Path      string
	SafePaths []string
}

func (e ErrNotSafePath) Error() string {
	return fmt.Sprintf("path is not subpath of home directory or SAFE_PATHS: %s \n allowed paths: %s", e.Path, e.SafePaths)
}

func (p *path) GetPathByHash(prefix, name string) string {
	hash := utils.MakeHash([]byte(name))
	filename := hash.String()
	return filepath.Join(p.HomeDir(), prefix, filename)
}

// GeoPaths is one path snapshot. Queries never rewrite canonical file names.
type GeoPaths struct {
	HomeDir    string
	Generation uint64
	MMDB       string
	ASN        string
	GeoIP      string
	GeoSite    string
	BundleMRS  string
}

func (p *path) Generation() uint64 {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.generation
}

var ErrGeoPathsChanged = errors.New("GEO paths changed during update")

// WithGeoPaths keeps a file publication in its original home generation. The
// callback must not query or modify Path; use the supplied immutable snapshot.
func (p *path) WithGeoPaths(paths GeoPaths, publish func() error) error {
	p.mu.RLock()
	defer p.mu.RUnlock()
	if p.generation != paths.Generation || p.homeDir != paths.HomeDir {
		return ErrGeoPathsChanged
	}
	return publish()
}

func (p *path) GeoPaths() GeoPaths {
	p.mu.RLock()
	home, generation := p.homeDir, p.generation
	p.mu.RUnlock()
	paths := GeoPaths{
		HomeDir: home, Generation: generation,
		MMDB: P.Join(home, "geoip.metadb"), ASN: P.Join(home, ASNName),
		GeoIP: P.Join(home, GeoipName), GeoSite: P.Join(home, GeositeName),
		BundleMRS: P.Join(home, BundleMRSName),
	}
	files, _ := os.ReadDir(home)
	var foundMMDB, foundASN, foundIP, foundSite, foundBundle bool
	for _, file := range files {
		if file.IsDir() {
			continue
		}
		name := file.Name()
		switch strings.ToLower(name) {
		case "country.mmdb", "geoip.db", "geoip.metadb":
			if !foundMMDB {
				paths.MMDB = P.Join(home, name)
				foundMMDB = true
			}
		case "asn.mmdb":
			if !foundASN {
				paths.ASN = P.Join(home, name)
				foundASN = true
			}
		case "geoip.dat":
			if !foundIP {
				paths.GeoIP = P.Join(home, name)
				foundIP = true
			}
		case "geosite.dat":
			if !foundSite {
				paths.GeoSite = P.Join(home, name)
				foundSite = true
			}
		case "bundlemrs.7z":
			if !foundBundle {
				paths.BundleMRS = P.Join(home, name)
				foundBundle = true
			}
		}
	}
	return paths
}

func (p *path) MMDB() string                        { return p.GeoPaths().MMDB }
func (p *path) ASN() string                         { return p.GeoPaths().ASN }
func (p *path) BundleMRS() string                   { return p.GeoPaths().BundleMRS }
func (p *path) GeoIP() string                       { return p.GeoPaths().GeoIP }
func (p *path) GeoSite() string                     { return p.GeoPaths().GeoSite }
func (p *path) OldCache() string                    { return P.Join(p.HomeDir(), ".cache") }
func (p *path) Cache() string                       { return P.Join(p.HomeDir(), "cache.db") }
func (p *path) GetAssetLocation(file string) string { return p.Resolve(file) }

func (p *path) GetExecutableFullPath() string {
	exePath, err := os.Executable()
	if err != nil {
		return "mihomo"
	}
	res, _ := filepath.EvalSymlinks(exePath)
	return res
}
