package mmdb

import (
	"fmt"
	"net"
	"strings"

	"github.com/metacubex/mihomo/log"
)

type geoip2Country struct {
	Country struct {
		IsoCode string `maxminddb:"iso_code"`
	} `maxminddb:"country"`
}

type IPReader struct{ database *database }

type ASNReader struct{ database *database }

type GeoLite2 struct {
	AutonomousSystemNumber       uint32 `maxminddb:"autonomous_system_number"`
	AutonomousSystemOrganization string `maxminddb:"autonomous_system_organization"`
}

type IPInfo struct {
	ASN  string `maxminddb:"asn"`
	Name string `maxminddb:"name"`
}

func (r IPReader) LookupCode(ipAddress net.IP) []string {
	reader, release := r.database.acquire()
	defer release()
	if reader == nil {
		return nil
	}
	switch reader.Metadata.DatabaseType {
	default:
		var country geoip2Country
		_ = reader.Lookup(ipAddress, &country)
		if country.Country.IsoCode == "" {
			return []string{}
		}
		return []string{strings.ToLower(country.Country.IsoCode)}

	case "sing-geoip":
		var code string
		_ = reader.Lookup(ipAddress, &code)
		if code == "" {
			return []string{}
		}
		return []string{code}

	case "Meta-geoip0":
		var record any
		_ = reader.Lookup(ipAddress, &record)
		switch record := record.(type) {
		case string:
			return []string{record}
		case []any: // lookup returned type of slice is []any
			result := make([]string, 0, len(record))
			for _, item := range record {
				if code, ok := item.(string); ok {
					result = append(result, code)
				}
			}
			return result
		}
		return []string{}

	}
}

func (r ASNReader) LookupASN(ip net.IP) (string, string) {
	reader, release := r.database.acquire()
	defer release()
	if reader == nil {
		return "", ""
	}
	switch reader.Metadata.DatabaseType {
	case "GeoLite2-ASN", "DBIP-ASN-Lite (compat=GeoLite2-ASN)":
		var result GeoLite2
		_ = reader.Lookup(ip, &result)
		return fmt.Sprint(result.AutonomousSystemNumber), result.AutonomousSystemOrganization
	case "ipinfo generic_asn_free.mmdb":
		var result IPInfo
		_ = reader.Lookup(ip, &result)
		return strings.TrimPrefix(result.ASN, "AS"), result.Name
	default:
		log.Warnln("Unsupported ASN type: %s", reader.Metadata.DatabaseType)
	}
	return "", ""
}
