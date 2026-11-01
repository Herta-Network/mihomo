package inbound

import (
	"net"
	"net/netip"
	"slices"

	C "github.com/metacubex/mihomo/constant"
)

func SetSkipAuthPrefixes(prefixes []netip.Prefix) {
	UpdateAccessPolicy(AccessPolicyPatch{SkipAuthPrefixes: &prefixes})
}

func SkipAuthPrefixes() []netip.Prefix {
	return slices.Clone(accessPolicy.Load().SkipAuthPrefixes)
}

func SkipAuthRemoteAddr(addr net.Addr) bool {
	m := C.Metadata{}
	if err := m.SetRemoteAddr(addr); err != nil {
		return false
	}
	return skipAuth(m.AddrPort().Addr())
}

func SkipAuthRemoteAddress(addr string) bool {
	m := C.Metadata{}
	if err := m.SetRemoteAddress(addr); err != nil {
		return false
	}
	return skipAuth(m.AddrPort().Addr())
}

func skipAuth(addr netip.Addr) bool {
	return prefixesContains(accessPolicy.Load().SkipAuthPrefixes, addr)
}
