package inbound

import (
	"net"
	"net/netip"
	"slices"
)

func SetAllowedIPs(prefixes []netip.Prefix) {
	UpdateAccessPolicy(AccessPolicyPatch{AllowedIPs: &prefixes})
}

func SetDisAllowedIPs(prefixes []netip.Prefix) {
	UpdateAccessPolicy(AccessPolicyPatch{DisAllowedIPs: &prefixes})
}

func AllowedIPs() []netip.Prefix {
	return slices.Clone(accessPolicy.Load().AllowedIPs)
}

func DisAllowedIPs() []netip.Prefix {
	return slices.Clone(accessPolicy.Load().DisAllowedIPs)
}

func IsRemoteAddrDisAllowed(addr net.Addr) bool {
	allowed, _ := EvaluateRemoteAddr(addr)
	return allowed
}
