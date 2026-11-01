package inbound

import (
	"net"
	"net/netip"
	"slices"
	"sync"
	"sync/atomic"

	C "github.com/metacubex/mihomo/constant"
)

// AccessPolicy is published as one immutable policy for new connections.
// Setters and getters copy the slices; callers never own the live snapshot.
type AccessPolicy struct {
	AllowedIPs       []netip.Prefix
	DisAllowedIPs    []netip.Prefix
	SkipAuthPrefixes []netip.Prefix
}

// AccessPolicyPatch changes only non-nil fields, including explicitly empty lists.
type AccessPolicyPatch struct {
	AllowedIPs       *[]netip.Prefix
	DisAllowedIPs    *[]netip.Prefix
	SkipAuthPrefixes *[]netip.Prefix
}

var accessPolicyWrites sync.Mutex
var accessPolicy = func() *atomic.Pointer[AccessPolicy] {
	value := new(atomic.Pointer[AccessPolicy])
	value.Store(&AccessPolicy{})
	return value
}()

func GetAccessPolicy() AccessPolicy {
	current := accessPolicy.Load()
	return AccessPolicy{
		AllowedIPs:       slices.Clone(current.AllowedIPs),
		DisAllowedIPs:    slices.Clone(current.DisAllowedIPs),
		SkipAuthPrefixes: slices.Clone(current.SkipAuthPrefixes),
	}
}

func SetAccessPolicy(policy AccessPolicy) {
	UpdateAccessPolicy(AccessPolicyPatch{AllowedIPs: &policy.AllowedIPs, DisAllowedIPs: &policy.DisAllowedIPs, SkipAuthPrefixes: &policy.SkipAuthPrefixes})
}

func UpdateAccessPolicy(patch AccessPolicyPatch) {
	if patch.AllowedIPs == nil && patch.DisAllowedIPs == nil && patch.SkipAuthPrefixes == nil {
		return
	}
	accessPolicyWrites.Lock()
	defer accessPolicyWrites.Unlock()
	next := *accessPolicy.Load()
	if patch.AllowedIPs != nil {
		next.AllowedIPs = slices.Clone(*patch.AllowedIPs)
	}
	if patch.DisAllowedIPs != nil {
		next.DisAllowedIPs = slices.Clone(*patch.DisAllowedIPs)
	}
	if patch.SkipAuthPrefixes != nil {
		next.SkipAuthPrefixes = slices.Clone(*patch.SkipAuthPrefixes)
	}
	accessPolicy.Store(&next)
}

// EvaluateRemoteAddr makes admission and authentication-bypass decisions from
// the same policy version, even when a reload publishes another snapshot.
func EvaluateRemoteAddr(addr net.Addr) (allowed, skipAuthentication bool) {
	policy := accessPolicy.Load()
	metadata := C.Metadata{}
	if err := metadata.SetRemoteAddr(addr); err != nil {
		return false, false
	}
	ip := metadata.AddrPort().Addr()
	if !ip.IsValid() {
		return false, false
	}
	return prefixesContains(policy.AllowedIPs, ip) && !prefixesContains(policy.DisAllowedIPs, ip), prefixesContains(policy.SkipAuthPrefixes, ip)
}
