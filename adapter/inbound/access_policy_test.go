package inbound

import (
	"net"
	"net/netip"
	"sync"
	"testing"
)

func restoreAccessPolicy(t *testing.T) {
	t.Helper()
	allowed := append([]netip.Prefix(nil), AllowedIPs()...)
	denied := append([]netip.Prefix(nil), DisAllowedIPs()...)
	skip := append([]netip.Prefix(nil), SkipAuthPrefixes()...)
	t.Cleanup(func() { SetAllowedIPs(allowed); SetDisAllowedIPs(denied); SetSkipAuthPrefixes(skip) })
}

func TestAccessPolicyOwnsConfigurationSlices(t *testing.T) {
	restoreAccessPolicy(t)
	for _, field := range []struct {
		name string
		set  func([]netip.Prefix)
		get  func() []netip.Prefix
	}{
		{"allowed", SetAllowedIPs, AllowedIPs},
		{"denied", SetDisAllowedIPs, DisAllowedIPs},
		{"skip-auth", SetSkipAuthPrefixes, SkipAuthPrefixes},
	} {
		t.Run(field.name, func(t *testing.T) {
			want := netip.MustParsePrefix("127.0.0.0/8")
			input := []netip.Prefix{want}
			field.set(input)
			input[0] = netip.MustParsePrefix("10.0.0.0/8")
			if field.get()[0] != want {
				t.Fatal("setter retained caller-owned mutable storage")
			}
			output := field.get()
			output[0] = netip.MustParsePrefix("192.168.0.0/16")
			if field.get()[0] != want {
				t.Fatal("getter exposed live policy storage")
			}
		})
	}
}

func TestAccessPolicyConcurrentReloadAndConnections(t *testing.T) {
	restoreAccessPolicy(t)
	prefixes := []netip.Prefix{netip.MustParsePrefix("127.0.0.0/8")}
	address := &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 12345}
	start := make(chan struct{})
	var workers sync.WaitGroup
	workers.Add(4)
	go func() {
		defer workers.Done()
		<-start
		for i := 0; i < 2000; i++ {
			SetAllowedIPs(prefixes)
			SetAllowedIPs(nil)
		}
	}()
	go func() {
		defer workers.Done()
		<-start
		for i := 0; i < 2000; i++ {
			SetDisAllowedIPs(prefixes)
			SetDisAllowedIPs(nil)
		}
	}()
	go func() {
		defer workers.Done()
		<-start
		for i := 0; i < 2000; i++ {
			SetSkipAuthPrefixes(prefixes)
			SetSkipAuthPrefixes(nil)
		}
	}()
	go func() {
		defer workers.Done()
		<-start
		for i := 0; i < 6000; i++ {
			_ = IsRemoteAddrDisAllowed(address)
			_ = SkipAuthRemoteAddr(address)
			_ = AllowedIPs()
			_ = DisAllowedIPs()
			_ = SkipAuthPrefixes()
		}
	}()
	close(start)
	workers.Wait()
}

func TestAccessPolicyAdmissionUsesOneGeneration(t *testing.T) {
	restoreAccessPolicy(t)
	prefix := []netip.Prefix{netip.MustParsePrefix("127.0.0.0/8")}
	a := AccessPolicy{AllowedIPs: prefix}
	b := AccessPolicy{AllowedIPs: prefix, DisAllowedIPs: prefix, SkipAuthPrefixes: prefix}
	SetAccessPolicy(a)
	address := &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 12345}
	start := make(chan struct{})
	var workers sync.WaitGroup
	workers.Add(2)
	go func() {
		defer workers.Done()
		<-start
		for i := 0; i < 10000; i++ {
			SetAccessPolicy(a)
			SetAccessPolicy(b)
		}
	}()
	go func() {
		defer workers.Done()
		<-start
		for i := 0; i < 20000; i++ {
			allowed, skip := EvaluateRemoteAddr(address)
			if allowed == skip {
				t.Errorf("mixed policy generations: allowed=%v skip=%v", allowed, skip)
				return
			}
		}
	}()
	close(start)
	workers.Wait()
}

func TestAccessPolicyConcurrentPatchesPreserveOtherFields(t *testing.T) {
	restoreAccessPolicy(t)
	SetAccessPolicy(AccessPolicy{})
	allowed := []netip.Prefix{netip.MustParsePrefix("0.0.0.0/0")}
	denied := []netip.Prefix{netip.MustParsePrefix("10.0.0.0/8")}
	start := make(chan struct{})
	var workers sync.WaitGroup
	workers.Add(2)
	go func() { defer workers.Done(); <-start; UpdateAccessPolicy(AccessPolicyPatch{AllowedIPs: &allowed}) }()
	go func() { defer workers.Done(); <-start; UpdateAccessPolicy(AccessPolicyPatch{DisAllowedIPs: &denied}) }()
	close(start)
	workers.Wait()
	policy := GetAccessPolicy()
	if len(policy.AllowedIPs) != 1 || len(policy.DisAllowedIPs) != 1 {
		t.Fatal("partial update lost an unrelated policy field")
	}
}
