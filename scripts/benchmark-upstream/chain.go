package main

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"math/big"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"syscall"
	"time"
)

func chainPort() (int, error) {
	for {
		tcp, e := net.Listen("tcp4", "127.0.0.1:0")
		if e != nil {
			return 0, e
		}
		port := tcp.Addr().(*net.TCPAddr).Port
		udp, e := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: port})
		tcp.Close()
		if e != nil {
			continue
		}
		udp.Close()
		return port, nil
	}
}
func chainBase() map[string]any {
	return map[string]any{
		"allow-lan": false, "bind-address": "127.0.0.1", "mode": "direct", "log-level": "error", "ipv6": false, "geo-auto-update": false,
		"dns": map[string]any{"enable": false}, "profile": map[string]any{"store-selected": false},
	}
}
func chainCertificate(dir string) (string, string, error) {
	key, e := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if e != nil {
		return "", "", e
	}
	cert := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "loopback-benchmark"}, NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour), DNSNames: []string{"localhost"}, IPAddresses: []net.IP{net.IPv4(127, 0, 0, 1)}, KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}}
	der, e := x509.CreateCertificate(rand.Reader, cert, cert, &key.PublicKey, key)
	if e != nil {
		return "", "", e
	}
	encoded, e := x509.MarshalPKCS8PrivateKey(key)
	if e != nil {
		return "", "", e
	}
	certPath, keyPath := filepath.Join(dir, "cert.pem"), filepath.Join(dir, "key.pem")
	if e = os.WriteFile(certPath, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), 0600); e != nil {
		return "", "", e
	}
	e = os.WriteFile(keyPath, pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: encoded}), 0600)
	return certPath, keyPath, e
}

// Both proxy processes use ordinary configuration and routing. The final echo
// backend remains in the driver; CPU/RSS are recorded separately for each proxy.
func startChain(r *result, dir string, socksPort int) (string, func(), error) {
	const uuid = "00000000-0000-0000-0000-000000000001"
	const password = "benchmark-password"
	serverDir := filepath.Join(dir, "server")
	if e := os.Mkdir(serverDir, 0700); e != nil {
		return "", nil, e
	}
	port, e := chainPort()
	if e != nil {
		return "", nil, e
	}
	r.ProtocolAddress = net.JoinHostPort("127.0.0.1", strconv.Itoa(port))
	listener := map[string]any{"name": "benchmark-server", "listen": "127.0.0.1", "port": port}
	proxy := map[string]any{"name": "benchmark-proxy", "server": "127.0.0.1", "port": port, "udp": true}
	switch {
	case r.Path == "vmess" || r.Path == "vmess-xudp":
		listener["type"] = "vmess"
		listener["users"] = []any{map[string]any{"username": "bench", "uuid": uuid}}
		proxy["type"] = "vmess"
		proxy["uuid"] = uuid
		proxy["alterId"] = 0
		proxy["cipher"] = "aes-128-gcm"
		proxy["xudp"] = r.Path == "vmess-xudp"
	case r.Path == "vless" || strings.HasPrefix(r.Path, "mux-"):
		listener["type"] = "vless"
		listener["allow-insecure"] = true
		listener["users"] = []any{map[string]any{"username": "bench", "uuid": uuid}}
		proxy["type"] = "vless"
		proxy["uuid"] = uuid
		if strings.HasPrefix(r.Path, "mux-") {
			proxy["smux"] = map[string]any{"enabled": true, "protocol": strings.TrimPrefix(r.Path, "mux-"), "padding": false}
		}
	case r.Path == "ss-udp" || r.Path == "ss-uot":
		listener["type"] = "shadowsocks"
		listener["cipher"] = "aes-128-gcm"
		listener["password"] = password
		listener["udp"] = true
		proxy["type"] = "ss"
		proxy["cipher"] = "aes-128-gcm"
		proxy["password"] = password
		proxy["udp-over-tcp"] = r.Path == "ss-uot"
		proxy["udp-over-tcp-version"] = 2
	case strings.HasPrefix(r.Path, "snell"):
		parts := strings.SplitN(strings.TrimPrefix(r.Path, "snell"), "-", 2)
		version, parseErr := strconv.Atoi(parts[0])
		if parseErr != nil || version < 3 || version > 6 {
			return "", nil, fmt.Errorf("invalid Snell path %q", r.Path)
		}
		if len(parts) > 1 {
			listener["version6-mode"] = parts[1]
			proxy["version6-mode"] = parts[1]
		}
		listener["type"] = "snell"
		listener["version"] = version
		listener["psk"] = "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA="
		listener["udp"] = true
		proxy["type"] = "snell"
		proxy["version"] = version
		proxy["psk"] = listener["psk"]
	case r.Path == "anytls-uot" || r.Path == "hysteria2" || r.Path == "tuic4" || r.Path == "tuic5":
		cert, key, e := chainCertificate(serverDir)
		if e != nil {
			return "", nil, e
		}
		listener["certificate"] = cert
		listener["private-key"] = key
		proxy["skip-cert-verify"] = true
		if strings.HasPrefix(r.Path, "tuic") {
			listener["type"] = "tuic"
			listener["token"] = []string{password}
			listener["users"] = map[string]string{uuid: password}
			listener["congestion-controller"] = "cubic"
			proxy["type"] = "tuic"
			proxy["congestion-controller"] = "cubic"
			if r.Path == "tuic4" {
				proxy["token"] = password
			} else {
				proxy["uuid"] = uuid
				proxy["password"] = password
			}
		} else {
			kind := r.Path
			if kind == "anytls-uot" {
				kind = "anytls"
			}
			listener["type"] = kind
			listener["users"] = map[string]string{"bench": password}
			proxy["type"] = kind
			proxy["password"] = password
		}
	default:
		return "", nil, fmt.Errorf("unknown path %q", r.Path)
	}
	config := chainBase()
	config["listeners"] = []any{listener}
	data, e := json.Marshal(config)
	if e != nil {
		return "", nil, e
	}
	file := filepath.Join(serverDir, "config.json")
	if e = os.WriteFile(file, data, 0600); e != nil {
		return "", nil, e
	}
	logs, e := os.Create(filepath.Join(serverDir, "server.log"))
	if e != nil {
		return "", nil, e
	}
	executable := r.ServerBinary
	if executable == "" {
		executable = r.Binary
	}
	r.ServerBinary = executable
	cmd := exec.Command(executable, "-d", serverDir, "-f", file)
	cmd.Env = append(os.Environ(), "GOMAXPROCS=2")
	cmd.Stdout, cmd.Stderr = logs, logs
	if e = cmd.Start(); e != nil {
		logs.Close()
		return "", nil, e
	}
	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()
	stop := func() {
		cmd.Process.Signal(os.Interrupt)
		select {
		case <-done:
		case <-time.After(3 * time.Second):
			cmd.Process.Kill()
			<-done
		}
		if state := cmd.ProcessState; state != nil {
			r.ServerCPUSeconds = state.UserTime().Seconds() + state.SystemTime().Seconds()
			if usage, ok := state.SysUsage().(*syscall.Rusage); ok {
				scale := 1024.0
				if runtime.GOOS == "darwin" {
					scale = 1
				}
				r.ServerPeakRSSMiB = float64(usage.Maxrss) * scale / (1024 * 1024)
			}
		}
		if os.Getenv("BENCH_DEBUG") == "1" {
			logs.Sync()
			data, _ := os.ReadFile(logs.Name())
			os.Stderr.Write(data)
		}
		logs.Close()
	}
	client := chainBase()
	client["socks-port"] = socksPort
	client["mode"] = "rule"
	client["proxies"] = []any{proxy}
	client["rules"] = []string{"MATCH,benchmark-proxy"}
	data, e = json.Marshal(client)
	if e != nil {
		stop()
		return "", nil, e
	}
	return string(data), stop, nil
}
