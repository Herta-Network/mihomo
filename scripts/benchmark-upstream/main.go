// Command benchmark-upstream measures a separate mihomo process through its
// standard SOCKS5 wire interface. It uses only the Go standard library.
package main

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sort"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

type result struct {
	SocksAddress                  string  `json:"socks_address"`
	ProtocolAddress               string  `json:"protocol_address,omitempty"`
	UDPBackendAddress             string  `json:"udp_backend_address,omitempty"`
	UDPBackendFirstSource         string  `json:"udp_backend_first_source,omitempty"`
	UDPBackendSelfPackets         int64   `json:"udp_backend_self_packets,omitempty"`
	Path                          string  `json:"path"`
	ServerBinary                  string  `json:"server_binary,omitempty"`
	ServerCPUSeconds              float64 `json:"server_cpu_seconds_including_setup"`
	ServerPeakRSSMiB              float64 `json:"server_peak_rss_mib"`
	SetupRetries                  int     `json:"setup_retries"`
	BackendPacketsIncludingWarmup int64   `json:"backend_packets_including_warmup"`
	ColdBurst                     bool    `json:"cold_burst"`
	Binary                        string  `json:"binary"`
	Mode                          string  `json:"mode"`
	PayloadBytes                  int     `json:"payload_bytes"`
	Connections                   int     `json:"connections"`
	Batch                         int     `json:"batch"`
	RoundsPerConnection           int     `json:"rounds_per_connection"`
	Requests                      int     `json:"requests"`
	Received                      int     `json:"received"`
	Lost                          int     `json:"lost"`
	Invalid                       int     `json:"invalid"`
	WallSeconds                   float64 `json:"wall_seconds"`
	PayloadMBps                   float64 `json:"request_payload_MBps"`
	PacketsPerSecond              float64 `json:"packets_per_second"`
	P50US                         float64 `json:"p50_us"`
	P95US                         float64 `json:"p95_us"`
	P99US                         float64 `json:"p99_us"`
	ProcessCPUSeconds             float64 `json:"process_cpu_seconds_including_setup"`
	ProcessPeakRSSMiB             float64 `json:"process_peak_rss_mib"`
	Error                         string  `json:"error,omitempty"`
}

func main() {
	binaryPath := flag.String("binary", "", "mihomo executable")
	mode := flag.String("mode", "tcp", "tcp or udp")
	size := flag.Int("size", 65536, "payload bytes")
	connections := flag.Int("connections", 1, "simultaneous SOCKS connections")
	batch := flag.Int("batch", 1, "UDP packets per round")
	rounds := flag.Int("rounds", 8192, "rounds per connection")
	coldBurst := flag.Bool("cold-burst", false, "begin UDP warmup with a burst instead of establishing NAT with one packet")
	pathMode := flag.String("path", "direct", "direct or a shared proxy protocol")
	serverBinary := flag.String("server-binary", "", "optional protocol server executable; defaults to -binary")
	flag.Parse()
	if *binaryPath == "" || *size < 8 || *connections < 1 || *batch < 1 || *rounds < 1 {
		panic("invalid options")
	}
	r := result{Path: *pathMode, ServerBinary: *serverBinary, ColdBurst: *coldBurst, Binary: *binaryPath, Mode: *mode, PayloadBytes: *size, Connections: *connections, Batch: *batch, RoundsPerConnection: *rounds}
	if err := measure(&r); err != nil {
		r.Error = err.Error()
	}
	json.NewEncoder(os.Stdout).Encode(r)
	if r.Error != "" {
		os.Exit(1)
	}
}

func writeFull(w io.Writer, p []byte) error {
	for len(p) > 0 {
		n, e := w.Write(p)
		if e != nil {
			return e
		}
		if n == 0 {
			return io.ErrShortWrite
		}
		p = p[n:]
	}
	return nil
}
func socks(proxy string, command byte, target *net.TCPAddr) (net.Conn, *net.UDPAddr, error) {
	c, err := net.DialTimeout("tcp4", proxy, 2*time.Second)
	if err != nil {
		return nil, nil, err
	}
	fail := func(e error) (net.Conn, *net.UDPAddr, error) { c.Close(); return nil, nil, e }
	c.SetDeadline(time.Now().Add(3 * time.Second))
	if err = writeFull(c, []byte{5, 1, 0}); err != nil {
		return fail(err)
	}
	var auth [2]byte
	if _, err = io.ReadFull(c, auth[:]); err != nil {
		return fail(err)
	}
	if auth != [2]byte{5, 0} {
		return fail(fmt.Errorf("SOCKS auth %v", auth))
	}
	request := []byte{5, command, 0, 1, 0, 0, 0, 0, 0, 0}
	if target != nil {
		copy(request[4:8], target.IP.To4())
		binary.BigEndian.PutUint16(request[8:], uint16(target.Port))
	}
	if err = writeFull(c, request); err != nil {
		return fail(err)
	}
	var head [4]byte
	if _, err = io.ReadFull(c, head[:]); err != nil {
		return fail(err)
	}
	if head[1] != 0 {
		return fail(fmt.Errorf("SOCKS status %v", head))
	}
	addressSize := 4
	if head[3] == 4 {
		addressSize = 16
	} else if head[3] != 1 {
		return fail(fmt.Errorf("unexpected SOCKS address type %d", head[3]))
	}
	reply := make([]byte, addressSize+2)
	if _, err = io.ReadFull(c, reply); err != nil {
		return fail(err)
	}
	address := &net.UDPAddr{IP: net.IP(reply[:addressSize]), Port: int(binary.BigEndian.Uint16(reply[addressSize:]))}
	if address.IP.IsUnspecified() {
		address.IP = net.IPv4(127, 0, 0, 1)
	}
	c.SetDeadline(time.Time{})
	return c, address, nil
}

// Listening can precede the initial routing configuration. Retry only an EOF
// during SOCKS setup; never retry application data or a measured operation.
func socksReady(proxy string, command byte, target *net.TCPAddr) (net.Conn, *net.UDPAddr, int, error) {
	deadline := time.Now().Add(2 * time.Second)
	for retries := 0; ; retries++ {
		c, addr, err := socks(proxy, command, target)
		if err == nil {
			return c, addr, retries, nil
		}
		if (!errors.Is(err, io.EOF) && !errors.Is(err, io.ErrUnexpectedEOF)) || time.Now().After(deadline) {
			return nil, nil, retries, err
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func waitProxyReady(proxy string, target *net.TCPAddr) (int, error) {
	deadline := time.Now().Add(3 * time.Second)
	for retries := 0; ; retries++ {
		c, _, err := socks(proxy, 1, target)
		if err == nil {
			c.SetDeadline(time.Now().Add(250 * time.Millisecond))
			payload := []byte("ready")
			err = writeFull(c, payload)
			if err == nil {
				reply := make([]byte, len(payload))
				_, err = io.ReadFull(c, reply)
				if err == nil && !bytes.Equal(reply, payload) {
					err = fmt.Errorf("readiness payload mismatch")
				}
			}
			c.Close()
		}
		if err == nil {
			return retries, nil
		}
		if time.Now().After(deadline) {
			return retries, fmt.Errorf("proxy readiness: %w", err)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

type clientResult struct {
	samples                            []int64
	received, lost, invalid, attempted int
	setupRetries                       int
	err                                error
}

func measure(r *result) error {
	dir, err := os.MkdirTemp("", "mihomo-process-benchmark-")
	if err != nil {
		return err
	}
	defer os.RemoveAll(dir)
	var port int
	for {
		probe, e := net.Listen("tcp4", "127.0.0.1:0")
		if e != nil {
			return e
		}
		candidate := probe.Addr().(*net.TCPAddr).Port
		udp, e := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: candidate})
		probe.Close()
		if e != nil {
			continue
		}
		udp.Close()
		port = candidate
		break
	}
	r.SocksAddress = fmt.Sprintf("127.0.0.1:%d", port)
	config := fmt.Sprintf("socks-port: %d\nallow-lan: false\nbind-address: 127.0.0.1\nmode: direct\nlog-level: error\nipv6: false\ngeo-auto-update: false\ndns:\n  enable: false\nprofile:\n  store-selected: false\n", port)
	if r.Path != "direct" {
		chained, stop, e := startChain(r, dir, port)
		if e != nil {
			return e
		}
		defer stop()
		config = chained
	}
	path := filepath.Join(dir, "config.yaml")
	if err = os.WriteFile(path, []byte(config), 0600); err != nil {
		return err
	}
	logs, err := os.Create(filepath.Join(dir, "proxy.log"))
	if err != nil {
		return err
	}
	defer logs.Close()
	defer func() {
		if os.Getenv("BENCH_DEBUG") == "1" {
			logs.Sync()
			data, _ := os.ReadFile(logs.Name())
			os.Stderr.Write(data)
		}
	}()
	command := exec.Command(r.Binary, "-d", dir, "-f", path)
	command.Env = append(os.Environ(), "GOMAXPROCS=2")
	command.Stdout, command.Stderr = logs, logs
	if err = command.Start(); err != nil {
		return err
	}
	stopped := make(chan error, 1)
	go func() { stopped <- command.Wait() }()
	defer func() {
		command.Process.Signal(os.Interrupt)
		select {
		case <-stopped:
		case <-time.After(3 * time.Second):
			command.Process.Kill()
			<-stopped
		}
		if state := command.ProcessState; state != nil {
			r.ProcessCPUSeconds = state.UserTime().Seconds() + state.SystemTime().Seconds()
			if usage, ok := state.SysUsage().(*syscall.Rusage); ok {
				scale := 1024.0
				if runtime.GOOS == "darwin" {
					scale = 1
				}
				r.ProcessPeakRSSMiB = float64(usage.Maxrss) * scale / (1024 * 1024)
			}
		}
	}()
	proxy := fmt.Sprintf("127.0.0.1:%d", port)
	deadline := time.Now().Add(8 * time.Second)
	for {
		c, e := net.DialTimeout("tcp4", proxy, 50*time.Millisecond)
		if e == nil {
			c.Close()
			break
		}
		if time.Now().After(deadline) {
			logs.Sync()
			data, _ := os.ReadFile(logs.Name())
			return fmt.Errorf("proxy startup: %s", data)
		}
		time.Sleep(10 * time.Millisecond)
	}
	var tcpListener net.Listener
	var udpListener *net.UDPConn
	var target *net.TCPAddr
	var backendPackets atomic.Int64
	var backendSelfPackets atomic.Int64
	var firstBackendSource atomic.Pointer[net.UDPAddr]
	defer func() {
		r.BackendPacketsIncludingWarmup = backendPackets.Load()
		r.UDPBackendSelfPackets = backendSelfPackets.Load()
		if source := firstBackendSource.Load(); source != nil {
			r.UDPBackendFirstSource = source.String()
		}
	}()
	if r.Mode == "tcp" {
		tcpListener, err = net.Listen("tcp4", "127.0.0.1:0")
		if err != nil {
			return err
		}
		defer tcpListener.Close()
		target = tcpListener.Addr().(*net.TCPAddr)
		go func() {
			for {
				c, e := tcpListener.Accept()
				if e != nil {
					return
				}
				go func() { defer c.Close(); io.Copy(c, c) }()
			}
		}()
	} else {
		udpListener, err = net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
		if err != nil {
			return err
		}
		defer udpListener.Close()
		addr := udpListener.LocalAddr().(*net.UDPAddr)
		r.UDPBackendAddress = addr.String()
		target = &net.TCPAddr{IP: addr.IP, Port: addr.Port}
		udpListener.SetReadBuffer(4 << 20)
		udpListener.SetWriteBuffer(4 << 20)
		go func() {
			data := make([]byte, 65535)
			first := true
			for {
				n, a, e := udpListener.ReadFromUDP(data)
				if e != nil {
					return
				}
				if first {
					firstBackendSource.Store(a)
					first = false
				}
				if a.Port == addr.Port && a.IP.Equal(addr.IP) {
					backendSelfPackets.Add(1)
				}
				backendPackets.Add(1)
				udpListener.WriteToUDP(data[:n], a)
			}
		}()
	}
	// A bound listener is not yet evidence that the initial routing config has
	// been applied. Check one complete TCP round trip before warming clients.
	readyTarget := target
	if r.Mode != "tcp" {
		readyListener, e := net.Listen("tcp4", "127.0.0.1:0")
		if e != nil {
			return e
		}
		defer readyListener.Close()
		readyTarget = readyListener.Addr().(*net.TCPAddr)
		go func() {
			for {
				c, e := readyListener.Accept()
				if e != nil {
					return
				}
				go func() { defer c.Close(); io.Copy(c, c) }()
			}
		}()
	}
	retries, e := waitProxyReady(proxy, readyTarget)
	if e != nil {
		return e
	}
	r.SetupRetries += retries
	const warm = 10
	start := make(chan struct{})
	ready := make(chan error, r.Connections)
	results := make(chan clientResult, r.Connections)
	var wg sync.WaitGroup
	for i := 0; i < r.Connections; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if r.Mode == "tcp" {
				runTCP(proxy, target, r, warm, ready, start, results)
			} else {
				runUDP(proxy, target, r, warm, ready, start, results)
			}
		}()
	}
	for i := 0; i < r.Connections; i++ {
		if e := <-ready; e != nil {
			close(start)
			wg.Wait()
			return e
		}
	}
	started := time.Now()
	close(start)
	wg.Wait()
	r.WallSeconds = time.Since(started).Seconds()
	var samples []int64
	for i := 0; i < r.Connections; i++ {
		part := <-results
		r.SetupRetries += part.setupRetries
		r.Requests += part.attempted
		r.Received += part.received
		r.Lost += part.lost
		r.Invalid += part.invalid
		samples = append(samples, part.samples...)
		if part.err != nil {
			r.Error = part.err.Error()
		}
	}
	r.PayloadMBps = float64(r.Received*r.PayloadBytes) / r.WallSeconds / 1e6
	r.PacketsPerSecond = float64(r.Received) / r.WallSeconds
	sort.Slice(samples, func(i, j int) bool { return samples[i] < samples[j] })
	if len(samples) > 0 {
		r.P50US = float64(samples[(len(samples)-1)*50/100]) / 1000
		r.P95US = float64(samples[(len(samples)-1)*95/100]) / 1000
		r.P99US = float64(samples[(len(samples)-1)*99/100]) / 1000
	}
	return nil
}

func runTCP(proxy string, target *net.TCPAddr, r *result, warm int, ready chan<- error, start <-chan struct{}, results chan<- clientResult) {
	part := clientResult{samples: make([]int64, 0, r.RoundsPerConnection)}
	defer func() { results <- part }()
	c, _, retries, err := socksReady(proxy, 1, target)
	part.setupRetries = retries
	if err != nil {
		ready <- err
		part.err = err
		return
	}
	defer c.Close()
	c.SetDeadline(time.Now().Add(120 * time.Second))
	payload := bytes.Repeat([]byte{0x5a}, r.PayloadBytes)
	ack := make(chan error, 1)
	done := make(chan struct{})
	defer close(done)
	go func() {
		reply := make([]byte, len(payload))
		for i := 0; i < warm+r.RoundsPerConnection; i++ {
			_, e := io.ReadFull(c, reply)
			if e == nil && !bytes.Equal(reply, payload) {
				e = fmt.Errorf("TCP corrupt payload")
			}
			select {
			case ack <- e:
			case <-done:
				return
			}
			if e != nil {
				return
			}
		}
	}()
	round := func() error {
		if e := writeFull(c, payload); e != nil {
			return e
		}
		return <-ack
	}
	for i := 0; i < warm; i++ {
		if err = round(); err != nil {
			ready <- err
			part.err = err
			return
		}
	}
	ready <- nil
	<-start
	for i := 0; i < r.RoundsPerConnection; i++ {
		now := time.Now()
		part.attempted++
		if err = round(); err != nil {
			part.lost++
			part.err = err
			return
		}
		part.samples = append(part.samples, time.Since(now).Nanoseconds())
		part.received++
	}
}
func runUDP(proxy string, target *net.TCPAddr, r *result, warm int, ready chan<- error, start <-chan struct{}, results chan<- clientResult) {
	part := clientResult{samples: make([]int64, 0, r.RoundsPerConnection)}
	defer func() { results <- part }()
	control, relay, retries, err := socksReady(proxy, 3, nil)
	part.setupRetries = retries
	if err != nil {
		ready <- err
		part.err = err
		return
	}
	defer control.Close()
	c, err := net.DialUDP("udp4", nil, relay)
	if err != nil {
		ready <- err
		part.err = err
		return
	}
	defer c.Close()
	c.SetReadBuffer(4 << 20)
	frame := make([]byte, 10+r.PayloadBytes)
	frame[3] = 1
	copy(frame[4:8], target.IP.To4())
	binary.BigEndian.PutUint16(frame[8:], uint16(target.Port))
	for i := 18; i < len(frame); i++ {
		frame[i] = 0x5a
	}
	reply := make([]byte, 65535)
	sequence := uint64(0)
	round := func(count int) (received, lost, invalid int, e error) {
		first := sequence
		sequence += uint64(count)
		for i := 0; i < count; i++ {
			binary.BigEndian.PutUint64(frame[10:], first+uint64(i))
			if _, e = c.Write(frame); e != nil {
				return
			}
		}
		c.SetReadDeadline(time.Now().Add(200 * time.Millisecond))
		seen := make([]bool, count)
		for received < count {
			var n int
			n, e = c.Read(reply)
			if e != nil {
				if timeout, ok := e.(net.Error); ok && timeout.Timeout() {
					lost = count - received
					e = nil
				}
				return
			}
			if n != len(frame) || reply[3] != 1 || !bytes.Equal(reply[4:10], frame[4:10]) || !bytes.Equal(reply[18:n], frame[18:]) {
				invalid++
				continue
			}
			id := binary.BigEndian.Uint64(reply[10:])
			if id < first || id >= first+uint64(count) || seen[id-first] {
				invalid++
				continue
			}
			seen[id-first] = true
			received++
		}
		return
	}
	for i := 0; i < warm; i++ {
		count := r.Batch
		if i == 0 && !r.ColdBurst {
			count = 1
		}
		_, lost, invalid, e := round(count)
		if e != nil || lost != 0 || invalid != 0 {
			ready <- fmt.Errorf("UDP warmup: lost=%d invalid=%d err=%v", lost, invalid, e)
			return
		}
	}
	ready <- nil
	<-start
	for i := 0; i < r.RoundsPerConnection; i++ {
		now := time.Now()
		part.attempted += r.Batch
		received, lost, invalid, e := round(r.Batch)
		part.received += received
		part.lost += lost
		part.invalid += invalid
		part.samples = append(part.samples, time.Since(now).Nanoseconds())
		if e != nil {
			part.err = e
			return
		}
		if lost > 0 || invalid > 0 {
			part.err = fmt.Errorf("UDP loss/corruption: lost=%d invalid=%d", lost, invalid)
			return
		}
	}
}
