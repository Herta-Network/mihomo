package net

import (
	"bytes"
	"io"
	"net"
	"runtime"
	"sync/atomic"
	"testing"
	"time"

	"github.com/metacubex/mihomo/constant/features"
	"github.com/metacubex/sing/common/buf"
	"github.com/metacubex/sing/common/bufio"
	"github.com/metacubex/sing/common/network"
)

func tcpPair(t *testing.T) (*net.TCPConn, *net.TCPConn) {
	t.Helper()
	listener, err := net.ListenTCP("tcp4", &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	client, err := net.DialTCP("tcp4", nil, listener.Addr().(*net.TCPAddr))
	if err != nil {
		t.Fatal(err)
	}
	server, err := listener.AcceptTCP()
	if err != nil {
		client.Close()
		t.Fatal(err)
	}
	for _, c := range []*net.TCPConn{client, server} {
		c.SetDeadline(time.Now().Add(5 * time.Second))
		t.Cleanup(func() { c.Close() })
	}
	return client, server
}

type countedRelayConn struct {
	*net.TCPConn
	read, written atomic.Int64
}

func (c *countedRelayConn) UnwrapReader() (io.Reader, []network.CountFunc) {
	return c.TCPConn, []network.CountFunc{func(n int64) { c.read.Add(n) }}
}
func (c *countedRelayConn) UnwrapWriter() (io.Writer, []network.CountFunc) {
	return c.TCPConn, []network.CountFunc{func(n int64) { c.written.Add(n) }}
}

// This wrapper transforms payloads and deliberately cannot be unwrapped.
type xorRelayConn struct{ net.Conn }

func (c xorRelayConn) Read(p []byte) (int, error) {
	n, err := c.Conn.Read(p)
	for i := 0; i < n; i++ {
		p[i] ^= 0x5a
	}
	return n, err
}
func (c xorRelayConn) Write(p []byte) (int, error) {
	out := bytes.Clone(p)
	for i := range out {
		out[i] ^= 0x5a
	}
	return c.Conn.Write(out)
}
func (c xorRelayConn) CloseWrite() error {
	return c.Conn.(interface{ CloseWrite() error }).CloseWrite()
}

func TestRelayHalfCloseCountersAndProtocolWrapper(t *testing.T) {
	for _, wrapped := range []bool{false, true} {
		name := "raw"
		if wrapped {
			name = "protocol"
		}
		t.Run(name, func(t *testing.T) {
			client, left := tcpPair(t)
			right, server := tcpPair(t)
			counted := &countedRelayConn{TCPConn: right}
			var incoming net.Conn = left
			if wrapped {
				incoming = xorRelayConn{left}
			}
			options := relayCopyOptions(counted, incoming)
			if !features.WithLowMemory && !wrapped {
				if options.ReadPolicy != nil || options.TransferSize != nil {
					t.Fatal("normal relay replaced native copy policy")
				}
			} else if features.WithLowMemory && runtime.GOOS == "linux" && !wrapped {
				if options.ReadPolicy != nil || options.TransferSize == nil || options.TransferSize() != 16*1024 {
					t.Fatal("low-memory splice limit lost")
				}
			} else {
				if options.ReadPolicy == nil {
					t.Fatal("low-memory read limit lost")
				}
				policy := options.ReadPolicy()
				expectedSize := 32 * 1024
				if features.WithLowMemory {
					expectedSize = 16 * 1024
				}
				if policy.BatchSize != 1 || policy.MaxBufferSize != expectedSize {
					t.Fatal("low-memory read policy changed")
				}
			}
			done := make(chan struct{})
			go func() { Relay(incoming, counted); close(done) }()
			request := bytes.Repeat([]byte("request"), 12000)
			response := bytes.Repeat([]byte("response"), 12000)
			serverErr := make(chan error, 1)
			go func() {
				got, err := io.ReadAll(server)
				if err == nil && !bytes.Equal(got, request) {
					err = io.ErrUnexpectedEOF
				}
				if err == nil {
					_, err = server.Write(response)
				}
				server.CloseWrite()
				serverErr <- err
			}()
			wire := bytes.Clone(request)
			if wrapped {
				for i := range wire {
					wire[i] ^= 0x5a
				}
			}
			if _, err := client.Write(wire); err != nil {
				t.Fatal(err)
			}
			client.CloseWrite()
			got, err := io.ReadAll(client)
			if err != nil {
				t.Fatal(err)
			}
			if wrapped {
				for i := range got {
					got[i] ^= 0x5a
				}
			}
			if !bytes.Equal(got, response) {
				t.Fatalf("response length %d", len(got))
			}
			if err := <-serverErr; err != nil {
				t.Fatal(err)
			}
			select {
			case <-done:
			case <-time.After(5 * time.Second):
				t.Fatal("relay did not close")
			}
			if counted.written.Load() != int64(len(request)) || counted.read.Load() != int64(len(response)) {
				t.Fatalf("incorrect counters: read=%d written=%d", counted.read.Load(), counted.written.Load())
			}
		})
	}
}

func TestRelayPreservesCachedHandshakePayload(t *testing.T) {
	client, left := tcpPair(t)
	right, server := tcpPair(t)
	payload := bytes.Repeat([]byte("cached-handshake-payload"), 1000)
	prefix := buf.NewSize(4096)
	prefix.Write(payload[:4096])
	incoming := bufio.NewCachedConn(left, prefix)
	counted := &countedRelayConn{TCPConn: right}
	done := make(chan struct{})
	go func() { Relay(incoming, counted); close(done) }()
	response := make(chan error, 1)
	go func() {
		got, err := io.ReadAll(server)
		if err == nil && !bytes.Equal(got, payload) {
			err = io.ErrUnexpectedEOF
		}
		if err == nil {
			_, err = server.Write([]byte("ack"))
		}
		server.CloseWrite()
		response <- err
	}()
	if _, err := client.Write(payload[4096:]); err != nil {
		t.Fatal(err)
	}
	client.CloseWrite()
	got, err := io.ReadAll(client)
	if err != nil || string(got) != "ack" {
		t.Fatalf("reply=%q err=%v", got, err)
	}
	if err = <-response; err != nil {
		t.Fatal(err)
	}
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("cached relay did not close")
	}
	if prefix.RawCap() != 0 || counted.written.Load() != int64(len(payload)) || counted.read.Load() != 3 {
		t.Fatal("cached data ownership or traffic count changed")
	}
}

type shortRelayWriter struct{}

func (shortRelayWriter) Write(p []byte) (int, error) { return 1, nil }
func TestRelayOpaqueWriterShortSuccessIsRejected(t *testing.T) {
	n, err := relayCopy(shortRelayWriter{}, bytes.NewReader([]byte("payload")))
	if n != 1 || err != io.ErrShortWrite {
		t.Fatalf("progress=%d err=%v", n, err)
	}
}
