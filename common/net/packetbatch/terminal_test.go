package packetbatch

import (
	"errors"
	"io"
	"net"
	"sync"
	"testing"
	"time"

	P "github.com/metacubex/mihomo/common/net/packet"
)

type terminalTestConn struct {
	net.PacketConn
	n, calls, closes int
	err              error
	data             []byte
	released         int
}

func (c *terminalTestConn) WriteTo([]byte, net.Addr) (int, error) {
	c.calls++
	return c.n, c.err
}
func (c *terminalTestConn) Close() error { c.closes++; return nil }
func (c *terminalTestConn) WaitReadFrom() ([]byte, func(), net.Addr, error) {
	return c.data, func() { c.released++ }, target, nil
}

func TestPacketTerminalWriteFailureAndProgress(t *testing.T) {
	for _, tc := range []struct {
		n       int
		err     error
		wantN   int
		wantErr error
	}{
		{2, nil, 0, io.ErrShortWrite},
		{2, io.ErrClosedPipe, 0, io.ErrClosedPipe},
		{4, io.ErrClosedPipe, 4, io.ErrClosedPipe},
		{-1, nil, 0, io.ErrShortWrite},
		{5, nil, 0, io.ErrShortWrite},
	} {
		raw := &terminalTestConn{n: tc.n, err: tc.err}
		conn := NewTerminalConn(raw)
		if _, ok := P.CreatePacketBatchWriterWithCount(conn); ok {
			t.Fatal("scalar error guard advertised a batch capability")
		}
		if n, err := conn.WriteTo([]byte("test"), target); n != tc.wantN || !errors.Is(err, tc.wantErr) {
			t.Fatalf("n=%d err=%v", n, err)
		}
		if _, err := conn.WriteTo([]byte("later"), target); !errors.Is(err, tc.wantErr) || raw.calls != 1 {
			t.Fatal("failed packet stream was reused")
		}
		conn.Close()
		if raw.closes != 1 {
			t.Fatal("transport was not closed exactly once")
		}
	}
}

func TestPacketTerminalEmptyAndReadOwnership(t *testing.T) {
	raw := &terminalTestConn{data: []byte("borrowed")}
	conn := NewTerminalConn(raw)
	if n, err := conn.WriteTo(nil, target); n != 0 || err != nil {
		t.Fatalf("empty datagram: %d %v", n, err)
	}
	data, release, _, err := conn.WaitReadFrom()
	if err != nil || &data[0] != &raw.data[0] {
		t.Fatal("original wait reader was replaced")
	}
	conn.Close()
	if raw.released != 0 || string(data) != "borrowed" {
		t.Fatal("close released a caller-owned packet")
	}
	release()
	if raw.released != 1 {
		t.Fatal("packet was not released once")
	}
	if _, err = conn.WriteTo(nil, target); !errors.Is(err, net.ErrClosed) || raw.calls != 1 {
		t.Fatal("write after close reached the transport")
	}
}

type blockedTerminalConn struct {
	net.PacketConn
	started chan struct{}
	closed  chan struct{}
	once    sync.Once
}

func (c *blockedTerminalConn) WriteTo([]byte, net.Addr) (int, error) {
	close(c.started)
	<-c.closed
	return 0, net.ErrClosed
}
func (c *blockedTerminalConn) Close() error { c.once.Do(func() { close(c.closed) }); return nil }

func TestPacketTerminalCloseInterruptsWrite(t *testing.T) {
	raw := &blockedTerminalConn{started: make(chan struct{}), closed: make(chan struct{})}
	conn := NewTerminalConn(raw)
	done := make(chan error, 1)
	go func() { _, err := conn.WriteTo([]byte("blocked"), target); done <- err }()
	<-raw.started
	closed := make(chan struct{})
	go func() { conn.Close(); close(closed) }()
	for _, ch := range []<-chan struct{}{closed, raw.closed} {
		select {
		case <-ch:
		case <-time.After(time.Second):
			t.Fatal("close waited for the writer lock")
		}
	}
	select {
	case err := <-done:
		if !errors.Is(err, net.ErrClosed) {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("failed writer did not return")
	}
}
