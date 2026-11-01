package vmess

import (
	"errors"
	"io"
	"net"
	"testing"

	"github.com/gobwas/ws"
)

type partialWire struct {
	net.Conn
	remaining    int
	failure      error
	closed       bool
	last         []byte
	payloadLimit *int
}

func (c *partialWire) Write(p []byte) (int, error) {
	c.last = append([]byte(nil), p...)
	if c.payloadLimit != nil {
		return len(p) - 10 + *c.payloadLimit, c.failure
	}
	n := min(len(p), max(0, c.remaining))
	c.remaining -= n
	if n != len(p) || c.remaining == 0 {
		return n, c.failure
	}
	return n, nil
}
func (c *partialWire) Close() error { c.closed = true; return nil }
func TestPacketWebSocketWriteProgress(t *testing.T) {
	failure := errors.New("wire failure")
	for _, state := range []ws.State{ws.StateClientSide, ws.StateServerSide} {
		for _, size := range []int{10, 200, 70000} {
			header := ws.HeaderSize(ws.Header{Length: int64(size), Masked: state.ClientSide()})
			for _, prefix := range []int{-1, 0, 3, size} {
				raw := &partialWire{remaining: header + prefix, failure: failure}
				conn := &websocketConn{Conn: raw, state: state}
				n, err := conn.Write(make([]byte, size))
				if n != max(0, prefix) || !errors.Is(err, failure) {
					t.Fatalf("size=%d prefix=%d n=%d err=%v", size, prefix, n, err)
				}
			}
			raw := &partialWire{remaining: header + 3}
			conn := &websocketConn{Conn: raw, state: state}
			n, err := conn.Write(make([]byte, size))
			if n != 3 || !errors.Is(err, io.ErrShortWrite) {
				t.Fatalf("short-nil n=%d err=%v", n, err)
			}
		}
	}
}
func TestPacketHTTPFirstWriteProgress(t *testing.T) {
	failure := errors.New("wire failure")
	for _, prefix := range []int{-1, 0, 3, 10} {
		raw := &partialWire{payloadLimit: &prefix, failure: failure}
		conn := &httpConn{Conn: raw, cfg: &HTTPConfig{Method: "GET", Host: "example.org"}}
		n, err := conn.Write(make([]byte, 10))
		if n != max(0, prefix) || !errors.Is(err, failure) || !raw.closed {
			t.Fatalf("prefix=%d n=%d err=%v", prefix, n, err)
		}
	}
}
