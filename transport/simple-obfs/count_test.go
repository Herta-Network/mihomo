package obfs

import (
	"errors"
	"io"
	"net"
	"testing"
)

type countShortConn struct {
	net.Conn
	calls  int
	closed bool
}

func (c *countShortConn) Write(p []byte) (int, error) {
	c.calls++
	if c.calls == 1 {
		return len(p), nil
	}
	return 8, nil
}
func (c *countShortConn) Close() error { c.closed = true; return nil }
func TestTLSPayloadCountAcrossShortRecords(t *testing.T) {
	for _, server := range []bool{false, true} {
		raw := &countShortConn{}
		var conn net.Conn
		if server {
			conn = &TLSObfsServer{Conn: raw}
		} else {
			conn = &TLSObfs{Conn: raw}
		}
		n, err := conn.Write(make([]byte, chunkSize+10))
		if n != chunkSize+3 || !errors.Is(err, io.ErrShortWrite) || !raw.closed {
			t.Fatalf("n=%d err=%v closed=%v", n, err, raw.closed)
		}
	}
}
