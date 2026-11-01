package vless

import (
	"errors"
	"io"
	"net"
	"testing"

	"github.com/metacubex/sing/common/buf"
	M "github.com/metacubex/sing/common/metadata"
)

type partialRequestConn struct {
	net.Conn
	payloadLimit  int
	payloadLength int
	failure       error
	closed        bool
	calls         int
}

func (c *partialRequestConn) Write(p []byte) (int, error) {
	c.calls++
	return len(p) - c.payloadLength + c.payloadLimit, c.failure
}
func (c *partialRequestConn) Close() error { c.closed = true; return nil }
func TestPacketBatchCountsPastLazyRequestHeader(t *testing.T) {
	for _, tc := range []struct {
		limit, want int
		err         error
	}{{-1, 0, io.ErrClosedPipe}, {2, 1, io.ErrClosedPipe}, {3, 1, nil}, {5, 2, io.ErrClosedPipe}, {9, 3, io.ErrClosedPipe}} {
		raw := &partialRequestConn{payloadLimit: tc.limit, payloadLength: 9, failure: tc.err}
		client, err := NewClient("00000000-0000-4000-8000-000000000001", nil)
		if err != nil {
			t.Fatal(err)
		}
		stream, err := client.StreamConn(raw, &DstAddr{UDP: true, AddrType: AtypIPv4, Addr: []byte{127, 0, 0, 1}, Port: 5353})
		if err != nil {
			t.Fatal(err)
		}
		target := M.ParseSocksaddr("127.0.0.1:5353")
		conn := client.PacketConn(stream, target.UDPAddr()).(*PacketConn)
		bs := []*buf.Buffer{buf.NewSize(1), buf.NewSize(1), buf.NewSize(2)}
		_, _ = bs[1].WriteString("a")
		_, _ = bs[2].WriteString("bc")
		n, err := conn.WritePacketBatchCount(bs, []M.Socksaddr{target, target, target})
		wantErr := tc.err
		if wantErr == nil {
			wantErr = io.ErrShortWrite
		}
		if n != tc.want || !errors.Is(err, wantErr) || !raw.closed {
			t.Fatalf("limit=%d n=%d err=%v", tc.limit, n, err)
		}
		for _, b := range bs {
			if b.RawCap() != 0 {
				t.Fatal("buffer retained")
			}
		}
		if _, err = conn.WriteTo([]byte("retry"), target); err == nil || raw.calls != 1 {
			t.Fatal("failed header was replayed")
		}
	}
}
