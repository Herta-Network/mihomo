package gost

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"testing"

	"github.com/metacubex/sing/common/buf"
	M "github.com/metacubex/sing/common/metadata"
)

type packetWire struct {
	net.Conn
	bytes.Buffer
	limit   int
	failure error
	calls   int
	closed  bool
}

func (c *packetWire) Read(p []byte) (int, error) { return c.Buffer.Read(p) }
func (c *packetWire) Write(p []byte) (int, error) {
	c.calls++
	n := len(p)
	if c.limit >= 0 {
		n = min(n, c.limit)
	}
	_, _ = c.Buffer.Write(p[:n])
	return n, c.failure
}
func (c *packetWire) Close() error { c.closed = true; return nil }
func TestPacketBatchFramingAndBoundDestination(t *testing.T) {
	destination := M.ParseSocksaddr("127.0.0.1:5353")
	raw := &packetWire{limit: -1}
	conn := &relayPacketConn{conn: raw, raddr: destination.UDPAddr()}
	bs := []*buf.Buffer{buf.NewSize(1), buf.NewSize(3)}
	bs[1].WriteString("one")
	wrong := destination
	wrong.Port++
	n, err := conn.WritePacketBatchCount(bs, []M.Socksaddr{destination, wrong})
	if err == nil || n != 0 || raw.calls != 0 || raw.closed {
		t.Fatal("address validation changed stream")
	}
	for _, b := range bs {
		if b.RawCap() != 0 {
			t.Fatal("input retained")
		}
	}
	bs = []*buf.Buffer{buf.As(nil), buf.As([]byte("next"))}
	n, err = conn.WritePacketBatchCount(bs, []M.Socksaddr{destination, destination})
	if err != nil || n != 2 || raw.calls != 1 {
		t.Fatalf("n=%d err=%v", n, err)
	}
	for _, want := range []string{"", "next"} {
		var size uint16
		if err = binary.Read(&raw.Buffer, binary.BigEndian, &size); err != nil {
			t.Fatal(err)
		}
		p := make([]byte, size)
		io.ReadFull(&raw.Buffer, p)
		if string(p) != want {
			t.Fatal("packet boundaries changed")
		}
	}
}
func TestPacketBatchPartialStreamStopsScalarRetry(t *testing.T) {
	destination := M.ParseSocksaddr("127.0.0.1:5353")
	raw := &packetWire{limit: 3, failure: nil}
	conn := &relayPacketConn{conn: raw, raddr: destination.UDPAddr()}
	n, err := conn.WritePacketBatchCount([]*buf.Buffer{buf.As(nil), buf.As([]byte("second"))}, []M.Socksaddr{destination, destination})
	if n != 1 || !errors.Is(err, io.ErrShortWrite) || !raw.closed {
		t.Fatalf("n=%d err=%v", n, err)
	}
	if _, err = conn.WriteTo([]byte("retry"), destination); err == nil || raw.calls != 1 {
		t.Fatal("failed stream retried")
	}
}
