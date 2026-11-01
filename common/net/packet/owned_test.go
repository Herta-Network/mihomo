package packet

import (
	"errors"
	"io"
	"net"
	"testing"

	"github.com/metacubex/mihomo/constant/features"
	"github.com/metacubex/sing/common/buf"
	M "github.com/metacubex/sing/common/metadata"
)

func TestPacketOwnedReaderSharesPendingBatch(t *testing.T) {
	if features.WithLowMemory {
		t.Skip("batch disabled in low-memory build")
	}
	for _, wrap := range []func(net.PacketConn) EnhancePacketConn{
		NewEnhancePacketConn,
		func(c net.PacketConn) EnhancePacketConn {
			return NewRefPacketConn(NewThreadSafePacketConn(c), new(int))
		},
	} {
		destination := M.ParseSocksaddr("example.org:53")
		source := &batchSource{}
		for _, payload := range []string{"first", "", "pending"} {
			b := buf.NewSize(64)
			b.Resize(16, 0)
			b.WriteString(payload)
			source.packets = append(source.packets, b)
			source.dests = append(source.dests, destination)
		}
		conn := wrap(source)
		data, put, _, err := conn.WaitReadFrom()
		if err != nil || string(data) != "first" {
			t.Fatal(err)
		}
		put()
		owned, got, err := ReadPacketBuffer(conn)
		if err != nil || owned != source.packets[1] || !owned.IsEmpty() || owned.Start() != 16 || got != destination {
			t.Fatalf("owned=%v source=%v err=%v", owned, got, err)
		}
		conn.Close()
		if source.packets[2].RawCap() != 0 || owned.RawCap() == 0 || source.reads != 1 || source.releases != 1 {
			t.Fatal("close crossed the caller/prefetch ownership boundary")
		}
		owned.Release()
		if b, _, err := ReadPacketBuffer(conn); b != nil || !errors.Is(err, net.ErrClosed) {
			t.Fatal(err)
		}
	}
}

type borrowedOwnedFallback struct {
	EnhancePacketConn
	data     []byte
	released int
	err      error
	addr     net.Addr
}

func (c *borrowedOwnedFallback) WaitReadFrom() ([]byte, func(), net.Addr, error) {
	return c.data, func() { c.released++; clear(c.data) }, c.addr, c.err
}

func TestPacketOwnedReaderFallbackReleaseAndInvalidSource(t *testing.T) {
	for _, err := range []error{nil, io.ErrUnexpectedEOF} {
		var invalid *net.UDPAddr
		c := &borrowedOwnedFallback{data: []byte("borrowed"), addr: invalid, err: err}
		buffer, source, gotErr := ReadPacketBuffer(c)
		if c.released != 1 || gotErr != err || source.IsValid() {
			t.Fatalf("released=%d source=%v err=%v", c.released, source, gotErr)
		}
		if err != nil {
			if buffer != nil {
				t.Fatal("failure transferred buffer")
			}
		} else {
			if string(buffer.Bytes()) != "borrowed" {
				t.Fatal("borrowed storage was returned after release")
			}
			buffer.Release()
		}
	}
}

func TestPacketOwnedReaderPreservesIPv6Zone(t *testing.T) {
	address := &net.UDPAddr{IP: net.ParseIP("fe80::1"), Port: 53, Zone: "en0"}
	conn := &borrowedOwnedFallback{data: []byte("zone"), addr: address}
	buffer, source, err := ReadPacketBuffer(conn)
	if err != nil {
		t.Fatal(err)
	}
	defer buffer.Release()
	if source.AddrPort() != address.AddrPort() {
		t.Fatalf("source scope changed: %v", source)
	}
}
