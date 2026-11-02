package mieru

import (
	"bytes"
	"errors"
	M "github.com/metacubex/sing/common/metadata"
	"io"
	"net"
	"testing"
)

type writeBackTestConn struct {
	net.PacketConn
	data  []byte
	short bool
	err   error
}

func (c *writeBackTestConn) WriteTo(data []byte, _ net.Addr) (int, error) {
	c.data = bytes.Clone(data)
	if c.short {
		return len(data) - 1, c.err
	}
	return len(data), c.err
}

func TestPacketWriteBackPayloadCount(t *testing.T) {
	for _, destination := range []M.Socksaddr{M.ParseSocksaddr("127.0.0.1:53"), M.ParseSocksaddr("[::1]:53"), M.ParseSocksaddr("example.test:443")} {
		for _, size := range []int{0, 1, 1200, 60000} {
			payload := bytes.Repeat([]byte{7}, size)
			sink := &writeBackTestConn{}
			reply := &packet{pc: sink, addr: destination.UDPAddr()}
			n, err := reply.WriteBack(payload, destination)
			if err != nil || n != size || len(sink.data) <= size || !bytes.HasSuffix(sink.data, payload) {
				t.Fatalf("size=%d target=%v n=%d wire=%d err=%v", size, destination, n, len(sink.data), err)
			}
		}
	}
}

func TestPacketWriteBackFailureProgress(t *testing.T) {
	target := M.ParseSocksaddr("127.0.0.1:53")
	for _, test := range []struct {
		short   bool
		err     error
		want    int
		wantErr error
	}{
		{true, nil, 0, io.ErrShortWrite}, {true, net.ErrClosed, 0, net.ErrClosed}, {false, net.ErrClosed, 4, net.ErrClosed},
	} {
		sink := &writeBackTestConn{short: test.short, err: test.err}
		reply := &packet{pc: sink, addr: target.UDPAddr()}
		n, err := reply.WriteBack([]byte("test"), target)
		if n != test.want || !errors.Is(err, test.wantErr) {
			t.Fatalf("progress=%d err=%v", n, err)
		}
	}
}
