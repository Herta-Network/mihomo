package constant

import (
	"errors"
	"io"
	"net"
	"testing"

	"github.com/metacubex/sing/common/buf"
	M "github.com/metacubex/sing/common/metadata"
)

type shortReply struct{}

func (shortReply) WriteBack(data []byte, _ net.Addr) (int, error) { return len(data) - 1, nil }

func TestPacketOwnedReplyFallbackShortWrite(t *testing.T) {
	buffer := buf.NewSize(32)
	buffer.WriteString("reply")
	err := WriteBackBuffer(shortReply{}, buffer, M.ParseSocksaddr("127.0.0.1:53"))
	if !errors.Is(err, io.ErrShortWrite) || buffer.RawCap() != 0 {
		t.Fatalf("err=%v retained=%d", err, buffer.RawCap())
	}
}
