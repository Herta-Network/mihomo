package gun

import (
	"errors"
	"io"
	"testing"
)

type countWriter struct {
	payloadLimit int
	failure      error
}

func (w countWriter) Write(p []byte) (int, error) { return len(p) - 10 + w.payloadLimit, w.failure }
func TestPacketCarrierWriteCountsPayload(t *testing.T) {
	failure := errors.New("wire write failed")
	for _, tc := range []struct {
		limit, want int
		err         error
	}{{-1, 0, failure}, {3, 3, failure}, {3, 3, nil}, {10, 10, failure}} {
		conn := &Conn{writer: countWriter{payloadLimit: tc.limit, failure: tc.err}}
		n, err := conn.Write(make([]byte, 10))
		wantErr := tc.err
		if wantErr == nil {
			wantErr = io.ErrShortWrite
		}
		if n != tc.want || !errors.Is(err, wantErr) {
			t.Fatalf("n=%d err=%v", n, err)
		}
	}
}
