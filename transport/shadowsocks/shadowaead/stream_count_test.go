package shadowaead

import (
	"crypto/aes"
	"crypto/cipher"
	"errors"
	"io"
	"testing"
)

type recordProgressWriter struct {
	calls int
	full  bool
	err   error
}

func (w *recordProgressWriter) Write(p []byte) (int, error) {
	w.calls++
	if w.calls == 1 {
		return len(p), nil
	}
	if w.full {
		return len(p), w.err
	}
	return len(p) - 1, w.err
}

func TestPacketRecordWriteProgress(t *testing.T) {
	for _, tc := range []struct {
		name    string
		full    bool
		err     error
		want    int
		wantErr error
	}{
		{"short-nil", false, nil, payloadSizeMask, io.ErrShortWrite},
		{"short-error", false, io.ErrClosedPipe, payloadSizeMask, io.ErrClosedPipe},
		{"full-error", true, io.ErrClosedPipe, payloadSizeMask + 10, io.ErrClosedPipe},
	} {
		t.Run(tc.name, func(t *testing.T) {
			block, err := aes.NewCipher(make([]byte, 16))
			if err != nil {
				t.Fatal(err)
			}
			aead, err := cipher.NewGCM(block)
			if err != nil {
				t.Fatal(err)
			}
			writer := NewWriter(&recordProgressWriter{full: tc.full, err: tc.err}, aead)
			n, err := writer.Write(make([]byte, payloadSizeMask+10))
			if n != tc.want || !errors.Is(err, tc.wantErr) {
				t.Fatalf("written=%d err=%v", n, err)
			}
		})
	}
}
