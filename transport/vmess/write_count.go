package vmess

import "io"

// wireCountWriter preserves progress through framing helpers that expose only
// an error. Its count includes headers; the owning carrier subtracts them.
type wireCountWriter struct {
	io.Writer
	written int
}

func (w *wireCountWriter) Write(p []byte) (int, error) {
	n, err := w.Writer.Write(p)
	if n < 0 || n > len(p) {
		return 0, io.ErrShortWrite
	}
	w.written += n
	if n != len(p) && err == nil {
		err = io.ErrShortWrite
	}
	return n, err
}
