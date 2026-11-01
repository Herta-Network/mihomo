// Package packetbatch adapts protocol-owned packet encoders to counted writes.
package packetbatch

import (
	"io"
	"net"
	"os"
	"sync"
	"sync/atomic"

	"github.com/metacubex/sing/common/buf"
	"github.com/metacubex/sing/common/bufio"
	M "github.com/metacubex/sing/common/metadata"
)

const MaxPackets = 64

// State belongs to a connection, not to a factory result. Scalar and batch
// writers use the same Access lock; Close must interrupt I/O without taking it.
type State struct {
	Access    sync.Mutex
	Err       error
	closed    atomic.Bool
	closeOnce sync.Once
	closeErr  error
}

func (s *State) Check() error {
	if s.Err != nil {
		return s.Err
	}
	if s.closed.Load() {
		return net.ErrClosed
	}
	return nil
}
func (s *State) Close(closer io.Closer) error {
	s.closed.Store(true)
	s.closeOnce.Do(func() { s.closeErr = closer.Close() })
	return s.closeErr
}
func (s *State) Fail(closer io.Closer, err error) error {
	s.Err = err
	_ = s.Close(closer)
	return err
}

func Validate(buffers []*buf.Buffer, destinations []M.Socksaddr) error {
	if len(buffers) == 0 || len(buffers) != len(destinations) {
		return os.ErrInvalid
	}
	for i, b := range buffers {
		if b == nil {
			return os.ErrInvalid
		}
		addr := destinations[i]
		if !addr.IsIP() && (!addr.IsFqdn() || len(addr.Fqdn) > 255) {
			return os.ErrInvalid
		}
	}
	return nil
}

// Prepare adds a stateless protocol header. On success it transfers ownership
// to the returned buffer; on error the input remains owned by the caller.
type Prepare func(*buf.Buffer, M.Socksaddr) (*buf.Buffer, error)

// WriteStream serializes packet writes and groups at most MaxPackets frames.
// Each prepared buffer is one logical datagram, including an empty payload.
// No failed stream batch may be replayed: encoding/writing can advance state.
func (s *State) WriteStream(writer io.Writer, closer io.Closer, buffers []*buf.Buffer, destinations []M.Socksaddr, prepare Prepare) (written int, err error) {
	s.Access.Lock()
	defer s.Access.Unlock()
	if err = s.Check(); err != nil {
		buf.ReleaseMulti(buffers)
		return
	}
	if err = Validate(buffers, destinations); err != nil {
		buf.ReleaseMulti(buffers)
		return
	}
	for start := 0; start < len(buffers); {
		if err = s.Check(); err != nil {
			buf.ReleaseMulti(buffers[start:])
			return
		}
		end := min(start+MaxPackets, len(buffers))
		for i := start; i < end; i++ {
			var prepared *buf.Buffer
			prepared, err = prepare(buffers[i], destinations[i])
			if err != nil {
				buf.ReleaseMulti(buffers[start:])
				return
			}
			buffers[i] = prepared
		}
		n, writeErr := WriteFrames(writer, buffers[start:end])
		written += n
		if writeErr != nil {
			buf.ReleaseMulti(buffers[end:])
			return written, s.Fail(closer, writeErr)
		}
		start = end
	}
	return
}

// WriteFrames consumes already-framed buffers and maps byte progress to the
// complete logical packet prefix. It never bypasses an io.Writer's framing.
func WriteFrames(writer io.Writer, frames []*buf.Buffer) (written int, err error) {
	if len(frames) > MaxPackets {
		buf.ReleaseMulti(frames)
		return 0, os.ErrInvalid
	}
	var ends [MaxPackets]int64
	var total int64
	for i, frame := range frames {
		if frame == nil || frame.IsEmpty() {
			buf.ReleaseMulti(frames)
			return 0, os.ErrInvalid
		}
		total += int64(frame.Len())
		ends[i] = total
	}
	n, err := bufio.NewVectorisedWriterWithCount(writer).WriteVectorisedCount(frames)
	if n < 0 || n > total {
		n, err = 0, io.ErrShortWrite
	} else if n != total && err == nil {
		err = io.ErrShortWrite
	}
	for _, end := range ends[:len(frames)] {
		if end > n {
			break
		}
		written++
	}
	return
}
