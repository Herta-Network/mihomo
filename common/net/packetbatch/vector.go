package packetbatch

import (
	"io"
	"net"

	"github.com/metacubex/sing/common/buf"
	N "github.com/metacubex/sing/common/network"
)

// VectorWriter reuses the iovec descriptors for a serialized connection writer.
// It consumes every input buffer. Only standard stream sockets use writev;
// protocol wrappers keep their explicit counted method or their ordinary Write.
// Calls must be serialized by the owning protocol's write lock.
type VectorWriter struct {
	writer  io.Writer
	counted N.VectorisedWriterWithCount
	native  bool
	inline  [4][]byte
	storage net.Buffers
	vectors net.Buffers
}

func NewVectorWriter(writer io.Writer) *VectorWriter {
	w := new(VectorWriter)
	w.Reset(writer)
	return w
}

// Reset changes the output while retaining scratch storage. It must run under
// the same owner lock as writes, including when a protocol replaces a wrapper.
func (w *VectorWriter) Reset(writer io.Writer) {
	w.writer, w.counted, w.native = writer, nil, false
	if counted, ok := writer.(N.VectorisedWriterWithCount); ok {
		w.counted = counted
	} else {
		switch writer.(type) {
		case *net.TCPConn, *net.UnixConn:
			w.native = true
		}
	}
}

func (w *VectorWriter) WriteVectorisedCount(buffers []*buf.Buffer) (int64, error) {
	if w.counted != nil {
		return w.counted.WriteVectorisedCount(buffers)
	}
	defer buf.ReleaseMulti(buffers)
	total := buf.LenMulti(buffers)
	var written int64
	var err error
	if w.native {
		if w.storage == nil {
			w.storage = w.inline[:]
		}
		if len(buffers) > len(w.storage) {
			w.storage = make(net.Buffers, len(buffers))
		}
		for i, buffer := range buffers {
			w.storage[i] = buffer.Bytes()
		}
		w.vectors = w.storage[:len(buffers)]
		written, err = w.vectors.WriteTo(w.writer)
		// WriteTo advances the slice and its elements. Clear the original span
		// as well, so neither successful nor failed writes retain payloads.
		clear(w.storage[:len(buffers)])
		w.vectors = nil
	} else {
		var data []byte
		if len(buffers) == 1 {
			data = buffers[0].Bytes()
		} else if total > 0 {
			data = buf.Get(total)
			if data != nil {
				defer buf.Put(data)
			} else {
				data = make([]byte, total)
			}
			buf.CopyMulti(data, buffers)
		}
		n, writeErr := w.writer.Write(data)
		written, err = int64(n), writeErr
	}
	if written < 0 || written > int64(total) {
		return 0, io.ErrShortWrite
	}
	if written != int64(total) && err == nil {
		err = io.ErrShortWrite
	}
	return written, err
}
