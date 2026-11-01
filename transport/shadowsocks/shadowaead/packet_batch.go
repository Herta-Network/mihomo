package shadowaead

import (
	"encoding/binary"
	"io"
	"os"

	"github.com/metacubex/mihomo/common/net/packetbatch"
	"github.com/metacubex/sing/common/buf"
	N "github.com/metacubex/sing/common/network"
)

// WritePacketFramesCount writes one AEAD record per frame. The owning packet
// adapter serializes calls with scalar writes; frames remain caller-owned.
func (c *Conn) WritePacketFramesCount(frames [][]byte) (written int, err error) {
	buffers := make([]*buf.Buffer, len(frames))
	for i, frame := range frames {
		buffers[i] = buf.As(frame)
	}
	return c.WritePacketBuffersCount(buffers)
}

// WritePacketBuffersCount consumes owned frames and encrypts each in place when
// headroom permits. Validation precedes both the lazy salt and nonce changes.
// The packet adapter serializes this method with scalar writes.
func (c *Conn) WritePacketBuffersCount(frames []*buf.Buffer) (written int, err error) {
	if len(frames) == 0 {
		return 0, os.ErrInvalid
	}
	for _, frame := range frames {
		if frame == nil || frame.Len() == 0 || frame.Len() > payloadSizeMask {
			buf.ReleaseMulti(frames)
			return 0, io.ErrShortBuffer
		}
	}
	if c.w == nil {
		if err = c.initWriter(); err != nil {
			buf.ReleaseMulti(frames)
			c.Close()
			return 0, err
		}
	}
	w := c.w
	if c.packetWriter == nil {
		c.packetWriter = packetbatch.NewVectorWriter(w.Writer)
	}
	const batchSize = 64
	tag := w.Overhead()
	options := N.ReadWaitOptions{FrontHeadroom: 2 + tag, RearHeadroom: tag}
	for start := 0; start < len(frames); {
		end := min(start+batchSize, len(frames))
		var ends [batchSize]int64
		var total int64
		nonce := w.nonce[:w.NonceSize()]
		for i, frame := range frames[start:end] {
			frame = options.Copy(frame)
			frames[start+i] = frame
			payload := frame.Bytes()
			header := frame.ExtendHeader(2 + tag)
			binary.BigEndian.PutUint16(header, uint16(len(payload)))
			w.Seal(header[:0], nonce, header[:2], nil)
			increment(nonce)
			frame.Extend(tag)
			w.Seal(payload[:0], nonce, payload, nil)
			increment(nonce)
			total += int64(frame.Len())
			ends[i] = total
		}
		n, writeErr := c.packetWriter.WriteVectorisedCount(frames[start:end])
		if n < 0 || n > total {
			n, writeErr = 0, io.ErrShortWrite
		} else if n != total && writeErr == nil {
			writeErr = io.ErrShortWrite
		}
		for _, recordEnd := range ends[:end-start] {
			if recordEnd > n {
				break
			}
			written++
		}
		if writeErr != nil {
			buf.ReleaseMulti(frames[end:])
			c.Close()
			return written, writeErr
		}
		start = end
	}
	return written, nil
}
