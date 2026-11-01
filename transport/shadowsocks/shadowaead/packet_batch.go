package shadowaead

import (
	"encoding/binary"
	"io"

	"github.com/metacubex/sing/common/buf"
	"github.com/metacubex/sing/common/bufio"
)

// WritePacketFramesCount writes one AEAD record per frame. The owning packet
// adapter serializes calls with scalar writes; frames remain caller-owned.
func (c *Conn) WritePacketFramesCount(frames [][]byte) (written int, err error) {
	for _, frame := range frames {
		if len(frame) == 0 || len(frame) > payloadSizeMask {
			return 0, io.ErrShortBuffer
		}
	}
	if c.w == nil {
		if err = c.initWriter(); err != nil {
			c.Close()
			return 0, err
		}
	}
	w := c.w
	upstream := bufio.NewVectorisedWriterWithCount(w.Writer)
	const batchSize = 64
	for start := 0; start < len(frames); {
		end := min(start+batchSize, len(frames))
		var records [batchSize]*buf.Buffer
		var ends [batchSize]int64
		var total int64
		nonce := w.nonce[:w.NonceSize()]
		tag := w.Overhead()
		for i, frame := range frames[start:end] {
			record := buf.NewSize(2 + tag + len(frame) + tag)
			header := record.Extend(2 + tag)
			binary.BigEndian.PutUint16(header, uint16(len(frame)))
			w.Seal(header[:0], nonce, header[:2], nil)
			increment(nonce)
			payload := record.Extend(len(frame) + tag)
			w.Seal(payload[:0], nonce, frame, nil)
			increment(nonce)
			records[i] = record
			total += int64(record.Len())
			ends[i] = total
		}
		n, writeErr := upstream.WriteVectorisedCount(records[:end-start])
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
			c.Close()
			return written, writeErr
		}
		start = end
	}
	return written, nil
}
