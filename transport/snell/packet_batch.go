package snell

import (
	"encoding/binary"
	"io"
	"net"
	"os"

	"github.com/metacubex/sing/common/buf"
	"github.com/metacubex/sing/common/bufio"
	M "github.com/metacubex/sing/common/metadata"
	N "github.com/metacubex/sing/common/network"
)

type packetFrameBatchWriter interface {
	WritePacketBuffersCount([]*buf.Buffer) (int, error)
}

// Snell v3 uses AES-GCM with a 16-byte tag on the length and payload records.
// Reserve the largest address header so the ingress can supply owned buffers
// that need no copying during framing or encryption.
func (pc *packetConn) FrontHeadroom() int { return 2 + 16 + 1 + 1 + 255 + 2 }
func (pc *packetConn) RearHeadroom() int  { return 16 }

func (pc *packetConn) frameBatchWriter() (packetFrameBatchWriter, bool) {
	stream, ok := pc.Conn.(*Snell)
	if !ok {
		return nil, false
	}
	writer, ok := stream.Conn.(packetFrameBatchWriter)
	return writer, ok
}
func (pc *packetConn) CreatePacketBatchWriterWithCount() (N.PacketBatchWriterWithCount, bool) {
	_, ok := pc.frameBatchWriter()
	if !ok {
		return nil, false
	}
	return pc, true
}
func (pc *packetConn) WritePacketBatchCount(buffers []*buf.Buffer, destinations []M.Socksaddr) (int, error) {
	pc.wMux.Lock()
	defer pc.wMux.Unlock()
	if pc.writeErr != nil {
		buf.ReleaseMulti(buffers)
		return 0, pc.writeErr
	}
	if pc.closed.Load() {
		buf.ReleaseMulti(buffers)
		return 0, net.ErrClosed
	}
	if len(buffers) == 0 || len(buffers) != len(destinations) {
		buf.ReleaseMulti(buffers)
		return 0, os.ErrInvalid
	}
	writer, ok := pc.frameBatchWriter()
	if !ok {
		buf.ReleaseMulti(buffers)
		return 0, os.ErrInvalid
	}
	// Validate the entire batch before consuming headroom or advancing AEAD.
	for i, buffer := range buffers {
		if buffer == nil {
			buf.ReleaseMulti(buffers)
			return 0, os.ErrInvalid
		}
		header, err := packetHeaderLength(destinations[i])
		if err == nil && buffer.Len() > maxLength-header {
			err = io.ErrShortBuffer
		}
		if err != nil {
			buf.ReleaseMulti(buffers)
			return 0, err
		}
	}
	for i, buffer := range buffers {
		destination := destinations[i]
		headerSize, _ := packetHeaderLength(destination)
		buffer = (N.ReadWaitOptions{FrontHeadroom: 2 + 16 + headerSize, RearHeadroom: 16}).Copy(buffer)
		buffers[i] = buffer
		header := buffer.ExtendHeader(headerSize)
		header[0] = CommondUDPForward
		if destination.IsFqdn() {
			header[1] = byte(len(destination.Fqdn))
			copy(header[2:], destination.Fqdn)
		} else {
			ip := destination.Addr.Unmap()
			header[1] = 0
			if ip.Is4() {
				header[2] = 4
				address := ip.As4()
				copy(header[3:], address[:])
			} else {
				header[2] = 6
				address := ip.As16()
				copy(header[3:], address[:])
			}
		}
		binary.BigEndian.PutUint16(header[len(header)-2:], destination.Port)
	}
	n, err := writer.WritePacketBuffersCount(buffers)
	n, err = bufio.CheckPacketBatchCount(n, len(buffers), err)
	if err != nil {
		pc.writeErr = err
		_ = pc.Close()
	}
	return n, err
}

func packetHeaderLength(destination M.Socksaddr) (int, error) {
	if destination.IsFqdn() {
		if len(destination.Fqdn) > 255 {
			return 0, os.ErrInvalid
		}
		return 1 + 1 + len(destination.Fqdn) + 2, nil
	}
	if !destination.IsIP() {
		return 0, os.ErrInvalid
	}
	if destination.Addr.Unmap().Is4() {
		return 1 + 2 + net.IPv4len + 2, nil
	}
	return 1 + 2 + net.IPv6len + 2, nil
}
func (pc *packetConn) Close() error {
	pc.closed.Store(true)
	pc.closeOnce.Do(func() { pc.closeErr = pc.Conn.Close() })
	return pc.closeErr
}
