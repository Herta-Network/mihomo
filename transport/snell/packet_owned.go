package snell

import (
	"encoding/binary"
	"io"
	"net"
	"net/netip"
	"os"

	"github.com/metacubex/sing/common/buf"
	"github.com/metacubex/sing/common/bufio"
	M "github.com/metacubex/sing/common/metadata"
	N "github.com/metacubex/sing/common/network"
)

func (pc *packetConn) ReadPacketBuffer() (*buf.Buffer, M.Socksaddr, error) {
	pc.rMux.Lock()
	defer pc.rMux.Unlock()
	var buffer *buf.Buffer
	var err error
	if reader, ok := pc.Conn.(interface{ ReadPacketBuffer() (*buf.Buffer, error) }); ok {
		buffer, err = reader.ReadPacketBuffer()
	} else {
		buffer = buf.NewSize(maxLength)
		var n int
		n, err = pc.Conn.Read(buffer.FreeBytes())
		buffer.Resize(0, n)
	}
	if err != nil {
		buffer.Release()
		return nil, M.Socksaddr{}, err
	}
	frame := buffer.Bytes()
	addressLen := 0
	if len(frame) > 0 {
		switch frame[0] {
		case 4:
			addressLen = net.IPv4len
		case 6:
			addressLen = net.IPv6len
		}
	}
	if addressLen == 0 || len(frame) < 1+addressLen+2 {
		buffer.Release()
		return nil, M.Socksaddr{}, os.ErrInvalid
	}
	address, _ := netip.AddrFromSlice(frame[1 : 1+addressLen])
	destination := M.SocksaddrFrom(address.Unmap(), binary.BigEndian.Uint16(frame[1+addressLen:]))
	buffer.Advance(1 + addressLen + 2)
	return buffer, destination, nil
}

// WritePacketResponseBuffer consumes one reply and preserves its buffer through
// address framing and AEAD. The caller serializes this with all other writes.
func (s *Snell) WritePacketResponseBuffer(buffer *buf.Buffer, source M.Socksaddr) error {
	if !source.IsIP() {
		buffer.Release()
		return os.ErrInvalid
	}
	address := source.Addr.Unmap()
	addressLen := net.IPv6len
	if address.Is4() {
		addressLen = net.IPv4len
	}
	headerLen := 1 + addressLen + 2
	if buffer.Len() > maxLength-headerLen {
		buffer.Release()
		return io.ErrShortBuffer
	}
	writer, ok := s.Conn.(packetFrameBatchWriter)
	if !ok {
		defer buffer.Release()
		_, err := WritePacketResponse(s, source.UDPAddr(), buffer.Bytes())
		return err
	}
	buffer = (N.ReadWaitOptions{FrontHeadroom: 2 + 16 + headerLen, RearHeadroom: 16}).Copy(buffer)
	header := buffer.ExtendHeader(headerLen)
	if address.Is4() {
		header[0] = 4
		ip := address.As4()
		copy(header[1:], ip[:])
	} else {
		header[0] = 6
		ip := address.As16()
		copy(header[1:], ip[:])
	}
	binary.BigEndian.PutUint16(header[1+addressLen:], source.Port)
	s.responseBuffers[0] = buffer
	n, err := writer.WritePacketBuffersCount(s.responseBuffers[:])
	s.responseBuffers[0] = nil
	_, err = bufio.CheckPacketBatchCount(n, 1, err)
	if err != nil {
		_ = s.Close()
	}
	return err
}
