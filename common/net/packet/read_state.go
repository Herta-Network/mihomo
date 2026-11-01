package packet

import (
	"net"
	"sync"

	"github.com/metacubex/sing/common/buf"
	M "github.com/metacubex/sing/common/metadata"
	N "github.com/metacubex/sing/common/network"
)

// readState owns queued packets shared by ReadFrom, ReadPacket and WaitReadFrom.
// Closing the source must precede close(), because a read may hold the mutex.
type readState struct {
	mutex  sync.Mutex
	reader *Reader
	closed bool
}

func (s *readState) read(source N.PacketReader) (*buf.Buffer, M.Socksaddr, error) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	if s.closed {
		return nil, M.Socksaddr{}, net.ErrClosed
	}
	if s.reader == nil {
		s.reader = NewReader(source, N.ReadWaitOptions{})
	}
	return s.reader.ReadPacket()
}

func (s *readState) close() {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	s.closed = true
	if s.reader != nil {
		s.reader.Close()
	}
}

func (s *readState) waitReadFrom(source N.PacketReader) ([]byte, func(), net.Addr, error) {
	buffer, destination, err := s.read(source)
	if err != nil {
		return nil, nil, nil, err
	}
	var addr net.Addr = destination.UDPAddr()
	if destination.IsFqdn() {
		addr = destination
	}
	return buffer.Bytes(), buffer.Release, addr, nil
}

func (s *readState) readFrom(source N.PacketReader, p []byte) (int, net.Addr, error) {
	data, put, addr, err := s.waitReadFrom(source)
	if put != nil {
		defer put()
	}
	return copy(p, data), addr, err
}
