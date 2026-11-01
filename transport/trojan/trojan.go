package trojan

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"io"
	"net"
	"sync"

	N "github.com/metacubex/mihomo/common/net"
	"github.com/metacubex/mihomo/common/net/packetbatch"
	"github.com/metacubex/mihomo/common/pool"
	"github.com/metacubex/mihomo/transport/socks5"
)

const (
	// max packet length
	maxLength = 65535
)

var (
	DefaultALPN          = []string{"h2", "http/1.1"}
	DefaultWebsocketALPN = []string{"http/1.1"}

	crlf = []byte{'\r', '\n'}
)

type Command = byte

const (
	CommandTCP byte = 1
	CommandUDP byte = 3
	CommandMux byte = 0x7f

	KeyLength = 56
)

func WriteHeader(w io.Writer, hexPassword [KeyLength]byte, command Command, socks5Addr []byte) error {
	buf := pool.GetBuffer()
	defer pool.PutBuffer(buf)

	buf.Write(hexPassword[:])
	buf.Write(crlf)

	buf.WriteByte(command)
	buf.Write(socks5Addr)
	buf.Write(crlf)

	n, err := w.Write(buf.Bytes())
	if n != buf.Len() && err == nil {
		err = io.ErrShortWrite
	}
	return err
}

func writePacket(w io.Writer, socks5Addr, payload []byte) (int, error) {
	buf := pool.GetBuffer()
	defer pool.PutBuffer(buf)

	buf.Write(socks5Addr)
	binary.Write(buf, binary.BigEndian, uint16(len(payload)))
	buf.Write(crlf)
	buf.Write(payload)

	if n, err := w.Write(buf.Bytes()); err != nil || n != buf.Len() {
		if err == nil {
			err = io.ErrShortWrite
		}
		return 0, err
	}
	return len(payload), nil
}

func WritePacket(w io.Writer, socks5Addr, payload []byte) (int, error) {
	if len(payload) > maxLength {
		return 0, errors.New("trojan UDP payload too large")
	}
	return writePacket(w, socks5Addr, payload)
}

func ReadPacket(r io.Reader, payload []byte) (net.Addr, int, int, error) {
	addr, err := socks5.ReadAddr(r, payload)
	if err != nil {
		return nil, 0, 0, errors.New("read addr error")
	}
	uAddr := addr.UDPAddr()
	if uAddr == nil {
		return nil, 0, 0, errors.New("parse addr error")
	}

	if _, err = io.ReadFull(r, payload[:2]); err != nil {
		return nil, 0, 0, errors.New("read length error")
	}

	total := int(binary.BigEndian.Uint16(payload[:2]))
	if total > maxLength {
		return nil, 0, 0, errors.New("packet invalid")
	}

	// read crlf
	if _, err = io.ReadFull(r, payload[:2]); err != nil {
		return nil, 0, 0, errors.New("read crlf error")
	}

	length := len(payload)
	if total < length {
		length = total
	}

	if _, err = io.ReadFull(r, payload[:length]); err != nil {
		return nil, 0, 0, errors.New("read packet error")
	}

	return uAddr, length, total - length, nil
}

var _ N.EnhancePacketConn = (*PacketConn)(nil)

type PacketConn struct {
	net.Conn
	mux    sync.Mutex
	writes packetbatch.State
}

func (pc *PacketConn) WriteTo(b []byte, addr net.Addr) (int, error) {
	return pc.writePacketCount(b, addr)
}

func (pc *PacketConn) ReadFrom(p []byte) (int, net.Addr, error) {
	data, release, addr, err := pc.WaitReadFrom()
	if release != nil {
		defer release()
	}
	if err != nil {
		return 0, nil, err
	}
	return copy(p, data), addr, nil
}

func (pc *PacketConn) WaitReadFrom() (data []byte, put func(), addr net.Addr, err error) {
	pc.mux.Lock()
	defer pc.mux.Unlock()

	destination, err := socks5.ReadAddr0(pc.Conn)
	if err != nil {
		return nil, nil, nil, err
	}
	udpAddr := destination.UDPAddr()
	if udpAddr == nil {
		return nil, nil, nil, errors.New("parse addr error")
	}
	addr = udpAddr

	var header [4]byte
	if _, err = io.ReadFull(pc.Conn, header[:]); err != nil {
		return nil, nil, nil, err
	}
	length := int(binary.BigEndian.Uint16(header[:2]))
	if length == 0 {
		return nil, nil, addr, nil
	}
	readBuf := pool.Get(length)
	put = func() { _ = pool.Put(readBuf) }
	if _, err = io.ReadFull(pc.Conn, readBuf); err != nil {
		put()
		return nil, nil, nil, err
	}
	data = readBuf

	return
}

func NewPacketConn(conn net.Conn) *PacketConn {
	return &PacketConn{Conn: conn}
}

func Key(password string) (key [56]byte) {
	hash := sha256.Sum224([]byte(password))
	hex.Encode(key[:], hash[:])
	return
}
