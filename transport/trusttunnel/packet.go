package trusttunnel

import (
	"encoding/binary"
	"math"
	"net"

	"github.com/metacubex/mihomo/common/net/packetbatch"
	"github.com/metacubex/sing/common"
	"github.com/metacubex/sing/common/buf"
	E "github.com/metacubex/sing/common/exceptions"
	M "github.com/metacubex/sing/common/metadata"
	N "github.com/metacubex/sing/common/network"
	"github.com/metacubex/sing/common/rw"
)

type packetConn struct {
	httpConn
	readWaitOptions N.ReadWaitOptions
	writes          packetbatch.State
}

func (c *packetConn) InitializeReadWaiter(options N.ReadWaitOptions) (needCopy bool) {
	c.readWaitOptions = options
	return false
}

var (
	_ N.NetPacketConn    = (*clientPacketConn)(nil)
	_ N.FrontHeadroom    = (*clientPacketConn)(nil)
	_ N.PacketReadWaiter = (*clientPacketConn)(nil)
)

type clientPacketConn struct {
	packetConn
}

func (u *clientPacketConn) FrontHeadroom() int {
	return 4 + 16 + 2 + 16 + 2 + 1 + math.MaxUint8
}

func (u *clientPacketConn) WaitReadPacket() (buffer *buf.Buffer, destination M.Socksaddr, err error) {
	buffer = u.readWaitOptions.NewPacketBuffer()
	destination, err = u.ReadPacket(buffer)
	if err != nil {
		buffer.Release()
		return nil, M.Socksaddr{}, err
	}
	u.readWaitOptions.PostReturn(buffer)
	return buffer, destination, nil
}

func (u *clientPacketConn) ReadPacket(buffer *buf.Buffer) (destination M.Socksaddr, err error) {
	err = u.waitCreated()
	if err != nil {
		return M.Socksaddr{}, err
	}
	return u.readPacketFromServer(buffer)
}

func (u *clientPacketConn) ReadFrom(p []byte) (n int, addr net.Addr, err error) {
	buffer := buf.With(p)
	destination, err := u.ReadPacket(buffer)
	if err != nil {
		return 0, nil, err
	}
	return buffer.Len(), destination.UDPAddr(), nil
}

func (u *clientPacketConn) WritePacket(buffer *buf.Buffer, destination M.Socksaddr) error {
	return u.writePacketToServer(buffer, destination)
}

func (u *clientPacketConn) WriteTo(p []byte, addr net.Addr) (n int, err error) {
	err = u.WritePacket(buf.As(p), M.SocksaddrFromNet(addr))
	if err != nil {
		return 0, err
	}
	return len(p), nil
}

func (u *clientPacketConn) readPacketFromServer(buffer *buf.Buffer) (destination M.Socksaddr, err error) {
	header := buf.NewSize(4 + 16 + 2 + 16 + 2)
	defer header.Release()
	_, err = header.ReadFullFrom(u.body, header.Cap())
	if err != nil {
		return
	}
	var length uint32
	common.Must(binary.Read(header, binary.BigEndian, &length))
	var sourceAddressBuffer [16]byte
	common.Must1(header.Read(sourceAddressBuffer[:]))
	destination.Addr = parse16BytesIP(sourceAddressBuffer)
	common.Must(binary.Read(header, binary.BigEndian, &destination.Port))
	common.Must(rw.SkipN(header, 16+2)) // To local address:port
	payloadLen := int(length) - (16 + 2 + 16 + 2)
	if payloadLen < 0 {
		return M.Socksaddr{}, E.New("invalid udp length: ", length)
	}
	_, err = buffer.ReadFullFrom(u.body, payloadLen)
	return
}

func (u *clientPacketConn) writePacketToServer(buffer *buf.Buffer, source M.Socksaddr) error {
	_, err := u.WritePacketBatchCount([]*buf.Buffer{buffer}, []M.Socksaddr{source})
	return err
}

var (
	_ N.NetPacketConn    = (*serverPacketConn)(nil)
	_ N.FrontHeadroom    = (*serverPacketConn)(nil)
	_ N.PacketReadWaiter = (*serverPacketConn)(nil)
)

type serverPacketConn struct {
	packetConn
}

func (u *serverPacketConn) FrontHeadroom() int {
	return 4 + 16 + 2 + 16 + 2
}

func (u *serverPacketConn) WaitReadPacket() (buffer *buf.Buffer, destination M.Socksaddr, err error) {
	buffer = u.readWaitOptions.NewPacketBuffer()
	destination, err = u.ReadPacket(buffer)
	if err != nil {
		buffer.Release()
		return nil, M.Socksaddr{}, err
	}
	u.readWaitOptions.PostReturn(buffer)
	return buffer, destination, nil
}

func (u *serverPacketConn) ReadPacket(buffer *buf.Buffer) (destination M.Socksaddr, err error) {
	err = u.waitCreated()
	if err != nil {
		return M.Socksaddr{}, err
	}
	return u.readPacketFromClient(buffer)
}

func (u *serverPacketConn) ReadFrom(p []byte) (n int, addr net.Addr, err error) {
	buffer := buf.With(p)
	destination, err := u.ReadPacket(buffer)
	if err != nil {
		return 0, nil, err
	}
	return buffer.Len(), destination.UDPAddr(), nil
}

func (u *serverPacketConn) WritePacket(buffer *buf.Buffer, destination M.Socksaddr) error {
	return u.writePacketToClient(buffer, destination)
}

func (u *serverPacketConn) WriteTo(p []byte, addr net.Addr) (n int, err error) {
	err = u.WritePacket(buf.As(p), M.SocksaddrFromNet(addr))
	if err != nil {
		return 0, err
	}
	return len(p), nil
}

func (u *serverPacketConn) readPacketFromClient(buffer *buf.Buffer) (destination M.Socksaddr, err error) {
	header := buf.NewSize(4 + 16 + 2 + 16 + 2 + 1)
	defer header.Release()
	_, err = header.ReadFullFrom(u.body, header.Cap())
	if err != nil {
		return
	}
	var length uint32
	common.Must(binary.Read(header, binary.BigEndian, &length))
	var sourceAddressBuffer [16]byte
	common.Must1(header.Read(sourceAddressBuffer[:]))
	var sourcePort uint16
	common.Must(binary.Read(header, binary.BigEndian, &sourcePort))
	_ = sourcePort
	var destinationAddressBuffer [16]byte
	common.Must1(header.Read(destinationAddressBuffer[:]))
	destination.Addr = parse16BytesIP(destinationAddressBuffer)
	common.Must(binary.Read(header, binary.BigEndian, &destination.Port))
	var appNameLen uint8
	common.Must(binary.Read(header, binary.BigEndian, &appNameLen))
	if appNameLen > 0 {
		err = rw.SkipN(u.body, int(appNameLen))
		if err != nil {
			return M.Socksaddr{}, err
		}
	}
	payloadLen := int(length) - (16 + 2 + 16 + 2 + 1 + int(appNameLen))
	if payloadLen < 0 {
		return M.Socksaddr{}, E.New("invalid udp length: ", length)
	}
	_, err = buffer.ReadFullFrom(u.body, payloadLen)
	return
}

func (u *serverPacketConn) writePacketToClient(buffer *buf.Buffer, source M.Socksaddr) error {
	_, err := u.WritePacketBatchCount([]*buf.Buffer{buffer}, []M.Socksaddr{source})
	return err
}
