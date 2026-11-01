package packet

import (
	"io"
	"net"
	"runtime"

	"github.com/metacubex/sing/common/buf"
	M "github.com/metacubex/sing/common/metadata"
)

// BufferReader transfers one owned packet, preserving any available headroom.
// It shares all prefetched packets with the connection's other read APIs.
// On error it releases its storage and returns a nil buffer.
type BufferReader interface {
	ReadPacketBuffer() (*buf.Buffer, M.Socksaddr, error)
}

// ReadPacketBuffer uses only an explicit capability. The legacy fallback copies
// before releasing borrowed bytes; it never unwraps a pending-packet owner.
func ReadPacketBuffer(conn EnhancePacketConn) (*buf.Buffer, M.Socksaddr, error) {
	if reader, ok := conn.(BufferReader); ok {
		return reader.ReadPacketBuffer()
	}
	data, put, addr, err := conn.WaitReadFrom()
	if put != nil {
		defer put()
	}
	if err != nil {
		return nil, M.Socksaddr{}, err
	}
	buffer := buf.NewSize(len(data))
	_, _ = buffer.Write(data)
	return buffer, ownedPacketSource(addr), nil
}

func (c *enhancePacketConn) ReadPacketBuffer() (*buf.Buffer, M.Socksaddr, error) {
	// Preserve a protocol's explicit owned reader, including its own caches.
	if reader, ok := c.PacketConn.(BufferReader); ok {
		return reader.ReadPacketBuffer()
	}
	buffer := buf.NewSize(maxPacketSize)
	n, addr, err := c.PacketConn.ReadFrom(buffer.FreeBytes())
	if err != nil {
		buffer.Release()
		return nil, M.Socksaddr{}, err
	}
	buffer.Resize(0, n)
	return buffer, ownedPacketSource(addr), nil
}

func (c *enhanceUDPConn) ReadPacketBuffer() (*buf.Buffer, M.Socksaddr, error) {
	return c.readState.read(c.source)
}

func (c *enhanceSingPacketConn) ReadPacketBuffer() (*buf.Buffer, M.Socksaddr, error) {
	return c.readState.read(c.SingPacketConn)
}

func (c *refPacketConn) ReadPacketBuffer() (*buf.Buffer, M.Socksaddr, error) {
	defer runtime.KeepAlive(c.ref)
	return ReadPacketBuffer(c.pc)
}

func (c *threadSafePacketConn) ReadPacketBuffer() (*buf.Buffer, M.Socksaddr, error) {
	return ReadPacketBuffer(c.EnhancePacketConn)
}

func (c *enhanceWithBufferPacketConn) ReadPacketBuffer() (*buf.Buffer, M.Socksaddr, error) {
	var buffer *buf.Buffer
	n, addr, err := c.ReadFromWithBuffer(func(sizeHint int) []byte {
		buffer.Release()
		buffer = buf.NewSize(sizeHint)
		return buffer.FreeBytes()
	})
	if err != nil {
		buffer.Release()
		return nil, M.Socksaddr{}, err
	}
	if buffer == nil {
		if n != 0 {
			return nil, M.Socksaddr{}, io.ErrNoProgress
		}
		buffer = buf.NewSize(0)
	}
	buffer.Resize(0, n)
	return buffer, ownedPacketSource(addr), nil
}

func ownedPacketSource(addr net.Addr) M.Socksaddr {
	if udp, ok := addr.(*net.UDPAddr); ok {
		if udp == nil {
			return M.Socksaddr{}
		}
		return M.SocksaddrFromNetIP(udp.AddrPort())
	}
	return M.SocksaddrFromNet(addr)
}
