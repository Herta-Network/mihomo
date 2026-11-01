package packet

import (
	"net"
	"runtime"
	"sync"

	"github.com/metacubex/sing/common/buf"
	"github.com/metacubex/sing/common/bufio"
	M "github.com/metacubex/sing/common/metadata"
	N "github.com/metacubex/sing/common/network"
)

// CreatePacketBatchWriterWithCount only follows explicitly exposed capabilities.
// Unknown net.PacketConn wrappers retain their own WriteTo path.
func CreatePacketBatchWriterWithCount(conn net.PacketConn) (N.PacketBatchWriterWithCount, bool) {
	if creator, ok := conn.(N.PacketBatchWriteCountCreator); ok {
		return creator.CreatePacketBatchWriterWithCount()
	}
	if writer, ok := conn.(N.PacketBatchWriterWithCount); ok {
		return writer, true
	}
	if writer, ok := conn.(N.PacketWriter); ok {
		return bufio.CreatePacketBatchWriterWithCount(writer)
	}
	if conn, ok := conn.(*net.UDPConn); ok {
		return bufio.CreatePacketBatchWriterWithCount(bufio.NewPacketConn(conn))
	}
	return nil, false
}

func (c *enhanceUDPConn) CreatePacketBatchWriterWithCount() (N.PacketBatchWriterWithCount, bool) {
	return CreatePacketBatchWriterWithCount(c.PacketConn)
}
func (c *enhanceSingPacketConn) CreatePacketBatchWriterWithCount() (N.PacketBatchWriterWithCount, bool) {
	return bufio.CreatePacketBatchWriterWithCount(c.SingPacketConn)
}
func (c *threadSafePacketConn) CreatePacketBatchWriterWithCount() (N.PacketBatchWriterWithCount, bool) {
	writer, ok := CreatePacketBatchWriterWithCount(c.EnhancePacketConn)
	if !ok {
		return nil, false
	}
	return &protectedBatchWriter{writer: writer, access: &c.access, ref: c}, true
}
func (c *refPacketConn) CreatePacketBatchWriterWithCount() (N.PacketBatchWriterWithCount, bool) {
	writer, ok := CreatePacketBatchWriterWithCount(c.pc)
	if !ok {
		return nil, false
	}
	return &protectedBatchWriter{writer: writer, ref: c}, true
}

type protectedBatchWriter struct {
	writer N.PacketBatchWriterWithCount
	access *sync.Mutex
	ref    any
}

func (w *protectedBatchWriter) WritePacketBatchCount(buffers []*buf.Buffer, destinations []M.Socksaddr) (int, error) {
	defer runtime.KeepAlive(w.ref)
	if w.access != nil {
		w.access.Lock()
		defer w.access.Unlock()
	}
	return w.writer.WritePacketBatchCount(buffers, destinations)
}

func (c *enhancePacketConn) CreatePacketBatchWriterWithCount() (N.PacketBatchWriterWithCount, bool) {
	return CreatePacketBatchWriterWithCount(c.PacketConn)
}
