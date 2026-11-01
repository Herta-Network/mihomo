package trusttunnel

import (
	"encoding/binary"
	"io"

	"github.com/metacubex/sing/common/buf"
	M "github.com/metacubex/sing/common/metadata"
	N "github.com/metacubex/sing/common/network"
)

func (c *packetConn) Close() error { return c.writes.Close(&c.httpConn) }
func (c *clientPacketConn) WritePacketBatchCount(buffers []*buf.Buffer, destinations []M.Socksaddr) (int, error) {
	return c.writes.WriteStream(&packetFlushWriter{&c.httpConn}, &c.httpConn, buffers, destinations, prepareClientPacket)
}
func (c *serverPacketConn) WritePacketBatchCount(buffers []*buf.Buffer, destinations []M.Socksaddr) (int, error) {
	return c.writes.WriteStream(&packetFlushWriter{&c.httpConn}, &c.httpConn, buffers, destinations, prepareServerPacket)
}

// Flush once after the joined write, preserving the HTTP body writer's count.
type packetFlushWriter struct{ conn *httpConn }

func (w *packetFlushWriter) Write(p []byte) (int, error) { return w.conn.writeFlush(p) }

func prepareClientPacket(buffer *buf.Buffer, dest M.Socksaddr) (*buf.Buffer, error) {
	return preparePacket(buffer, dest, true)
}
func prepareServerPacket(buffer *buf.Buffer, dest M.Socksaddr) (*buf.Buffer, error) {
	return preparePacket(buffer, dest, false)
}
func preparePacket(buffer *buf.Buffer, dest M.Socksaddr, toServer bool) (*buf.Buffer, error) {
	if !dest.IsIP() || buffer.Len() > 65535 {
		return nil, io.ErrShortBuffer
	}
	appName := AppName
	if len(appName) > 255 {
		appName = appName[:255]
	}
	headerLen := 4 + 16 + 2 + 16 + 2
	if toServer {
		headerLen += 1 + len(appName)
	}
	size := buffer.Len()
	buffer = (N.ReadWaitOptions{FrontHeadroom: headerLen}).Copy(buffer)
	header := buffer.ExtendHeader(headerLen)
	clear(header)
	binary.BigEndian.PutUint32(header, uint32(headerLen-4+size))
	ip := buildPaddingIP(dest.Addr)
	offset := 4
	if toServer {
		offset += 16 + 2
	}
	copy(header[offset:], ip[:])
	binary.BigEndian.PutUint16(header[offset+16:], dest.Port)
	if toServer {
		header[4+16+2+16+2] = byte(len(appName))
		copy(header[4+16+2+16+2+1:], appName)
	}
	return buffer, nil
}
