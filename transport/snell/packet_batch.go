package snell

import (
	"net"
	"os"

	"github.com/metacubex/mihomo/transport/socks5"
	"github.com/metacubex/sing/common/buf"
	"github.com/metacubex/sing/common/bufio"
	M "github.com/metacubex/sing/common/metadata"
	N "github.com/metacubex/sing/common/network"
)

type packetFrameBatchWriter interface{ WritePacketFramesCount([][]byte) (int, error) }

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
	defer buf.ReleaseMulti(buffers)
	pc.wMux.Lock()
	defer pc.wMux.Unlock()
	if pc.writeErr != nil {
		return 0, pc.writeErr
	}
	if pc.closed.Load() {
		return 0, net.ErrClosed
	}
	if len(buffers) == 0 || len(buffers) != len(destinations) {
		return 0, os.ErrInvalid
	}
	writer, ok := pc.frameBatchWriter()
	if !ok {
		return 0, os.ErrInvalid
	}
	frames := &packetFrameCollector{}
	for i, buffer := range buffers {
		if buffer == nil {
			return 0, os.ErrInvalid
		}
		addr := destinations[i]
		if !addr.IsIP() && (len(addr.Fqdn) == 0 || len(addr.Fqdn) > 255) {
			return 0, os.ErrInvalid
		}
		var target net.Addr = addr.UDPAddr()
		if addr.IsFqdn() {
			target = addr
		}
		if _, err := WritePacket(frames, socks5.ParseAddrToSocksAddr(target), buffer.Bytes()); err != nil {
			return 0, err
		}
	}
	n, err := writer.WritePacketFramesCount(frames.data)
	n, err = bufio.CheckPacketBatchCount(n, len(buffers), err)
	if err != nil {
		pc.writeErr = err
		_ = pc.Close()
	}
	return n, err
}

type packetFrameCollector struct{ data [][]byte }

func (c *packetFrameCollector) Write(p []byte) (int, error) {
	c.data = append(c.data, append([]byte(nil), p...))
	return len(p), nil
}
func (pc *packetConn) Close() error {
	pc.closed.Store(true)
	pc.closeOnce.Do(func() { pc.closeErr = pc.Conn.Close() })
	return pc.closeErr
}
