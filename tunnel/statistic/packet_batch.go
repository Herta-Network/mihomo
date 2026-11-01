package statistic

import (
	"os"

	"github.com/metacubex/mihomo/common/net/packet"
	"github.com/metacubex/sing/common/buf"
	"github.com/metacubex/sing/common/bufio"
	M "github.com/metacubex/sing/common/metadata"
	N "github.com/metacubex/sing/common/network"
)

func (ut *udpTracker) ReadPacketBuffer() (*buf.Buffer, M.Socksaddr, error) {
	buffer, destination, err := packet.ReadPacketBuffer(ut.PacketConn)
	if err == nil {
		download := int64(buffer.Len())
		if ut.pushToManager {
			ut.manager.PushDownloaded(download)
		}
		ut.DownloadTotal.Add(download)
	}
	return buffer, destination, err
}

func (ut *udpTracker) CreatePacketBatchWriterWithCount() (N.PacketBatchWriterWithCount, bool) {
	writer, ok := packet.CreatePacketBatchWriterWithCount(ut.PacketConn)
	if !ok {
		return nil, false
	}
	return &udpBatchWriter{writer: writer, tracker: ut}, true
}

type udpBatchWriter struct {
	writer  N.PacketBatchWriterWithCount
	tracker *udpTracker
}

func (w *udpBatchWriter) WritePacketBatchCount(buffers []*buf.Buffer, destinations []M.Socksaddr) (int, error) {
	if len(buffers) == 0 || len(buffers) != len(destinations) {
		buf.ReleaseMulti(buffers)
		return 0, os.ErrInvalid
	}
	var inline [8]int
	lengths := inline[:min(len(buffers), len(inline))]
	if len(buffers) > len(inline) {
		lengths = make([]int, len(buffers))
	}
	for i, b := range buffers {
		lengths[i] = b.Len()
	}
	n, err := w.writer.WritePacketBatchCount(buffers, destinations)
	n, err = bufio.CheckPacketBatchCount(n, len(buffers), err)
	var uploaded int64
	for _, length := range lengths[:n] {
		uploaded += int64(length)
	}
	if w.tracker.pushToManager {
		w.tracker.manager.PushUploaded(uploaded)
	}
	w.tracker.UploadTotal.Add(uploaded)
	return n, err
}
