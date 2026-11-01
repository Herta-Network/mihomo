package packet

import (
	"io"
	"net"

	"github.com/metacubex/mihomo/constant/features"
	"github.com/metacubex/sing/common/buf"
	"github.com/metacubex/sing/common/bufio"
	M "github.com/metacubex/sing/common/metadata"
	N "github.com/metacubex/sing/common/network"
)

const maxPacketSize = 65535

// Reader adapts bounded read batches to one owned packet at a time. Reads and
// Close must be serialized; the owner closes the source first to interrupt I/O.
// Each returned buffer belongs to the caller, including empty datagrams.
type Reader struct {
	source       N.PacketReader
	options      N.ReadWaitOptions
	batch        N.PacketBatchReadWaiter
	connected    N.ConnectedPacketBatchReadWaiter
	single       N.PacketReadWaiter
	needCopy     bool
	buffers      []*buf.Buffer
	destinations []M.Socksaddr
	destination  M.Socksaddr
	offset       int
	closed       bool
}

func NewReader(source N.PacketReader, options N.ReadWaitOptions) *Reader {
	// Preserve an advertised MTU; otherwise allow a full UDP datagram. Low
	// memory builds reduce read-ahead, not packet capacity.
	if options.MTU == 0 {
		options.MTU = maxPacketSize
	}
	options.BatchSize = 4
	if features.WithLowMemory {
		options.BatchSize = 1
	}
	r := &Reader{source: source, options: options}
	if options.BatchSize > 1 {
		if batch, ok := bufio.CreatePacketBatchReadWaiter(source); ok {
			r.batch = batch
			r.needCopy = batch.InitializeReadWaiter(options)
			return r
		}
		if batch, ok := bufio.CreateConnectedPacketBatchReadWaiter(source); ok {
			r.connected = batch
			r.needCopy = batch.InitializeReadWaiter(options)
			return r
		}
	}
	if single, ok := bufio.CreatePacketReadWaiter(source); ok {
		r.single = single
		r.needCopy = single.InitializeReadWaiter(options)
	}
	return r
}

func (r *Reader) ReadPacket() (*buf.Buffer, M.Socksaddr, error) {
	if r.closed {
		return nil, M.Socksaddr{}, net.ErrClosed
	}
	if r.batch != nil || r.connected != nil {
		if r.offset == len(r.buffers) {
			var err error
			r.offset = 0
			if r.batch != nil {
				r.buffers, r.destinations, err = r.batch.WaitReadPackets()
			} else {
				r.buffers, r.destination, err = r.connected.WaitReadConnectedPackets()
			}
			if err == nil && (len(r.buffers) == 0 || (r.batch != nil && len(r.buffers) != len(r.destinations))) {
				err = io.ErrNoProgress
			}
			if err != nil {
				r.releasePending()
				return nil, M.Socksaddr{}, err
			}
		}
		buffer := r.buffers[r.offset]
		destination := r.destination
		if r.batch != nil {
			destination = r.destinations[r.offset]
		}
		r.buffers[r.offset] = nil
		r.offset++
		if buffer == nil {
			return nil, M.Socksaddr{}, io.ErrNoProgress
		}
		if r.needCopy {
			buffer = r.options.Copy(buffer)
		}
		return buffer, destination, nil
	}
	var buffer *buf.Buffer
	var destination M.Socksaddr
	var err error
	if r.single != nil {
		buffer, destination, err = r.single.WaitReadPacket()
	} else {
		buffer = r.options.NewPacketBuffer()
		destination, err = r.source.ReadPacket(buffer)
		r.options.PostReturn(buffer)
	}
	if err != nil {
		buffer.Release()
		return nil, M.Socksaddr{}, err
	}
	if buffer == nil {
		return nil, M.Socksaddr{}, io.ErrNoProgress
	}
	if r.needCopy {
		buffer = r.options.Copy(buffer)
	}
	return buffer, destination, nil
}

func (r *Reader) releasePending() {
	for i := r.offset; i < len(r.buffers); i++ {
		r.buffers[i].Release()
		r.buffers[i] = nil
	}
	r.buffers, r.destinations, r.offset = nil, nil, 0
}

func (r *Reader) Close() {
	if r.closed {
		return
	}
	r.closed = true
	r.releasePending()
	for _, waiter := range []any{r.batch, r.connected, r.single} {
		if releaser, ok := waiter.(N.ReadWaiterReleaser); ok {
			releaser.ReleaseReadWaiter()
		}
	}
}
