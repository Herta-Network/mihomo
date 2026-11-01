package nat

import (
	"net"
	"net/netip"
	"sync"

	C "github.com/metacubex/mihomo/constant"
	"github.com/metacubex/sing/common/buf"
	M "github.com/metacubex/sing/common/metadata"
)

type writeBackProxy struct {
	access        sync.RWMutex
	wb            C.WriteBack
	defaultSource netip.AddrPort
}

func (w *writeBackProxy) WriteBack(b []byte, addr net.Addr) (n int, err error) {
	w.access.RLock()
	wb := w.wb
	defaultSource := w.defaultSource
	w.access.RUnlock()
	// Do not hold the lock across I/O: a blocked reply must not prevent the
	// upload path from updating its target or initiating transport shutdown.
	if addr == nil && defaultSource.IsValid() {
		addr = net.UDPAddrFromAddrPort(defaultSource)
	}
	return wb.WriteBack(b, addr)
}

func (w *writeBackProxy) UpdateWriteBack(wb C.WriteBack) {
	var defaultSource netip.AddrPort
	if source, ok := wb.(C.WriteBackDefaultAddress); ok {
		defaultSource = source.DefaultWriteBackAddress()
	}
	wb = C.GetWriteBackTarget(wb)
	w.access.Lock()
	w.wb = wb
	w.defaultSource = defaultSource
	w.access.Unlock()
}

func (w *writeBackProxy) WriteBackBuffer(buffer *buf.Buffer, destination M.Socksaddr) error {
	w.access.RLock()
	wb := w.wb
	w.access.RUnlock()
	return C.WriteBackBuffer(wb, buffer, destination)
}

func NewWriteBackProxy(wb C.WriteBack) C.WriteBackProxy {
	w := &writeBackProxy{}
	w.UpdateWriteBack(wb)
	return w
}
