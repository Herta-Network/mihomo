package nat

import (
	"net"
	"sync"

	C "github.com/metacubex/mihomo/constant"
)

type writeBackProxy struct {
	access sync.RWMutex
	wb     C.WriteBack
}

func (w *writeBackProxy) WriteBack(b []byte, addr net.Addr) (n int, err error) {
	w.access.RLock()
	wb := w.wb
	w.access.RUnlock()
	// Do not hold the lock across I/O: a blocked reply must not prevent the
	// upload path from updating its target or initiating transport shutdown.
	return wb.WriteBack(b, addr)
}

func (w *writeBackProxy) UpdateWriteBack(wb C.WriteBack) {
	w.access.Lock()
	w.wb = wb
	w.access.Unlock()
}

func NewWriteBackProxy(wb C.WriteBack) C.WriteBackProxy {
	w := &writeBackProxy{}
	w.UpdateWriteBack(wb)
	return w
}
