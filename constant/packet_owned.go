package constant

import (
	"io"
	"net"
	"os"

	"github.com/metacubex/sing/common/buf"
	M "github.com/metacubex/sing/common/metadata"
)

func WriteBackBuffer(wb WriteBack, buffer *buf.Buffer, destination M.Socksaddr) error {
	if buffer == nil {
		return os.ErrInvalid
	}
	if writer, ok := wb.(BufferWriteBack); ok {
		return writer.WriteBackBuffer(buffer, destination)
	}
	defer buffer.Release()
	var addr net.Addr
	if destination.IsFqdn() {
		addr = destination
	} else {
		addr = destination.UDPAddr()
	}
	n, err := wb.WriteBack(buffer.Bytes(), addr)
	if err == nil && n != buffer.Len() {
		err = io.ErrShortWrite
	}
	return err
}
