package net

import (
	"io"
	"runtime"
	"syscall"

	"github.com/metacubex/mihomo/constant/features"
	"github.com/metacubex/sing/common/bufio"
	"github.com/metacubex/sing/common/network"
)

func relayCopyOptions(destination io.Writer, source io.Reader) bufio.CopyOptions {
	// Keep native socket copy paths. For opaque protocol wrappers, keep the
	// checked copy engine but avoid vector read-ahead and concurrency-driven
	// buffer shrinking. Only low-memory builds request the smaller cap.
	reader, _ := network.UnwrapCountReader(source, nil)
	writer, _ := network.UnwrapCountWriter(destination, nil)
	_, rawReader := reader.(syscall.Conn)
	_, rawWriter := writer.(syscall.Conn)
	if rawReader && rawWriter {
		if !features.WithLowMemory {
			return bufio.CopyOptions{}
		}
		if runtime.GOOS == "linux" {
			return bufio.CopyOptions{TransferSize: func() int { return 16 * 1024 }}
		}
	}
	size := 32 * 1024
	if features.WithLowMemory {
		size = 16 * 1024
	}
	return bufio.CopyOptions{ReadPolicy: func() bufio.CopyReadOptions {
		return bufio.CopyReadOptions{BatchSize: 1, MaxBufferSize: size}
	}}
}

func relayCopy(destination io.Writer, source io.Reader) (int64, error) {
	return bufio.CopyWithOptions(destination, source, relayCopyOptions(destination, source))
}
