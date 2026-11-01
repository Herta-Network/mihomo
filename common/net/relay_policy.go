package net

import (
	"io"
	"runtime"
	"sync/atomic"
	"syscall"

	"github.com/metacubex/mihomo/common/pool"
	"github.com/metacubex/mihomo/constant/features"
	"github.com/metacubex/sing/common/bufio"
	"github.com/metacubex/sing/common/network"
)

var activeRelayCopies atomic.Int64

func relayReadOptions(active int64, parallelism int, lowMemory bool) bufio.CopyReadOptions {
	options := bufio.CopyReadOptions{BatchSize: 1, MaxBufferSize: pool.RelayBufferSize}
	if lowMemory {
		options.MaxBufferSize = 16 * 1024
		return options
	}
	// Bound aggregate read-ahead as concurrent relay directions increase.
	threshold := max(4, min(64, parallelism*4))
	if active <= int64(threshold) {
		options.BatchSize = 4
	} else {
		options.MaxBufferSize = 16 * 1024
	}
	return options
}

func relayCopyOptions(destination io.Writer, source io.Reader) bufio.CopyOptions {
	policy := func() bufio.CopyReadOptions {
		return relayReadOptions(activeRelayCopies.Load(), runtime.GOMAXPROCS(0), features.WithLowMemory)
	}
	// A read policy intentionally disables sing's splice path. Preserve it for
	// replaceable raw socket pairs, including counter wrappers.
	reader, _ := network.UnwrapCountReader(source, nil)
	writer, _ := network.UnwrapCountWriter(destination, nil)
	_, rawReader := reader.(syscall.Conn)
	_, rawWriter := writer.(syscall.Conn)
	if runtime.GOOS == "linux" && rawReader && rawWriter {
		return bufio.CopyOptions{TransferSize: func() int {
			options := policy()
			return options.MaxBufferSize * options.BatchSize
		}}
	}
	return bufio.CopyOptions{ReadPolicy: policy}
}

func relayCopy(destination io.Writer, source io.Reader) (int64, error) {
	activeRelayCopies.Add(1)
	defer activeRelayCopies.Add(-1)
	return bufio.CopyWithOptions(destination, source, relayCopyOptions(destination, source))
}
