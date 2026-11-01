package deadline

import (
	"net"
	"testing"
	"time"
)

type benchmarkPacketConn struct{ net.PacketConn }

func (*benchmarkPacketConn) Close() error { return nil }

func BenchmarkPacketReadDeadlineReset(b *testing.B) {
	conn := NewNetPacketConn(&benchmarkPacketConn{})
	defer conn.Close()
	defer conn.SetReadDeadline(time.Time{})
	future := time.Now().Add(time.Hour)
	conn.SetReadDeadline(future)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := conn.SetReadDeadline(future); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkPipeDeadlineReset(b *testing.B) {
	for _, clear := range []bool{false, true} {
		name := "refresh"
		if clear {
			name = "clear_refresh"
		}
		b.Run(name, func(b *testing.B) {
			deadline := MakePipeDeadline()
			defer deadline.Set(time.Time{})
			future := time.Now().Add(time.Hour)
			deadline.Set(future)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if clear {
					deadline.Set(time.Time{})
				}
				deadline.Set(future)
			}
		})
	}
}
