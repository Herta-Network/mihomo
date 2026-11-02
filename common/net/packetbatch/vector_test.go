package packetbatch

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"net"
	"testing"
	"time"

	"github.com/metacubex/sing/common/buf"
)

type vectorWriteFunc func([]byte) (int, error)

func (f vectorWriteFunc) Write(p []byte) (int, error) { return f(p) }

func TestPacketVectorWriterFallbackProgress(t *testing.T) {
	for _, tc := range []struct {
		n       int
		err     error
		wantN   int64
		wantErr error
	}{
		{6, nil, 6, nil},
		{4, nil, 4, io.ErrShortWrite},
		{4, io.ErrClosedPipe, 4, io.ErrClosedPipe},
		{6, io.ErrClosedPipe, 6, io.ErrClosedPipe},
		{-1, nil, 0, io.ErrShortWrite},
		{7, nil, 0, io.ErrShortWrite},
	} {
		t.Run(fmt.Sprint(tc.n, tc.err), func(t *testing.T) {
			calls := 0
			writer := NewVectorWriter(vectorWriteFunc(func(data []byte) (int, error) {
				calls++
				if string(data) != "onetwo" {
					t.Fatalf("framing changed: %q", data)
				}
				return tc.n, tc.err
			}))
			buffers := []*buf.Buffer{buf.NewSize(3), buf.NewSize(3)}
			buffers[0].WriteString("one")
			buffers[1].WriteString("two")
			n, err := writer.WriteVectorisedCount(buffers)
			if n != tc.wantN || !errors.Is(err, tc.wantErr) || calls != 1 {
				t.Fatalf("n=%d err=%v calls=%d", n, err, calls)
			}
			for _, buffer := range buffers {
				if buffer.RawCap() != 0 {
					t.Fatal("fallback retained owned buffer")
				}
			}
		})
	}
}

func TestPacketVectorWriterNativeProgressAndRelease(t *testing.T) {
	listener, err := net.ListenTCP("tcp4", &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	client, err := net.DialTCP("tcp4", nil, listener.Addr().(*net.TCPAddr))
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	server, err := listener.AcceptTCP()
	if err != nil {
		t.Fatal(err)
	}
	defer server.Close()
	client.SetDeadline(time.Now().Add(3 * time.Second))
	server.SetDeadline(time.Now().Add(3 * time.Second))
	writer := NewVectorWriter(client)
	for _, count := range []int{4, 64, 1, 4} {
		buffers := make([]*buf.Buffer, count)
		want := make([]byte, count)
		for i := range buffers {
			buffers[i] = buf.NewSize(1)
			buffers[i].WriteByte(byte(i))
			want[i] = byte(i)
		}
		if n, err := writer.WriteVectorisedCount(buffers); n != int64(count) || err != nil {
			t.Fatalf("n=%d err=%v", n, err)
		}
		got := make([]byte, count)
		if _, err = io.ReadFull(server, got); err != nil || !bytes.Equal(got, want) {
			t.Fatalf("stream=%v err=%v", got, err)
		}
		for _, buffer := range buffers {
			if buffer.RawCap() != 0 {
				t.Fatal("native write retained owned buffer")
			}
		}
	}
	client.Close()
	buffer := buf.NewSize(1)
	buffer.WriteByte(1)
	if _, err := writer.WriteVectorisedCount([]*buf.Buffer{buffer}); err == nil || buffer.RawCap() != 0 {
		t.Fatal("closed socket retained input or succeeded")
	}
	// A protocol may replace its carrier while retaining the connection state.
	// Reset must stop using the previous (now closed) native socket.
	writer.Reset(io.Discard)
	buffer = buf.NewSize(1)
	buffer.WriteByte(1)
	if n, err := writer.WriteVectorisedCount([]*buf.Buffer{buffer}); n != 1 || err != nil || buffer.RawCap() != 0 {
		t.Fatalf("reset writer: n=%d err=%v", n, err)
	}
}

func TestPacketVectorWriterSteadyFallbackAllocations(t *testing.T) {
	writer := NewVectorWriter(io.Discard)
	// Unmanaged input storage models owned buffers supplied by the caller;
	// the writer's temporary merged payload is acquired from the real pool.
	buffers := []*buf.Buffer{buf.As([]byte("one")), buf.As([]byte("two"))}
	if allocations := testing.AllocsPerRun(100, func() {
		if n, err := writer.WriteVectorisedCount(buffers); n != 6 || err != nil {
			t.Fatalf("n=%d err=%v", n, err)
		}
	}); allocations != 0 {
		t.Fatalf("steady vector fallback allocates: %g", allocations)
	}
}
