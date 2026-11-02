package snell

import (
	"bytes"
	"errors"
	"io"
	"testing"

	M "github.com/metacubex/sing/common/metadata"
)

func TestPacketResponseRoundTrip(t *testing.T) {
	for _, source := range []M.Socksaddr{M.ParseSocksaddr("127.0.0.2:53"), M.ParseSocksaddr("[::1]:5353")} {
		wire := &packetMemoryConn{}
		psk := []byte("owned-response")
		writer := ServerStreamConn(wire, psk, Version3)
		reader := PacketConn(ServerStreamConn(wire, psk, Version3)).(*packetConn)
		for _, payload := range [][]byte{nil, []byte("reply"), bytes.Repeat([]byte{7}, 16000)} {
			if n, err := WritePacketResponse(writer, source.UDPAddr(), payload); err != nil || n != len(payload) {
				t.Fatalf("write n=%d err=%v", n, err)
			}
			output := make([]byte, 65535)
			n, destination, err := reader.ReadFrom(output)
			if err != nil || destination.String() != source.String() || !bytes.Equal(output[:n], payload) {
				t.Fatalf("source=%v size=%d err=%v", destination, n, err)
			}
		}
		if n, _, err := reader.ReadFrom(make([]byte, 65535)); n != 0 || !errors.Is(err, io.EOF) {
			t.Fatal(err)
		}
	}
}

func TestPacketResponseValidationPreservesCipher(t *testing.T) {
	wire := &packetMemoryConn{}
	psk := []byte("owned-response")
	writer := ServerStreamConn(wire, psk, Version3)
	for _, source := range []M.Socksaddr{{}, M.ParseSocksaddr("invalid.example:53")} {
		if _, err := WritePacketResponse(writer, source, []byte("invalid")); err == nil || wire.Len() != 0 {
			t.Fatal("invalid reply changed cipher")
		}
	}
	source := M.ParseSocksaddr("127.0.0.2:53")
	if _, err := WritePacketResponse(writer, source.UDPAddr(), []byte("valid")); err != nil {
		t.Fatal(err)
	}
	reader := PacketConn(ServerStreamConn(wire, psk, Version3)).(*packetConn)
	buffer := make([]byte, 64)
	n, got, err := reader.ReadFrom(buffer)
	if err != nil {
		t.Fatal(err)
	}
	if got.String() != source.String() || string(buffer[:n]) != "valid" {
		t.Fatal("invalid reply advanced nonce")
	}
}

type responseWriteFunc func([]byte) (int, error)

func (f responseWriteFunc) Write(p []byte) (int, error) { return f(p) }
func TestPacketResponseWriteProgress(t *testing.T) {
	for _, tc := range []struct {
		short   bool
		err     error
		want    int
		wantErr error
	}{
		{false, nil, 4, nil}, {true, nil, 0, io.ErrShortWrite}, {true, io.ErrClosedPipe, 0, io.ErrClosedPipe}, {false, io.ErrClosedPipe, 4, io.ErrClosedPipe},
	} {
		sink := responseWriteFunc(func(p []byte) (int, error) {
			if tc.short {
				return len(p) - 1, tc.err
			}
			return len(p), tc.err
		})
		n, err := WritePacketResponse(sink, M.ParseSocksaddr("127.0.0.1:53"), []byte("test"))
		if n != tc.want || !errors.Is(err, tc.wantErr) {
			t.Fatalf("n=%d err=%v", n, err)
		}
	}
}
