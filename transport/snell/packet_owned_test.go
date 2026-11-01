package snell

import (
	"bytes"
	"errors"
	"io"
	"testing"

	"github.com/metacubex/sing/common/buf"
	M "github.com/metacubex/sing/common/metadata"
)

func TestPacketOwnedResponseRoundTrip(t *testing.T) {
	for _, source := range []M.Socksaddr{M.ParseSocksaddr("127.0.0.2:53"), M.ParseSocksaddr("[::1]:5353")} {
		wire := &packetMemoryConn{}
		psk := []byte("owned-response")
		writer := ServerStreamConn(wire, psk, Version3)
		reader := PacketConn(ServerStreamConn(wire, psk, Version3)).(*packetConn)
		for _, payload := range [][]byte{nil, []byte("reply"), bytes.Repeat([]byte{7}, 16000)} {
			input := buf.NewSize(64 + len(payload) + 16)
			input.Resize(64, 0)
			input.Write(payload)
			if err := writer.WritePacketResponseBuffer(input, source); err != nil {
				t.Fatal(err)
			}
			if input.RawCap() != 0 || writer.responseBuffers[0] != nil {
				t.Fatal("response buffer retained")
			}
			output, destination, err := reader.ReadPacketBuffer()
			if err != nil {
				t.Fatal(err)
			}
			if destination != source || !bytes.Equal(output.Bytes(), payload) || output.Start() == 0 {
				t.Fatalf("source=%v size=%d", destination, output.Len())
			}
			output.Release()
		}
		if output, _, err := reader.ReadPacketBuffer(); output != nil || !errors.Is(err, io.EOF) {
			t.Fatal(err)
		}
	}
}

func TestPacketOwnedResponseValidationPreservesCipher(t *testing.T) {
	wire := &packetMemoryConn{}
	psk := []byte("owned-response")
	writer := ServerStreamConn(wire, psk, Version3)
	for _, source := range []M.Socksaddr{{}, M.ParseSocksaddr("invalid.example:53")} {
		buffer := buf.NewSize(64)
		buffer.WriteString("invalid")
		if err := writer.WritePacketResponseBuffer(buffer, source); err == nil || wire.Len() != 0 || buffer.RawCap() != 0 {
			t.Fatal("invalid reply changed cipher or retained input")
		}
	}
	source := M.ParseSocksaddr("127.0.0.2:53")
	if err := writer.WritePacketResponseBuffer(buf.As([]byte("valid")), source); err != nil {
		t.Fatal(err)
	}
	reader := PacketConn(ServerStreamConn(wire, psk, Version3)).(*packetConn)
	buffer, got, err := reader.ReadPacketBuffer()
	if err != nil {
		t.Fatal(err)
	}
	defer buffer.Release()
	if got != source || string(buffer.Bytes()) != "valid" {
		t.Fatal("invalid reply advanced nonce")
	}
}
