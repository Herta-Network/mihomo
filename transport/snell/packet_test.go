package snell

import (
	"bytes"
	"testing"
)

func TestPacketReadFullRecord(t *testing.T) {
	payload := bytes.Repeat([]byte{0x5a}, 16000)
	wire := append([]byte{4, 127, 0, 0, 1, 0, 53}, payload...)
	output := make([]byte, len(payload))
	addr, n, err := ReadPacket(bytes.NewReader(wire), output)
	if err != nil || n != len(payload) || !bytes.Equal(payload, output) || addr.String() != "127.0.0.1:53" {
		t.Fatalf("n=%d address=%v error=%v", n, addr, err)
	}
}
