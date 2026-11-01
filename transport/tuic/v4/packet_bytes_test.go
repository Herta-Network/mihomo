package v4

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"reflect"
	"testing"
)

func TestDatagramParserMatchesStream(t *testing.T) {
	addresses := []Address{{TYPE: AtypIPv4, ADDR: []byte{127, 0, 0, 1}, PORT: 443}, {TYPE: AtypIPv6, ADDR: make([]byte, 16), PORT: 5353}, {TYPE: AtypDomainName, ADDR: append([]byte{12}, []byte("example.test")...), PORT: 80}}
	for _, address := range addresses {
		for _, size := range []int{0, 1, 1200, 4096, 65535} {
			payload := bytes.Repeat([]byte{5}, size)
			packet := NewPacket(77, uint16(len(payload)), address, payload)
			var wire bytes.Buffer
			if err := packet.WriteTo(&wire); err != nil {
				t.Fatal(err)
			}
			expected, err := ReadPacket(bytes.NewBuffer(wire.Bytes()))
			if err != nil {
				t.Fatal(err)
			}
			actual, err := parsePacketBytes(wire.Bytes())
			if err != nil || !reflect.DeepEqual(actual, expected) {
				t.Fatalf("parsed packet differs: %v", err)
			}
			for n := 0; n < len(wire.Bytes()); n++ {
				if _, err := parsePacketBytes(wire.Bytes()[:n]); err == nil {
					t.Fatalf("truncated frame accepted at %d", n)
				}
			}
			if _, err := parsePacketBytes(append(append([]byte{}, wire.Bytes()...), 0)); err == nil {
				t.Fatal("trailing datagram byte accepted")
			}
			if size > 0 && &actual.DATA[0] != &wire.Bytes()[wire.Len()-size] {
				t.Fatal("payload was copied")
			}
		}
	}
}

type packetShortWriter struct{ bytes.Buffer }

func (w *packetShortWriter) Write(p []byte) (int, error) { return len(p) - 1, nil }
func TestPacketHeaderShortWrite(t *testing.T) {
	address := Address{TYPE: AtypIPv4, ADDR: []byte{127, 0, 0, 1}, PORT: 80}
	payload := []byte("data")
	packet := NewPacket(77, uint16(len(payload)), address, payload)
	if err := packet.WriteTo(&packetShortWriter{}); !errors.Is(err, io.ErrShortWrite) {
		t.Fatalf("short header: %v", err)
	}
}

func BenchmarkNativePacketParse(b *testing.B) {
	address := Address{TYPE: AtypIPv4, ADDR: []byte{127, 0, 0, 1}, PORT: 80}
	payload := make([]byte, 1200)
	packet := NewPacket(77, uint16(len(payload)), address, payload)
	var wire bytes.Buffer
	packet.WriteTo(&wire)
	data := wire.Bytes()
	for _, native := range []bool{false, true} {
		b.Run(fmt.Sprint(native), func(b *testing.B) {
			b.ReportAllocs()
			b.SetBytes(1200)
			for i := 0; i < b.N; i++ {
				var err error
				if native {
					_, err = parsePacketBytes(data)
				} else {
					_, err = ReadPacket(bytes.NewBuffer(data))
				}
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
