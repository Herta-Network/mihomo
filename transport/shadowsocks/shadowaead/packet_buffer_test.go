package shadowaead

import (
	"bytes"
	"crypto/aes"
	"crypto/cipher"
	"errors"
	"io"
	"testing"
)

func packetTestAEAD(t *testing.T) cipher.AEAD {
	t.Helper()
	block, err := aes.NewCipher(make([]byte, 16))
	if err != nil {
		t.Fatal(err)
	}
	aead, err := cipher.NewGCM(block)
	if err != nil {
		t.Fatal(err)
	}
	return aead
}

func TestPacketBufferReadPreservesRecordsAndScalarRemainder(t *testing.T) {
	aead := packetTestAEAD(t)
	var wire bytes.Buffer
	writer := NewWriter(&wire, aead)
	payloads := [][]byte{[]byte("prefix"), []byte("small"), bytes.Repeat([]byte{7}, payloadSizeMask)}
	for _, payload := range payloads {
		if _, err := writer.Write(payload); err != nil {
			t.Fatal(err)
		}
	}
	reader := NewReader(&wire, aead)
	var first [1]byte
	if n, err := reader.Read(first[:]); n != 1 || err != nil || first[0] != 'p' {
		t.Fatalf("scalar n=%d err=%v", n, err)
	}
	payloads[0] = payloads[0][1:]
	for i, want := range payloads {
		buffer, err := reader.ReadPacketBuffer()
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(buffer.Bytes(), want) || buffer.Start() < 2+aead.Overhead() || buffer.FreeLen() < aead.Overhead() {
			t.Fatalf("record %d lost payload or reserved space", i)
		}
		if len(want) < 10 && buffer.RawCap() > 128 {
			t.Fatal("small packet retained a maximum-size read buffer")
		}
		buffer.Release()
	}
	if b, err := reader.ReadPacketBuffer(); b != nil || !errors.Is(err, io.EOF) {
		t.Fatalf("EOF buffer=%v err=%v", b, err)
	}
}

func TestPacketBufferReadFailure(t *testing.T) {
	aead := packetTestAEAD(t)
	var wire bytes.Buffer
	if _, err := NewWriter(&wire, aead).Write([]byte("payload")); err != nil {
		t.Fatal(err)
	}
	encoded := wire.Bytes()
	for _, size := range []int{0, 1, 2 + aead.Overhead(), len(encoded) - 1} {
		reader := NewReader(bytes.NewReader(encoded[:size]), aead)
		if b, err := reader.ReadPacketBuffer(); b != nil || err == nil {
			t.Fatalf("truncated %d-byte record returned buffer=%v err=%v", size, b, err)
		}
	}
	corrupt := append([]byte(nil), encoded...)
	corrupt[len(corrupt)-1] ^= 1
	if b, err := NewReader(bytes.NewReader(corrupt), aead).ReadPacketBuffer(); b != nil || err == nil {
		t.Fatal("unauthenticated payload returned to caller")
	}
	wire.Reset()
	if _, err := NewWriter(&wire, aead).Write(nil); err != nil {
		t.Fatal(err)
	}
	if b, err := NewReader(&wire, aead).ReadPacketBuffer(); b != nil || !errors.Is(err, ErrZeroChunk) {
		t.Fatalf("zero chunk buffer=%v err=%v", b, err)
	}
}

func TestPacketBufferReadAfterWriteToEOF(t *testing.T) {
	aead := packetTestAEAD(t)
	var wire bytes.Buffer
	writer := NewWriter(&wire, aead)
	if _, err := writer.Write([]byte("first")); err != nil {
		t.Fatal(err)
	}
	reader := NewReader(&wire, aead)
	if n, err := reader.WriteTo(io.Discard); n != 5 || err != nil {
		t.Fatalf("WriteTo n=%d err=%v", n, err)
	}
	if buffer, err := reader.ReadPacketBuffer(); buffer != nil || !errors.Is(err, io.EOF) {
		buffer.Release()
		t.Fatalf("consumed workspace returned as packet: buffer=%v err=%v", buffer, err)
	}
	// A temporary EOF from this in-memory source can be followed by another
	// record. Switching APIs must preserve the cipher's nonce sequence.
	if _, err := writer.Write([]byte("next")); err != nil {
		t.Fatal(err)
	}
	buffer, err := reader.ReadPacketBuffer()
	if err != nil {
		t.Fatal(err)
	}
	defer buffer.Release()
	if string(buffer.Bytes()) != "next" {
		t.Fatalf("next record=%q", buffer.Bytes())
	}
}
