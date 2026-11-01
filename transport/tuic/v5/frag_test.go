package v5

import (
	"bytes"
	"testing"
)

func TestDefragOutOfOrderDuplicateAndBound(t *testing.T) {
	d := &deFragger{}
	address := Address{TYPE: AtypIPv4, ADDR: []byte{127, 0, 0, 1}, PORT: 443}
	makeFragment := func(id uint8, data string) *Packet {
		p := NewPacket(3, 7, 3, id, uint16(len(data)), Address{TYPE: AtypNone}, []byte(data))
		if id == 0 {
			p.ADDR = address
		}
		return &p
	}
	if d.Feed(makeFragment(2, "last")) != nil || d.Feed(makeFragment(2, "duplicate")) != nil || d.Feed(makeFragment(0, "first")) != nil {
		t.Fatal("incomplete packet returned")
	}
	got := d.Feed(makeFragment(1, "middle"))
	if got == nil || string(got.DATA) != "firstmiddlelast" || got.FRAG_TOTAL != 1 || got.FRAG_ID != 0 || got.ADDR.PORT != 443 {
		t.Fatalf("bad reassembly: %+v", got)
	}
	// Reusing a packet ID starts a fresh assembly after completion.
	if d.Feed(makeFragment(0, "new")) != nil || d.Feed(makeFragment(1, "middle")) != nil {
		t.Fatal("stale packet returned")
	}
	if got = d.Feed(makeFragment(2, "tail")); got == nil || string(got.DATA) != "newmiddletail" {
		t.Fatal("packet ID retained completed data")
	}
	for i := uint8(0); i < 3; i++ {
		p := makeFragment(i, string(bytes.Repeat([]byte{8}, 30000)))
		if d.Feed(p) != nil {
			t.Fatal("oversized assembly accepted")
		}
	}
}

func TestFragmentInvalidSizeDoesNotSend(t *testing.T) {
	p := NewPacket(1, 2, 1, 0, 4096, Address{}, make([]byte, 4096))
	for _, size := range []int{-1, 0, 1} {
		if err := fragWriteNative(nil, p, new(bytes.Buffer), size); err == nil {
			t.Fatalf("accepted fragment size %d", size)
		}
	}
}
