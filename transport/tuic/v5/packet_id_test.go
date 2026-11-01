package v5

import (
	"sync"
	"testing"

	"github.com/metacubex/mihomo/common/atomic"
)

func TestAssociationPacketIDsAcrossReplyWrappers(t *testing.T) {
	var sequence atomic.Uint32
	ids := make(chan uint16, 4096)
	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 256; j++ {
				reply := &quicStreamPacketConn{sharedPacketID: &sequence}
				ids <- reply.nextPacketID()
			}
		}()
	}
	wg.Wait()
	close(ids)
	seen := make(map[uint16]bool)
	for id := range ids {
		if seen[id] {
			t.Fatal("simultaneous replies reuse a fragment ID")
		}
		seen[id] = true
	}
	sequence.Store(65534)
	q := &quicStreamPacketConn{sharedPacketID: &sequence}
	for _, want := range []uint16{65535, 0, 1} {
		if got := q.nextPacketID(); got != want {
			t.Fatalf("wrap=%d want=%d", got, want)
		}
	}
}

func TestFragmentedConcurrentDatagramsKeepPayloads(t *testing.T) {
	q := &quicStreamPacketConn{}
	id1, id2 := q.nextPacketID(), q.nextPacketID()
	d := &deFragger{}
	fragment := func(id uint16, index uint8, data string) *Packet {
		p := NewPacket(1, id, 2, index, uint16(len(data)), Address{TYPE: AtypNone}, []byte(data))
		return &p
	}
	if d.Feed(fragment(id1, 0, "first-")) != nil || d.Feed(fragment(id2, 1, "two")) != nil {
		t.Fatal("incomplete datagram")
	}
	p1 := d.Feed(fragment(id1, 1, "one"))
	p2 := d.Feed(fragment(id2, 0, "second-"))
	if p1 == nil || p2 == nil || string(p1.DATA) != "first-one" || string(p2.DATA) != "second-two" {
		t.Fatal("concurrent fragments mixed")
	}
}
