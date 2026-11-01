package tunnel

import (
	"fmt"
	"net/netip"
	"sync"
	"testing"

	C "github.com/metacubex/mihomo/constant"
)

func TestPacketSenderQueueWrapAndCapacity(t *testing.T) {
	s := newPacketSender().(*packetSender)
	defer s.Close()
	metadata := &C.Metadata{NetWork: C.UDP, DstIP: netip.IPv4Unspecified(), DstPort: 53}
	packets := make([]senderTestPacket, senderCapacity+65)
	adapters := make([]C.PacketAdapter, len(packets))
	for i := range packets {
		adapters[i] = C.NewPacketAdapter(&packets[i], metadata)
	}
	for _, packet := range adapters[:senderCapacity] {
		s.Send(packet)
	}
	s.Send(adapters[senderCapacity])
	if packets[senderCapacity].drops.Load() != 1 {
		t.Fatal("full queue did not drop new packet")
	}
	var first [64]C.PacketAdapter
	if n := s.takePackets(first[:]); n != len(first) {
		t.Fatalf("first dequeue=%d", n)
	}
	for i, packet := range first {
		if packet != adapters[i] {
			t.Fatal("FIFO changed before wrap")
		}
		packet.Drop()
	}
	for _, packet := range adapters[senderCapacity+1:] {
		s.Send(packet)
	}
	if s.queue.Len() != senderCapacity || s.queue.Cap() != senderCapacity {
		t.Fatal("bounded queue grew during wrap")
	}
	want := append(adapters[64:senderCapacity:senderCapacity], adapters[senderCapacity+1:]...)
	for len(want) > 0 {
		var batch [7]C.PacketAdapter
		n := s.takePackets(batch[:])
		if n != min(len(batch), len(want)) {
			t.Fatal("wrong wrapped dequeue size")
		}
		for i, packet := range batch[:n] {
			if packet != want[i] {
				t.Fatal("FIFO changed across ring wrap")
			}
			packet.Drop()
		}
		want = want[n:]
	}
	s.Close()
	for i := range packets {
		if packets[i].drops.Load() != 1 {
			t.Fatalf("packet %d drops=%d", i, packets[i].drops.Load())
		}
	}
}

func TestPacketSenderQueueRepeatedEmptyWakeup(t *testing.T) {
	s := newPacketSender().(*packetSender)
	metadata := &C.Metadata{NetWork: C.UDP, DstIP: netip.MustParseAddr("127.0.0.1"), DstPort: 53}
	s.AddMapping(metadata, metadata)
	raw := &senderTestConn{fullBatch: true}
	done := make(chan struct{})
	go func() { s.Process(raw, nil); close(done) }()
	defer func() { s.Close(); waitSender(t, done) }()
	dropped := make(chan struct{}, 16)
	var want []string
	for round := 0; round < 100; round++ {
		count := 1 + round%7
		for i := 0; i < count; i++ {
			payload := fmt.Sprintf("%d/%d", round, i)
			want = append(want, payload)
			s.Send(C.NewPacketAdapter(&senderTestPacket{payload: []byte(payload), dropped: dropped}, metadata))
		}
		receiveDrops(t, dropped, count)
	}
	s.Close()
	waitSender(t, done)
	if len(raw.packets) != len(want) {
		t.Fatalf("received %d/%d", len(raw.packets), len(want))
	}
	for i := range want {
		if raw.packets[i] != want[i] {
			t.Fatalf("packet %d=%q want %q", i, raw.packets[i], want[i])
		}
	}
}

func TestPacketSenderQueueConcurrentProcessAndClose(t *testing.T) {
	s := newPacketSender().(*packetSender)
	metadata := &C.Metadata{NetWork: C.UDP, DstIP: netip.MustParseAddr("127.0.0.1"), DstPort: 53}
	s.AddMapping(metadata, metadata)
	packets := make([]senderTestPacket, 1000)
	done := make(chan struct{})
	go func() { s.Process(&senderTestConn{fullBatch: true}, nil); close(done) }()
	var workers sync.WaitGroup
	start := make(chan struct{})
	for worker := 0; worker < 4; worker++ {
		workers.Add(1)
		go func(worker int) {
			defer workers.Done()
			<-start
			for i := worker; i < len(packets); i += 4 {
				s.Send(C.NewPacketAdapter(&packets[i], metadata))
				if i == 0 {
					s.Close()
				}
			}
		}(worker)
	}
	close(start)
	workers.Wait()
	s.Close()
	waitSender(t, done)
	for i := range packets {
		if packets[i].drops.Load() != 1 {
			t.Fatalf("packet %d drops=%d", i, packets[i].drops.Load())
		}
	}
}
