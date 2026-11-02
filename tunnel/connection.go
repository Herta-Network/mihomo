package tunnel

import (
	"context"
	"errors"
	"net"
	"net/netip"
	"sync"
	"time"

	N "github.com/metacubex/mihomo/common/net"
	C "github.com/metacubex/mihomo/constant"
	"github.com/metacubex/mihomo/constant/features"
	"github.com/metacubex/mihomo/log"
	"github.com/metacubex/sing/common/buf"
	"github.com/metacubex/sing/common/bufio"
	M "github.com/metacubex/sing/common/metadata"
	SN "github.com/metacubex/sing/common/network"
)

type packetSender struct {
	ctx       context.Context
	cancel    context.CancelFunc
	queue     chan C.PacketAdapter
	sendMutex sync.Mutex

	// destination NAT mapping
	originToTarget map[packetDestination]netip.Addr
	targetToOrigin map[netip.Addr]netip.Addr
	mappingMutex   sync.RWMutex

	// Process is the sole owner of this scratch space. Reuse it across writes:
	// slices passed to an interface otherwise escape once per batch.
	buffers      [packetSendBatchSize]*buf.Buffer
	destinations [packetSendBatchSize]M.Socksaddr
}

type packetDestination struct {
	ip   netip.Addr
	host string
}

func destinationKey(metadata *C.Metadata) packetDestination {
	if metadata.Host != "" {
		return packetDestination{host: metadata.Host}
	}
	return packetDestination{ip: metadata.DstIP}
}

// newPacketSender returns a bounded, nonblocking packet sender.
// It ensures that packets can be sent sequentially and without blocking
func newPacketSender() C.PacketSender {
	ctx, cancel := context.WithCancel(context.Background())
	s := &packetSender{
		ctx:    ctx,
		cancel: cancel,
		queue:  make(chan C.PacketAdapter, senderCapacity),

		originToTarget: make(map[packetDestination]netip.Addr),
		targetToOrigin: make(map[netip.Addr]netip.Addr),
	}
	return s
}

func (s *packetSender) AddMapping(originMetadata *C.Metadata, metadata *C.Metadata) {
	s.mappingMutex.Lock()
	defer s.mappingMutex.Unlock()
	originKey := destinationKey(originMetadata)
	originAddr := originMetadata.DstIP
	targetAddr := metadata.DstIP
	if addr := s.originToTarget[originKey]; !addr.IsValid() { // overwrite only if the record is illegal
		s.originToTarget[originKey] = targetAddr
	}
	if addr := s.targetToOrigin[targetAddr]; !addr.IsValid() { // overwrite only if the record is illegal
		s.targetToOrigin[targetAddr] = originAddr
	}
}

func (s *packetSender) RestoreReadFrom(addr netip.Addr) netip.Addr {
	s.mappingMutex.RLock()
	defer s.mappingMutex.RUnlock()
	if originAddr := s.targetToOrigin[addr]; originAddr.IsValid() {
		return originAddr
	}
	return addr
}

func (s *packetSender) packetAddress(pc C.PacketConn, packet C.PacketAdapter) netip.AddrPort {
	metadata := packet.Metadata()

	s.mappingMutex.RLock()
	targetAddr := s.originToTarget[destinationKey(metadata)]
	s.mappingMutex.RUnlock()

	if targetAddr.IsValid() {
		return netip.AddrPortFrom(targetAddr, metadata.DstPort)
	}

	originMetadata := metadata  // save origin metadata
	metadata = metadata.Clone() // don't modify PacketAdapter's metadata

	_ = preHandleMetadata(metadata) // error was pre-checked
	metadata = metadata.Pure()
	if metadata.Host != "" {
		// TODO: ResolveUDP may take a long time to block the Process loop
		//       but we want keep sequence sending so can't open a new goroutine
		if err := pc.ResolveUDP(s.ctx, metadata); err != nil {
			log.Warnln("[UDP] Resolve Ip error: %s", err)
			return netip.AddrPort{}
		}
	}

	if !metadata.DstIP.IsValid() {
		log.Warnln("[UDP] Destination ip not valid: %#v", metadata)
		return netip.AddrPort{}
	}
	s.AddMapping(originMetadata, metadata)
	return metadata.AddrPort()
}

func (s *packetSender) processPacket(pc C.PacketConn, packet C.PacketAdapter, writer SN.PacketBatchWriterWithCount) {
	defer packet.Drop()
	if addr := s.packetAddress(pc, packet); addr.IsValid() && s.ctx.Err() == nil {
		// A single owned packet benefits from the same buffer path without any
		// read-ahead, including in low-memory builds. Borrowed packets retain
		// their scalar WriteTo path.
		if owned, ok := packet.(C.PacketBuffer); ok && writer != nil {
			if buffer := owned.TakeBuffer(); buffer != nil {
				s.buffers[0] = SN.NewReadWaitOptions(nil, pc).Copy(buffer)
				s.destinations[0] = M.SocksaddrFromNetIP(addr)
				n, err := writer.WritePacketBatchCount(s.buffers[:1], s.destinations[:1])
				n, _ = bufio.CheckPacketBatchCount(n, 1, err)
				s.buffers[0], s.destinations[0] = nil, M.Socksaddr{}
				if n == 1 {
					_ = pc.SetReadDeadline(time.Now().Add(udpTimeout))
				}
				return
			}
		}
		_ = handleUDPToRemote(packet, pc, net.UDPAddrFromAddrPort(addr))
	}
}

const packetSendBatchSize = 4

func (s *packetSender) Process(pc C.PacketConn, proxy C.WriteBackProxy) {
	writer, canBatch := N.CreatePacketBatchWriterWithCount(pc)
	if !canBatch {
		writer = nil
	}
	canBatch = canBatch && !features.WithLowMemory
	batchSize := 1
	if canBatch {
		batchSize = packetSendBatchSize
	}
	for {
		select {
		case <-s.ctx.Done():
			return
		case first := <-s.queue:
			var packets [packetSendBatchSize]C.PacketAdapter
			packets[0] = first
			count := 1 + s.takePackets(packets[1:batchSize])
			if s.ctx.Err() != nil {
				for _, packet := range packets[:count] {
					packet.Drop()
				}
				return
			}
			if count == 1 {
				first := packets[0]
				if proxy != nil {
					proxy.UpdateWriteBack(first)
				}
				s.processPacket(pc, first, writer)
				continue
			}
			s.processBatch(pc, proxy, writer, packets[:count])
		}
	}
}

func (s *packetSender) processBatch(pc C.PacketConn, proxy C.WriteBackProxy, writer SN.PacketBatchWriterWithCount, packets []C.PacketAdapter) {
	defer func() {
		for _, packet := range packets {
			packet.Drop()
		}
	}()
	buffers := s.buffers[:]
	destinations := s.destinations[:]
	pending := 0
	options := SN.NewReadWaitOptions(nil, pc)
	flush := func() {
		if pending == 0 {
			return
		}
		if s.ctx.Err() != nil {
			buf.ReleaseMulti(buffers[:pending])
		} else {
			n, err := writer.WritePacketBatchCount(buffers[:pending], destinations[:pending])
			n, _ = bufio.CheckPacketBatchCount(n, pending, err)
			if n > 0 {
				_ = pc.SetReadDeadline(time.Now().Add(udpTimeout))
			}
		}
		clear(buffers[:pending])
		clear(destinations[:pending])
		pending = 0
	}
	defer flush()
	for _, packet := range packets {
		if s.ctx.Err() != nil {
			return
		}
		// Do not hold earlier packets behind resolution of a new destination.
		s.mappingMutex.RLock()
		mapped := s.originToTarget[destinationKey(packet.Metadata())].IsValid()
		s.mappingMutex.RUnlock()
		if !mapped {
			flush()
		}
		if proxy != nil {
			proxy.UpdateWriteBack(packet)
		}
		addr := s.packetAddress(pc, packet)
		if !addr.IsValid() {
			continue
		}
		if owned, ok := packet.(C.PacketBuffer); ok {
			buffers[pending] = owned.TakeBuffer()
		}
		if buffers[pending] == nil {
			data := packet.Data()
			if options.NeedHeadroom() {
				// A borrowed packet needs one owned copy, but no temporary
				// buf.As descriptor before allocating its destination buffer.
				buffer := options.NewBufferSize(len(data))
				_, _ = buffer.Write(data)
				buffers[pending] = buffer
			} else {
				buffers[pending] = buf.As(data)
			}
		} else {
			buffers[pending] = options.Copy(buffers[pending])
		}
		destinations[pending] = M.SocksaddrFromNetIP(addr)
		pending++
	}
}

func (s *packetSender) dropAll() {
	for {
		var packets [packetSendBatchSize]C.PacketAdapter
		n := s.takePackets(packets[:])
		if n == 0 {
			return
		}
		for _, packet := range packets[:n] {
			packet.Drop()
		}
	}
}

// takePackets drains immediately available packets without waiting for a full batch.
func (s *packetSender) takePackets(packets []C.PacketAdapter) int {
	n := 0
	for n < len(packets) {
		select {
		case p := <-s.queue:
			packets[n] = p
			n++
		default:
			return n
		}
	}
	return n
}
func (s *packetSender) Send(packet C.PacketAdapter) {
	s.sendMutex.Lock()
	if s.ctx.Err() == nil {
		select {
		case s.queue <- packet:
			s.sendMutex.Unlock()
			return
		default:
		}
	}
	s.sendMutex.Unlock()
	packet.Drop()
}

func (s *packetSender) Close() {
	s.sendMutex.Lock()
	s.cancel()
	s.sendMutex.Unlock()
	// After cancellation no sender can enqueue. Release callbacks run outside
	// the admission lock, so a callback may safely close or send again.
	s.dropAll()
}

func (s *packetSender) DoSniff(metadata *C.Metadata) error { return nil }

func handleUDPToRemote(packet C.UDPPacket, pc C.PacketConn, addr *net.UDPAddr) error {
	if addr == nil {
		return errors.New("udp addr invalid")
	}

	if _, err := pc.WriteTo(packet.Data(), addr); err != nil {
		return err
	}
	// reset timeout
	_ = pc.SetReadDeadline(time.Now().Add(udpTimeout))

	return nil
}

func handleUDPToLocal(writeBack C.WriteBack, pc C.PacketConn, sender C.PacketSender, key string, oAddrPort netip.AddrPort) {
	defer func() {
		sender.Close()
		_ = pc.Close()
		closeAllLocalCoon(key)
		natTable.Delete(key)
	}()
	for {
		_ = pc.SetReadDeadline(time.Now().Add(udpTimeout))
		data, put, addr, err := pc.WaitReadFrom()
		if err != nil {
			if put != nil {
				put()
			}
			return
		}
		var from M.Socksaddr
		if udp, ok := addr.(*net.UDPAddr); ok {
			if udp != nil {
				from = M.SocksaddrFromNetIP(udp.AddrPort())
			}
		} else {
			from = M.SocksaddrFromNet(addr)
		}
		if !from.IsIP() {
			log.Warnln("server returned an invalid UDP source (%s), force replace to (%s)", from, oAddrPort)
			from = M.SocksaddrFromNetIP(oAddrPort)
		}
		from.Addr = sender.RestoreReadFrom(from.Addr.Unmap()).Unmap()
		from.Fqdn = ""
		size := len(data)
		n, err := writeBack.WriteBack(data, from.UDPAddr())
		if put != nil {
			put()
		}
		if err != nil || n != size {
			return
		}
	}
}

func closeAllLocalCoon(lAddr string) {
	natTable.RangeForLocalConn(lAddr, func(key string, value *net.UDPConn) bool {
		conn := value

		conn.Close()
		log.Debugln("Closing TProxy local conn... lAddr=%s rAddr=%s", lAddr, key)
		return true
	})
}

func handleSocket(inbound, outbound net.Conn) {
	N.Relay(inbound, outbound)
}
