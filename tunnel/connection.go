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
	ch        chan C.PacketAdapter
	sendMutex sync.Mutex

	// destination NAT mapping
	originToTarget map[string]netip.Addr
	targetToOrigin map[netip.Addr]netip.Addr
	mappingMutex   sync.RWMutex
}

// newPacketSender return a chan based C.PacketSender
// It ensures that packets can be sent sequentially and without blocking
func newPacketSender() C.PacketSender {
	ctx, cancel := context.WithCancel(context.Background())
	ch := make(chan C.PacketAdapter, senderCapacity)
	return &packetSender{
		ctx:    ctx,
		cancel: cancel,
		ch:     ch,

		originToTarget: make(map[string]netip.Addr),
		targetToOrigin: make(map[netip.Addr]netip.Addr),
	}
}

func (s *packetSender) AddMapping(originMetadata *C.Metadata, metadata *C.Metadata) {
	s.mappingMutex.Lock()
	defer s.mappingMutex.Unlock()
	originKey := originMetadata.String()
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

func (s *packetSender) packetAddress(pc C.PacketConn, packet C.PacketAdapter) *net.UDPAddr {
	metadata := packet.Metadata()

	var addr *net.UDPAddr

	s.mappingMutex.RLock()
	targetAddr := s.originToTarget[metadata.String()]
	s.mappingMutex.RUnlock()

	if targetAddr.IsValid() {
		addr = net.UDPAddrFromAddrPort(netip.AddrPortFrom(targetAddr, metadata.DstPort))
	}

	if addr == nil {
		originMetadata := metadata  // save origin metadata
		metadata = metadata.Clone() // don't modify PacketAdapter's metadata

		_ = preHandleMetadata(metadata) // error was pre-checked
		metadata = metadata.Pure()
		if metadata.Host != "" {
			// TODO: ResolveUDP may take a long time to block the Process loop
			//       but we want keep sequence sending so can't open a new goroutine
			if err := pc.ResolveUDP(s.ctx, metadata); err != nil {
				log.Warnln("[UDP] Resolve Ip error: %s", err)
				return nil
			}
		}

		if !metadata.DstIP.IsValid() {
			log.Warnln("[UDP] Destination ip not valid: %#v", metadata)
			return nil
		}
		s.AddMapping(originMetadata, metadata)
		addr = metadata.UDPAddr()
	}
	return addr
}

func (s *packetSender) processPacket(pc C.PacketConn, packet C.PacketAdapter) {
	defer packet.Drop()
	if addr := s.packetAddress(pc, packet); addr != nil && s.ctx.Err() == nil {
		_ = handleUDPToRemote(packet, pc, addr)
	}
}

const packetSendBatchSize = 4

func (s *packetSender) Process(pc C.PacketConn, proxy C.WriteBackProxy) {
	var writer SN.PacketBatchWriterWithCount
	canBatch := false
	if !features.WithLowMemory {
		writer, canBatch = N.CreatePacketBatchWriterWithCount(pc)
	}
	for {
		select {
		case <-s.ctx.Done():
			return
		case first := <-s.ch:
			if s.ctx.Err() != nil {
				first.Drop()
				return
			}
			if !canBatch {
				if proxy != nil {
					proxy.UpdateWriteBack(first)
				}
				s.processPacket(pc, first)
				continue
			}
			packets := [packetSendBatchSize]C.PacketAdapter{first}
			count := 1
		drain:
			for count < len(packets) {
				select {
				case packets[count] = <-s.ch:
					count++
				default:
					break drain
				}
			}
			if count == 1 {
				if proxy != nil {
					proxy.UpdateWriteBack(first)
				}
				s.processPacket(pc, first)
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
	var buffers [packetSendBatchSize]*buf.Buffer
	var destinations [packetSendBatchSize]M.Socksaddr
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
		mapped := s.originToTarget[packet.Metadata().String()].IsValid()
		s.mappingMutex.RUnlock()
		if !mapped {
			flush()
		}
		if proxy != nil {
			proxy.UpdateWriteBack(packet)
		}
		addr := s.packetAddress(pc, packet)
		if addr == nil {
			continue
		}
		buffers[pending] = options.Copy(buf.As(packet.Data()))
		destinations[pending] = M.SocksaddrFromNet(addr)
		pending++
	}
}

func (s *packetSender) dropAll() {
	for {
		select {
		case data := <-s.ch:
			data.Drop() // drop all data still in chan
		default:
			return // no data, exit goroutine
		}
	}
}

func (s *packetSender) Send(packet C.PacketAdapter) {
	s.sendMutex.Lock()
	if s.ctx.Err() == nil {
		select {
		case s.ch <- packet:
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
		data, put, from, err := pc.WaitReadFrom()
		if err != nil {
			return
		}

		fromUDPAddr, isUDPAddr := from.(*net.UDPAddr)
		if !isUDPAddr {
			fromUDPAddr = net.UDPAddrFromAddrPort(oAddrPort) // oAddrPort was Unmapped
			log.Warnln("server return a [%T](%s) which isn't a *net.UDPAddr, force replace to (%s), this may be caused by a wrongly implemented server", from, from, oAddrPort)
		} else if fromUDPAddr == nil {
			fromUDPAddr = net.UDPAddrFromAddrPort(oAddrPort) // oAddrPort was Unmapped
			log.Warnln("server return a nil *net.UDPAddr, force replace to (%s), this may be caused by a wrongly implemented server", oAddrPort)
		}

		fromAddrPort := fromUDPAddr.AddrPort()
		fromAddr := fromAddrPort.Addr().Unmap()

		// restore DestinationNAT
		fromAddr = sender.RestoreReadFrom(fromAddr).Unmap()

		fromAddrPort = netip.AddrPortFrom(fromAddr, fromAddrPort.Port())

		_, err = writeBack.WriteBack(data, net.UDPAddrFromAddrPort(fromAddrPort))
		if put != nil {
			put()
		}
		if err != nil {
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
