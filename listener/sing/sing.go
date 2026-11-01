package sing

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/netip"
	"sync"
	"time"

	"github.com/metacubex/mihomo/adapter/inbound"
	N "github.com/metacubex/mihomo/common/net"
	P "github.com/metacubex/mihomo/common/net/packet"
	"github.com/metacubex/mihomo/common/utils"
	C "github.com/metacubex/mihomo/constant"
	"github.com/metacubex/mihomo/log"

	"github.com/gofrs/uuid/v5"
	mux "github.com/metacubex/sing-mux"
	vmess "github.com/metacubex/sing-vmess"
	"github.com/metacubex/sing-vmess/packetaddr"
	"github.com/metacubex/sing/common"
	"github.com/metacubex/sing/common/buf"
	"github.com/metacubex/sing/common/bufio"
	"github.com/metacubex/sing/common/bufio/deadline"
	E "github.com/metacubex/sing/common/exceptions"
	M "github.com/metacubex/sing/common/metadata"
	"github.com/metacubex/sing/common/network"
	"github.com/metacubex/sing/common/uot"
)

const UDPTimeout = 5 * time.Minute
const ICMPTimeout = 10 * time.Second

type ListenerConfig struct {
	Tunnel     C.Tunnel
	Type       C.Type
	Additions  []inbound.Addition
	UDPTimeout time.Duration
	MuxOption  MuxOption
}

type MuxOption struct {
	Padding bool          `yaml:"padding" json:"padding,omitempty"`
	Brutal  BrutalOptions `yaml:"brutal" json:"brutal,omitempty"`
}

type BrutalOptions struct {
	Enabled bool   `yaml:"enabled" json:"enabled"`
	Up      string `yaml:"up" json:"up,omitempty"`
	Down    string `yaml:"down" json:"down,omitempty"`
}

type ListenerHandler struct {
	ListenerConfig
	handlerId  uuid.UUID
	muxService *mux.Service
}

func UpstreamMetadata(metadata M.Metadata) M.Metadata {
	return M.Metadata{
		Source:      metadata.Source,
		Destination: metadata.Destination,
	}
}

func ConvertMetadata(metadata *C.Metadata) M.Metadata {
	return M.Metadata{
		Protocol:    metadata.Type.String(),
		Source:      M.SocksaddrFrom(metadata.SrcIP, metadata.SrcPort),
		Destination: M.ParseSocksaddrHostPort(metadata.String(), metadata.DstPort),
	}
}

func NewListenerHandler(lc ListenerConfig) (h *ListenerHandler, err error) {
	h = &ListenerHandler{ListenerConfig: lc}
	h.handlerId = utils.NewUUIDV4()
	h.muxService, err = mux.NewService(mux.ServiceOptions{
		NewStreamContext: func(ctx context.Context, conn net.Conn) context.Context {
			return ctx
		},
		Logger:  log.SingInfoToDebugLogger, // convert sing-mux info log to debug
		Handler: h,
		Padding: lc.MuxOption.Padding,
		Brutal: mux.BrutalOptions{
			Enabled:    lc.MuxOption.Brutal.Enabled,
			SendBPS:    utils.StringToBps(lc.MuxOption.Brutal.Up),
			ReceiveBPS: utils.StringToBps(lc.MuxOption.Brutal.Down),
		},
	})
	return
}

func (h *ListenerHandler) IsSpecialFqdn(fqdn string) bool {
	switch fqdn {
	case mux.Destination.Fqdn,
		vmess.MuxDestination.Fqdn,
		uot.MagicAddress,
		uot.LegacyMagicAddress:
		return true
	default:
		return false
	}
}

func (h *ListenerHandler) ParseSpecialFqdn(ctx context.Context, conn net.Conn, metadata M.Metadata) error {
	switch metadata.Destination.Fqdn {
	case mux.Destination.Fqdn:
		return h.muxService.NewConnection(ctx, conn, UpstreamMetadata(metadata))
	case vmess.MuxDestination.Fqdn:
		return vmess.HandleMuxConnection(ctx, conn, metadata, h)
	case uot.MagicAddress:
		request, err := uot.ReadRequest(conn)
		if err != nil {
			return E.Cause(err, "read UoT request")
		}
		metadata.Destination = request.Destination
		return h.NewPacketConnection(ctx, uot.NewConn(conn, *request), metadata)
	case uot.LegacyMagicAddress:
		metadata.Destination = M.Socksaddr{Addr: netip.IPv4Unspecified()}
		return h.NewPacketConnection(ctx, uot.NewConn(conn, uot.Request{}), metadata)
	}
	return errors.New("not special fqdn")
}

func (h *ListenerHandler) NewConnection(ctx context.Context, conn net.Conn, metadata M.Metadata) error {
	if h.IsSpecialFqdn(metadata.Destination.Fqdn) {
		return h.ParseSpecialFqdn(ctx, conn, metadata)
	}

	if deadline.NeedAdditionalReadDeadline(conn) {
		conn = N.NewDeadlineConn(conn) // conn from sing should check NeedAdditionalReadDeadline
	}

	cMetadata := &C.Metadata{
		NetWork: C.TCP,
		Type:    h.Type,
	}
	if metadata.Source.IsIP() && metadata.Source.Fqdn == "" {
		cMetadata.RawSrcAddr = metadata.Source.Unwrap().TCPAddr()
	}
	if metadata.Destination.IsIP() && metadata.Destination.Fqdn == "" {
		cMetadata.RawDstAddr = metadata.Destination.Unwrap().TCPAddr()
	}
	inbound.ApplyAdditions(cMetadata, inbound.WithDstAddr(metadata.Destination), inbound.WithSrcAddr(metadata.Source), inbound.WithInAddr(conn.LocalAddr()))
	inbound.ApplyAdditions(cMetadata, h.Additions...)
	applyContextAdditions(ctx, cMetadata)

	h.Tunnel.HandleTCPConn(conn, cMetadata) // this goroutine must exit after conn unused
	return nil
}

func (h *ListenerHandler) NewPacketConnection(ctx context.Context, conn network.PacketConn, metadata M.Metadata) error {
	if metadata.Destination.Fqdn == packetaddr.SeqPacketMagicAddress {
		conn = packetaddr.NewConn(bufio.NewNetPacketConn(conn), M.Socksaddr{})
	}

	connID := utils.NewUUIDV4().String() // make a new SNAT key
	reply := &packetReply{writer: bufio.NewNetPacketWriter(conn), packetWriter: conn}
	rAddr := N.NewCustomAddr(h.Type.String(), connID, metadata.Source.UDPAddr())
	lAddr := conn.LocalAddr()
	if address := getInAddr(ctx); address != nil {
		lAddr = address
	}
	reader := P.NewReader(conn, network.ReadWaitOptions{})
	defer func() {
		_ = conn.Close() // interrupt an in-flight WriteBack before waiting for it
		reader.Close()
		reply.close()
	}()
	for {
		buff, dest, err := reader.ReadPacket()
		if err != nil {
			if ShouldIgnorePacketError(err) {
				break
			}
			return err
		}
		cPacket := &packet{
			reply: reply,
			rAddr: rAddr,
			lAddr: lAddr,
			buff:  buff,
		}
		h.handlePacket(ctx, cPacket, metadata.Source, dest)
	}
	return nil
}

type localAddr interface {
	LocalAddr() net.Addr
}

func (h *ListenerHandler) NewPacket(ctx context.Context, key netip.AddrPort, buffer *buf.Buffer, metadata M.Metadata, init func(natConn network.PacketConn) network.PacketWriter) {
	packetWriter := init(nil)
	writer := bufio.NewNetPacketWriter(packetWriter)
	cPacket := &packet{
		reply: &packetReply{writer: writer, packetWriter: packetWriter},
		rAddr: metadata.Source.UDPAddr(),
		buff:  buffer,
	}
	if h.Type != C.TUN { // make the handler-related SNAT key for not TUN listener
		connID := fmt.Sprintf("%s:%s", h.handlerId, key)
		cPacket.rAddr = N.NewCustomAddr(h.Type.String(), connID, cPacket.rAddr) // for tunnel's handleUDPConn
	}
	if conn, ok := common.Cast[localAddr](writer); ok { // tun does not have real inAddr
		cPacket.lAddr = conn.LocalAddr()
	}
	h.handlePacket(ctx, cPacket, metadata.Source, metadata.Destination)
}

func (h *ListenerHandler) handlePacket(ctx context.Context, cPacket *packet, source M.Socksaddr, destination M.Socksaddr) {
	cMetadata := &C.Metadata{
		NetWork: C.UDP,
		Type:    h.Type,
	}
	if source.IsIP() && source.Fqdn == "" {
		cMetadata.RawSrcAddr = source.Unwrap().UDPAddr()
	}
	if destination.IsIP() && destination.Fqdn == "" {
		cMetadata.RawDstAddr = destination.Unwrap().UDPAddr()
	}
	inbound.ApplyAdditions(cMetadata, inbound.WithDstAddr(destination), inbound.WithSrcAddr(source), inbound.WithInAddr(cPacket.InAddr()))
	inbound.ApplyAdditions(cMetadata, h.Additions...)
	applyContextAdditions(ctx, cMetadata)

	h.Tunnel.HandleUDPPacket(cPacket, cMetadata)
}

func (h *ListenerHandler) NewError(ctx context.Context, err error) {
	log.Warnln("%s listener get error: %+v", h.Type.String(), err)
}

func (h *ListenerHandler) TypeMutation(typ C.Type) *ListenerHandler {
	handler := *h
	handler.Type = typ
	return &handler
}

func ShouldIgnorePacketError(err error) bool {
	// ignore simple error
	if E.IsTimeout(err) || E.IsClosed(err) || E.IsCanceled(err) {
		return true
	}
	return false
}

type packet struct {
	reply *packetReply
	rAddr net.Addr
	lAddr net.Addr
	buff  *buf.Buffer
}

func (c *packet) Data() []byte {
	if c.buff == nil {
		return nil
	}
	return c.buff.Bytes()
}

func (c *packet) TakeBuffer() *buf.Buffer {
	buffer := c.buff
	c.buff = nil
	return buffer
}

// WriteBack wirtes UDP packet with source(ip, port) = `addr`
func (c *packet) WriteBack(b []byte, addr net.Addr) (n int, err error) {
	return c.reply.WriteBack(b, addr)
}

func (c *packet) WriteBackTarget() C.WriteBack { return c.reply }

// packetReply outlives any one payload/metadata pair. NAT stores this handle,
// while each packet owns only its payload and per-packet routing metadata.
type packetReply struct {
	mutex        sync.Mutex
	writer       network.NetPacketWriter
	packetWriter network.PacketWriter
}

func (c *packetReply) close() {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	c.writer, c.packetWriter = nil, nil
}

func (c *packetReply) WriteBack(b []byte, addr net.Addr) (n int, err error) {
	if addr == nil {
		err = errors.New("address is invalid")
		return
	}

	c.mutex.Lock()
	defer c.mutex.Unlock()
	conn := c.writer
	if conn == nil {
		err = errors.New("writeBack to closed connection")
		return
	}

	return conn.WriteTo(b, addr)
}

func (c *packetReply) WriteBackBuffer(buffer *buf.Buffer, destination M.Socksaddr) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	if c.packetWriter == nil {
		buffer.Release()
		return net.ErrClosed
	}
	buffer = network.NewReadWaitOptions(nil, c.packetWriter).Copy(buffer)
	return c.packetWriter.WritePacket(buffer, destination)
}

// LocalAddr returns the source IP/Port of UDP Packet
func (c *packet) LocalAddr() net.Addr {
	return c.rAddr
}

func (c *packet) Drop() {
	c.buff.Release()
	c.buff = nil
}

func (c *packet) InAddr() net.Addr {
	return c.lAddr
}
