package snell

import (
	"context"
	"errors"
	"io"
	"net"

	C "github.com/metacubex/mihomo/constant"
	"github.com/metacubex/mihomo/listener/sing"
	"github.com/metacubex/mihomo/transport/snell"

	"github.com/metacubex/sing/common/buf"
	"github.com/metacubex/sing/common/bufio"
	M "github.com/metacubex/sing/common/metadata"
	MN "github.com/metacubex/sing/common/network"
	SS "github.com/sagernet/sing-snell"
	"github.com/sagernet/sing-snell/snellv5"
	"github.com/sagernet/sing-snell/snellv6"
	SB "github.com/sagernet/sing/common/bufio"
	SM "github.com/sagernet/sing/common/metadata"
	SN "github.com/sagernet/sing/common/network"
)

func (l *Listener) initSingSnell(tunnel C.Tunnel) (err error) {
	l.handler, err = sing.NewListenerHandler(sing.ListenerConfig{Tunnel: tunnel, Type: C.SNELL})
	if err != nil {
		return err
	}
	handler := singHandler{handler: l.handler, udp: l.config.UDP, packetSize: SS.MaxPayloadLen}
	if l.config.Version == snell.Version6 {
		handler.packetSize = 65535
		mode, err := snellv6.ParseMode(l.config.Version6Mode)
		if err != nil {
			return err
		}
		l.service, err = snellv6.NewService(snellv6.ServerOptions{
			PSK: []byte(l.config.Psk), Mode: mode, Handler: handler,
		})
		return err
	}
	var obfsMode SS.ObfsMode
	switch l.config.ObfsMode {
	case "http":
		obfsMode = SS.ObfsModeHTTP
	case "tls":
		obfsMode = SS.ObfsModeTLS
	}
	// Snell v4 and v5 use the same TCP record transport.
	l.service, err = snellv5.NewService(snellv5.ServiceOptions{
		PSK: []byte(l.config.Psk), ObfsMode: obfsMode, Handler: handler,
	})
	return err
}

type singHandler struct {
	handler    *sing.ListenerHandler
	udp        bool
	packetSize int
}

func (h singHandler) NewConnectionEx(ctx context.Context, conn net.Conn, source, destination SM.Socksaddr, onClose SN.CloseHandlerFunc) {
	err := h.handler.NewConnection(ctx, conn, convertMetadata(source, destination))
	_ = conn.Close()
	if onClose != nil {
		onClose(err)
	}
	if err != nil {
		h.handler.NewError(ctx, err)
	}
}

func (h singHandler) NewPacketConnectionEx(ctx context.Context, conn SN.PacketConn, source, destination SM.Socksaddr, onClose SN.CloseHandlerFunc) {
	var err error
	if h.udp {
		// The two sing packages use different buffer/address types. Bridge their
		// packet interfaces through net.PacketConn rather than sharing buffers.
		packetConn := &singPacketConn{NetPacketConn: bufio.NewPacketConn(SB.NewNetPacketConn(conn)), packetSize: h.packetSize}
		err = h.handler.NewPacketConnection(ctx, packetConn, convertMetadata(source, destination))
	} else {
		err = errors.New("snell UDP is disabled")
	}
	_ = conn.Close()
	if onClose != nil {
		onClose(err)
	}
	if err != nil && !isClosedError(err) {
		h.handler.NewError(ctx, err)
	}
}

func isClosedError(err error) bool {
	var timeout net.Error
	return sing.ShouldIgnorePacketError(err) || errors.Is(err, io.EOF) ||
		(errors.As(err, &timeout) && timeout.Timeout())
}

// The generic mihomo packet reader allocates 16 KiB. Snell v6 datagrams can
// exceed that size; reserve a full datagram before crossing net.PacketConn.
type singPacketConn struct {
	MN.NetPacketConn
	readWaitOptions MN.ReadWaitOptions
	packetSize      int
}

func (c *singPacketConn) InitializeReadWaiter(options MN.ReadWaitOptions) bool {
	options.MTU = c.packetSize
	c.readWaitOptions = options
	return false
}

func (c *singPacketConn) WaitReadPacket() (*buf.Buffer, M.Socksaddr, error) {
	buffer := c.readWaitOptions.NewPacketBuffer()
	destination, err := c.ReadPacket(buffer)
	if err != nil {
		buffer.Release()
		return nil, M.Socksaddr{}, err
	}
	c.readWaitOptions.PostReturn(buffer)
	return buffer, destination, nil
}

func convertMetadata(source, destination SM.Socksaddr) M.Metadata {
	return M.Metadata{
		Source:      M.Socksaddr{Addr: source.Addr, Port: source.Port, Fqdn: source.Fqdn},
		Destination: M.Socksaddr{Addr: destination.Addr, Port: destination.Port, Fqdn: destination.Fqdn},
	}
}

var _ SS.Handler = singHandler{}
