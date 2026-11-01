package snell

import (
	"context"
	"errors"
	"io"
	"net"

	C "github.com/metacubex/mihomo/constant"
	"github.com/metacubex/mihomo/listener/sing"
	"github.com/metacubex/mihomo/transport/snell"

	M "github.com/metacubex/sing/common/metadata"
	SN "github.com/metacubex/sing/common/network"
	SS "github.com/sagernet/sing-snell"
	"github.com/sagernet/sing-snell/snellv5"
	"github.com/sagernet/sing-snell/snellv6"
)

func (l *Listener) initSingSnell(tunnel C.Tunnel) (err error) {
	l.handler, err = sing.NewListenerHandler(sing.ListenerConfig{Tunnel: tunnel, Type: C.SNELL})
	if err != nil {
		return err
	}
	handler := singHandler{handler: l.handler, udp: l.config.UDP}
	if l.config.Version == snell.Version6 {
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
	handler *sing.ListenerHandler
	udp     bool
}

func (h singHandler) NewConnectionEx(ctx context.Context, conn net.Conn, source, destination M.Socksaddr, onClose SN.CloseHandlerFunc) {
	err := h.handler.NewConnection(ctx, conn, M.Metadata{Source: source, Destination: destination})
	_ = conn.Close()
	if onClose != nil {
		onClose(err)
	}
	if err != nil {
		h.handler.NewError(ctx, err)
	}
}

func (h singHandler) NewPacketConnectionEx(ctx context.Context, conn SN.PacketConn, source, destination M.Socksaddr, onClose SN.CloseHandlerFunc) {
	var err error
	if h.udp {
		err = h.handler.NewPacketConnection(ctx, conn, M.Metadata{Source: source, Destination: destination})
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

var _ SS.Handler = singHandler{}
