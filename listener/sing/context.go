package sing

import (
	"context"
	"net"

	"github.com/metacubex/mihomo/adapter/inbound"
	C "github.com/metacubex/mihomo/constant"

	"github.com/metacubex/sing/common/auth"
	"golang.org/x/exp/slices"
)

type contextKey string

// Keep boxed keys once rather than boxing a string for each Context.Value call.
var ctxKeyAdditions any = contextKey("Additions")

func WithAdditions(ctx context.Context, additions ...inbound.Addition) context.Context {
	return context.WithValue(ctx, ctxKeyAdditions, additions)
}

func AdditionsFromContext(ctx context.Context) []inbound.Addition { return getAdditions(ctx) }

func getAdditions(ctx context.Context) (additions []inbound.Addition) {
	if v := ctx.Value(ctxKeyAdditions); v != nil {
		if a, ok := v.([]inbound.Addition); ok {
			additions = a
		}
	}
	if user, ok := auth.UserFromContext[string](ctx); ok {
		additions = slices.Clip(additions) // force the subsequent `append()` to copy the slice
		additions = append(additions, inbound.WithInUser(user))
	}
	return
}

func applyContextAdditions(ctx context.Context, metadata *C.Metadata) {
	if additions, ok := ctx.Value(ctxKeyAdditions).([]inbound.Addition); ok {
		inbound.ApplyAdditions(metadata, additions...)
	}
	if user, ok := auth.UserFromContext[string](ctx); ok {
		metadata.InUser = user
	}
}

var ctxKeyInAddr any = contextKey("InAddr")

func WithInAddr(ctx context.Context, inAddr net.Addr) context.Context {
	return context.WithValue(ctx, ctxKeyInAddr, inAddr)
}

func getInAddr(ctx context.Context) net.Addr {
	if v := ctx.Value(ctxKeyInAddr); v != nil {
		if a, ok := v.(net.Addr); ok {
			return a
		}
	}
	return nil
}
