package sing

import (
	"context"
	"testing"

	"github.com/metacubex/mihomo/adapter/inbound"
	C "github.com/metacubex/mihomo/constant"
	"github.com/metacubex/sing/common/auth"
)

func TestPacketContextAdditionsPreserveOrderWithoutAllocation(t *testing.T) {
	additions := []inbound.Addition{inbound.WithInName("context"), inbound.WithInUser("configured")}
	ctx := WithAdditions(context.Background(), additions...)
	ctx = auth.ContextWithUser(ctx, "authenticated")
	metadata := &C.Metadata{}
	if allocations := testing.AllocsPerRun(100, func() {
		*metadata = C.Metadata{InName: "listener", InUser: "listener"}
		applyContextAdditions(ctx, metadata)
	}); allocations != 0 {
		t.Fatalf("per-packet context allocations=%g", allocations)
	}
	if metadata.InName != "context" || metadata.InUser != "authenticated" {
		t.Fatalf("context precedence changed: %+v", metadata)
	}
	*metadata = C.Metadata{}
	inbound.ApplyAdditions(metadata, additions...)
	if metadata.InUser != "configured" {
		t.Fatal("context additions mutated the caller's list")
	}
}
