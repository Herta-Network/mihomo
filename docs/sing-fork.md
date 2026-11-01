# sing fork integration

`github.com/metacubex/sing` is replaced with the fixed Herta-Network revision
`6ff3643691ad28375809c4ccc03e726b8ad281f3` in `go.mod`. This also selects the fork
for MetaCubeX protocol dependencies that import that module path. The cronet-go and sing-snell forks use this same module path. Their public
package paths remain unchanged, but their sources are selected with fixed fork
replacements. There is no separate `github.com/sagernet/sing` in the production
module graph, including `with_naive` builds.
No local checkout or moving branch is needed to build mihomo.

| Module identity | Fixed source commit |
| --- | --- |
| `github.com/metacubex/sing` | Herta-Network/sing `6ff3643691ad` |
| `github.com/sagernet/cronet-go` | Herta-Network/cronet-go `511fbbf40a07` |
| `github.com/sagernet/sing-snell` | Herta-Network/sing-snell `6a642f127a54` |

## Data paths

TCP Relay uses `CopyWithOptions`. Its read policy requests buffers up to 32 KiB and
batches to four buffers at low concurrency. Above four active directions per
Go scheduler processor (threshold clamped to 4–64), it uses one 16 KiB buffer.
`with_low_memory` builds always request one 16 KiB buffer. Required protocol headroom
can exceed these requested caps. The sing copy engine still
owns protocol framing, handshake reporting, cached bytes and counter callbacks.
Raw socket pairs on Linux retain splice and use the same policy to bound each
transfer size; setting a buffered read policy there would disable splice.

UDP socket reads, sing packet connections and the sing Shadowsocks listener use
`PacketBatchReadWaiter` / `ConnectedPacketBatchReadWaiter` when available. Reads
request at most four packets; `with_low_memory` uses scalar reads. Readers preserve
an advertised MTU or allow a full UDP datagram when no MTU is supplied. This
bounds prefetch, but is not a measured reduction in memory use or a throughput
claim. Unsupported platforms and protocol wrappers keep their scalar read path.

One reader owns pending packets across `ReadFrom`, `ReadPacket` and
`WaitReadFrom`. A packet handed to the tunnel remains owned by its consumer;
closing releases only pending packets and waiter scratch storage. Ref and
thread-safe wrappers preserve this shared reader, and read unwrapping stops at
pending-packet owners. The deadline wrapper also releases cached and late
results after close. Listener teardown closes the transport before waiting for
an in-flight reply writer.

## Boundaries

mihomo retains its routing, sniffing, destination mapping and NAT ownership.
The fork's existing `udpnat` implementation is selected through the dependency;
`udpnat2` is not inserted as a second NAT layer.

Uploads drain at most four packets already queued for the same connection;
they do not wait for more packets. Isolated packets, `with_low_memory` builds,
and wrappers without an explicit progress capability use their scalar write
path. Pending packets are flushed before resolving a new destination.

`PacketBatchWriterWithCount.WritePacketBatchCount` reports the number of
completely written packets at the start of the batch, including when a later
write fails. The tracker snapshots plaintext payload sizes before ownership
transfer and counts only that successful prefix. Empty datagrams advance the
packet count and refresh the timeout even though their byte count is zero.
All buffers are consumed on success and failure; failed batches are not
replayed. Send/Close synchronization prevents enqueueing after the final drain.

Native UDP uses the kernel's packet progress and requires a datagram socket;
TCP sockets are never advertised as native packet batch writers. UoT uses a counted vector writer,
subtracts the lazy request header, and maps accepted stream bytes to complete
frames. A partial frame closes and poisons the stream. Ref, thread-safe,
deadline, NAT and traffic wrappers preserve this capability. Error-only batch
APIs remain available for compatibility, while copying with progress falls
back to scalar packet writes when exact batch progress is unavailable.

Datagram receive capacity is independent of low-memory stream buffers. Generic
packet connections allow a full UDP datagram, and legacy Snell reads reserve
its full protocol record size, preventing low-memory truncation.

## Regression checks

```sh
SKIP_INTEROP_TEST=1 go test -race \
  ./common/net/... ./listener/sing ./listener/sing_shadowsocks ./listener/inbound ./tunnel/... ./transport/snell \
  -run 'TestRelay|TestPacket|TestSingPacketIntegration' -count=1 -timeout=90s

SKIP_INTEROP_TEST=1 go test -tags with_low_memory \
  ./common/net/... ./listener/sing ./listener/sing_shadowsocks ./listener/inbound ./tunnel/... ./transport/snell \
  -run 'TestRelay|TestPacket|TestSingPacketIntegration' -count=1 -timeout=90s
```

These cover TCP half-close/counters/protocol wrappers, empty and 60 KB native
UDP datagrams, packet order across read APIs and reference wrappers, buffer
headroom/ownership, late deadline results, blocked reply shutdown, and loopback
Shadowsocks AEAD/2022 plus UoT v1/v2 with traffic counters. The Test workflow runs
these on Linux, macOS and Windows before the existing macOS inbound-test
exclusion, and also supports the `Meta` branch and manual dispatch.

Cross-compilation alone does not exercise Linux splice, Windows socket I/O,
real-device VPN behavior or production proxy endpoints.

## One sing source

cronet-go's `all` package is included in its root module; the old separate
`cronet-go/all` module requirement is removed. Native library artifacts retain
the previously validated revision. `scripts/build-naive.sh` discovers the
selected native module from the platform imports instead of assuming a
repository owner.

Snell now hands the shared packet interface and metadata directly to the sing
listener. Naive likewise uses the shared address types and buffer writer;
its native interoperability tests exercise controlled copying through the
padding transition over both HTTP/2 and QUIC.

Run `python3 scripts/check-sing-source.py` to verify both the module graph and
all production package imports with Naive enabled. CI runs the same guard.
The separate `test/` module contains independent reference clients for
interoperability testing and is not linked into the production executable.
