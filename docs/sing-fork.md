# sing fork integration

`github.com/metacubex/sing` is replaced with the fixed Herta-Network revision
`a402b0b490ba3514b46e242c0630420da7fec151` in `go.mod`. This also selects the fork
for MetaCubeX protocol dependencies that import that module path. The cronet-go and sing-snell forks use this same module path. Their public
package paths remain unchanged, but their sources are selected with fixed fork
replacements. There is no separate `github.com/sagernet/sing` in the production
module graph, including `with_naive` builds.
No local checkout or moving branch is needed to build mihomo.

| Module identity | Fixed source commit |
| --- | --- |
| `github.com/metacubex/sing` | Herta-Network/sing `a402b0b490ba` |
| `github.com/sagernet/cronet-go` | Herta-Network/cronet-go `511fbbf40a07` |
| `github.com/sagernet/sing-snell` | Herta-Network/sing-snell `42f9d87468f7` |

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

Snell v3, v4/v5 and all three v6 modes expose counted packet writes through the
outbound, deadline, reference and traffic wrappers. Each UDP datagram retains its
own encrypted record. Writes group at most 64 records and translate accepted
ciphertext bytes into complete packet counts; incomplete records do not count.
HTTP/TLS obfuscation preserves payload progress and rejects short handshakes.
Scalar and batch writes share the connection's lock and terminal error state;
closing interrupts blocked writes. Counts describe local write completion, not
peer delivery. Payload validation leaves the nonce unchanged when no record was
sent. Other protocol adapters still need their own explicit counted capability.

Datagram receive capacity is independent of low-memory stream buffers. Generic
packet connections allow a full UDP datagram, and legacy Snell reads reserve
its full protocol record size, preventing low-memory truncation.

## Regression checks

```sh
SKIP_INTEROP_TEST=1 go test -race \
  ./common/net/... ./listener/sing ./listener/sing_shadowsocks ./listener/inbound ./tunnel/... ./transport/snell ./transport/simple-obfs ./transport/shadowsocks/shadowaead \
  -run 'TestRelay|TestPacket|TestSingPacketIntegration|TestInboundSnellBatch|TestTLSPayloadCount' -count=1 -timeout=90s

SKIP_INTEROP_TEST=1 go test -tags with_low_memory \
  ./common/net/... ./listener/sing ./listener/sing_shadowsocks ./listener/inbound ./tunnel/... ./transport/snell ./transport/simple-obfs ./transport/shadowsocks/shadowaead \
  -run 'TestRelay|TestPacket|TestSingPacketIntegration|TestInboundSnellBatch|TestTLSPayloadCount' -count=1 -timeout=90s
```

These cover TCP half-close/counters/protocol wrappers, empty and 60 KB native
UDP datagrams, packet order across read APIs and reference wrappers, buffer
headroom/ownership, late deadline results, blocked reply shutdown, and loopback
Shadowsocks AEAD/2022 plus UoT v1/v2 with traffic counters. Snell regressions cover
v3-v6 batch capability through the production wrappers, empty packets, IPv4/IPv6,
HTTP/TLS obfuscation, v6 modes, partial progress, 64-record boundaries and blocked
write shutdown. The fork also tests both UDP directions and nonce preservation
after validation errors. The Test workflow runs
these on Linux, macOS and Windows before the existing macOS inbound-test
exclusion, and also supports the `Meta` branch and manual dispatch.

Cross-compilation alone does not exercise Linux splice, Windows socket I/O,
real-device VPN behavior or production proxy endpoints.

The wider Snell race suite still exposes races inside `restls-client-go` v0.1.9;
`TestInboundSnell_Restls` reproduces them on the pre-batch baseline `93ceeef5`.
The wider Shadowsocks cipher matrix also reproduces HTTP/2 SETTINGS timeouts
and connection resets on that baseline; lowering test parallelism does not
eliminate every failure. The focused checks above pass, but the broader suite
is not clean.
This does not establish a clean race result for Restls or official-server
interoperability; the batch checks above use local Go servers.

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
