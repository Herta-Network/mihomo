# sing fork integration

`github.com/metacubex/sing` is replaced with the fixed Herta-Network revision
`1d1b6994cf4aac99fe90d39831b8525233d744a1` in `go.mod`. This also selects the fork
for MetaCubeX protocol dependencies that import that module path. The cronet-go, sing-snell, sing-vmess and sing-mux forks use this same module path. Their public
package paths remain unchanged, but their sources are selected with fixed fork
replacements. There is no separate `github.com/sagernet/sing` in the production
module graph, including `with_naive` builds.
No local checkout or moving branch is needed to build mihomo.

| Module identity | Fixed source commit |
| --- | --- |
| `github.com/metacubex/sing` | Herta-Network/sing `1d1b6994cf4a` |
| `github.com/sagernet/cronet-go` | Herta-Network/cronet-go `511fbbf40a07` |
| `github.com/sagernet/sing-snell` | Herta-Network/sing-snell `1fa24ee9ea84` |
| `github.com/metacubex/sing-vmess` | Herta-Network/sing-vmess `83a9fdf3e38e` |
| `github.com/metacubex/sing-mux` | Herta-Network/sing-mux `9a83d0dd7caa` |

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

`CachedPacketConn` exposes an owned read waiter while retaining ownership of
the handshake-prefetched packet. It serves that packet first, including empty
datagrams, then hands through the underlying owned buffers. Scalar/cached/waiter
reads share state, and teardown interrupts reads before releasing waiter storage.
The shared counted vector writer retains its descriptors across writes; joined
fallbacks use pooled bytes without a temporary `Buffer` object.

## Boundaries

mihomo retains its routing, sniffing, destination mapping and NAT ownership.
The fork's existing `udpnat` implementation is selected through the dependency;
`udpnat2` is not inserted as a second NAT layer.

Uploads drain at most four packets already queued for the same connection;
they do not wait for more packets. Sing and legacy Snell inbounds transfer owned
buffers through `PacketBuffer.TakeBuffer`, retaining their headroom. Single
owned packets also use counted writes, including in `with_low_memory` builds,
without collecting additional packets. Borrowed isolated packets and wrappers
without an explicit progress capability retain their scalar write path.
Pending packets are flushed before resolving a new destination.
The per-connection sender uses a bounded 128-slot deque ring and coalesces worker
wakeups. FIFO order, full-queue drops and Close/Send ownership remain unchanged.
Deadline refreshes reuse their timer; closing the owner prevents timer rearming.

Downstream `ReadPacketBuffer` explicitly crosses the packet, deadline, reference,
outbound and traffic wrappers. It preserves cached packets, counts plaintext
download once, restores NAT source addresses as values and transfers buffers to
`WriteBackBuffer`. Sing and legacy Snell reply endpoints retain usable headroom;
legacy readers/writers keep their own API with controlled copy/release fallback.
Deadline result types are interchangeable after timeout, and read admission is
serialized across APIs to avoid stealing a cached result.

The NAT reply proxy stores a durable reply endpoint and optional default-source
value, independently of the packet adapter and metadata. Flow setup snapshots
metadata before asynchronous dialing. Packet `Drop` releases only its payload;
the reply handle remains valid and preserves a legacy nil-address fallback where
provided. Context additions retain their override order without a per-packet
list allocation. Packet/metadata envelopes are not globally pooled.

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
sent.

The Snell fork reuses address and record-header workspaces, packet encoders,
counted sinks and scalar-call descriptor arrays. Packet encryption reuses the
consumed descriptor slice, and padding scratch uses pooled bytes. These changes
preserve the original nonce, record-window, shaping and partial-write rules.

## Protocol capability coverage

Every UDP transport adapter now exposes a counted capability or preserves the
capability of its chosen backend. The operation differs by transport; exposing
packet progress does not imply a kernel batch or a throughput improvement.

| Adapters | Send operation |
| --- | --- |
| DIRECT / standard unconnected UDP | Native packet batching where available; atomic scalar packet fallback on other platforms. The loopback close callback preserves the capability. |
| Shadowsocks none/stream/AEAD/2022, SOCKS5, SSR | Run the existing address/protocol/cipher encoder, then batch the resulting wire datagrams through the underlying counted packet transport. An opaque proxy-chain transport retains scalar writes. |
| Snell v3-v6 | Count complete encrypted records, including v6 shaping and obfuscation. |
| Trojan, VLESS, VMess, XUDP, packetaddr, Gost Relay, Sudoku, TrustTunnel, Mieru | Encode complete logical datagrams and join stream writes. Progress includes lazy headers and excludes any incomplete packet. Mieru retains its SDK packet-over-stream framing. |
| sing-mux bound and full-cone UDP | Native framed batches over h2mux/smux/yamux; both client and server expose progress. |
| Shadowsocks UoT, AnyTLS, Naive UoT, TUIC UoT | Preserve the existing counted UoT path. |
| Hysteria, Hysteria2, TUIC native/QUIC packet modes, ShadowQUIC, VLESS Reverse | Count successful calls through the original protocol engine. QUIC fragmentation, channel ownership and routing remain in that engine; this is sequential packet submission, not a raw-socket shortcut. |
| WireGuard, Tailscale, EasyTier, MASQUE, OpenVPN, ZeroTier | Count the existing network stack's packet operations. ZeroTier's destination/stack validation runs for every packet. |

DNS hijacking, rejection and rematch adapters are special local actions, not
network transports. Unknown wrappers still require an explicit capability;
they are not unwrapped to guess whether their underlying socket can batch.

All write adapters retain the original read waiter and ownership of prefetched
packets. Wrapping a userspace stack preserves its native packet deadline path.
Stream failures close and poison the logical connection; independent
QUIC/datagram operations retain their protocol's error behavior. TUIC v4/v5
close state is atomic, and Close no longer mutates the reader pointer while
another goroutine can be using it.

The VMess fork removes the TCP-only 15 KB chunk limiter from UDP writes while
preserving cipher, nonce, mask and padding layers. XUDP now forwards empty UDP
frames. Plaintext VMess UDP still rejects empty payloads because a zero-length
chunk is a termination marker. Size/address checks run before advancing the
encoder; bound associations reject a different destination. sing-mux includes
an actual zero-length packet after its first request header and returns bound
source addresses from packet reads.

Trojan now carries a full uint16-sized datagram in one frame instead of splitting
it at 8 KiB. Its receive buffer follows the advertised packet length, and a short
ReadFrom buffer discards the remainder of that datagram. Mieru distinguishes an
empty decoded UDP payload from EOF before the next stream frame. HTTP,
WebSocket/early-data, VLESS request and GUN framing preserve payload write counts
when a lower write fails or returns a short success.

The [Snell performance report](benchmarks/snell-2026-11-01.md) compares the
committed Snell implementation with its parent using actual local packet traffic.
It records throughput and allocation observations; its v6 timings used varying
test PSKs and are not controlled speedup evidence. It does not claim a general
memory reduction or measure all of the adapters above.
The subsequent [UDP allocation report](benchmarks/udp-allocation-2026-11-01.md)
measures owned-buffer transfer, in-place Snell v3 records and reused counted
vector storage. It distinguishes zero-allocation steady stages from the full
loopback path, which still allocates.
The [owned downstream report](benchmarks/owned-downlink-2026-11-01.md) measures
the shared fork changes, cached-reader ownership and independent reply handles,
including real request/reply loopbacks and an isolated downstream-loop benchmark.

Datagram receive capacity is independent of low-memory stream buffers. Generic
packet connections allow a full UDP datagram, and legacy Snell reads reserve
its full protocol record size, preventing low-memory truncation.

## Protocol framing allocation follow-up

UoT advertises front headroom and prepends its header directly to owned packets.
Batches without sufficient headroom retain separate header/payload vectors so
an opaque joined writer copies each payload once. Scalar writes use the counted
writer and retain partial-frame poisoning and complete-prefix accounting.

Sing-mux caches its counted writer and single-packet workspace under its write
mutex. Address serialization writes directly into header storage. VMess's length
masking reuses its two-byte scratch; XUDP and packetaddr avoid temporary header
descriptors, and the CFB checksum layer preserves the owned `WriteBuffer` path.
Rejected packets release their input. Packetaddr's borrowed API reports payload
bytes, excluding its address header.

The [protocol framing report](benchmarks/protocol-framing-2026-11-01.md) includes
producer allocation and real TCP round trips. Normal 1200-byte encoding paths
retain one producer buffer allocation per packet; large XUDP frames still need
chunk workspaces, and generic opaque batch joining still copies output bytes.
[Configuration and GEO concurrency](concurrency-lifecycle.md) describes the
separate lifecycle fixes and their regression coverage.

## Regression checks

```sh
SKIP_INTEROP_TEST=1 go test -race \
  ./common/net/... ./common/callback ./adapter/outbound ./listener/sing ./listener/sing_shadowsocks ./listener/inbound ./tunnel/... ./transport/snell ./transport/trojan ./transport/vless ./transport/vless/reverse ./transport/vmess ./transport/gost ./transport/gun ./transport/sudoku ./transport/trusttunnel ./transport/simple-obfs ./transport/shadowsocks/shadowaead \
  -run 'TestRelay|TestPacket|TestProtocolPacket|TestSingPacketIntegration|TestInboundSnellBatch|TestTLSPayloadCount|TestReverse' -count=1 -timeout=90s

SKIP_INTEROP_TEST=1 go test -tags with_low_memory \
  ./common/net/... ./common/callback ./adapter/outbound ./listener/sing ./listener/sing_shadowsocks ./listener/inbound ./tunnel/... ./transport/snell ./transport/trojan ./transport/vless ./transport/vless/reverse ./transport/vmess ./transport/gost ./transport/gun ./transport/sudoku ./transport/trusttunnel ./transport/simple-obfs ./transport/shadowsocks/shadowaead \
  -run 'TestRelay|TestPacket|TestProtocolPacket|TestSingPacketIntegration|TestInboundSnellBatch|TestTLSPayloadCount|TestReverse' -count=1 -timeout=90s
```

These cover TCP half-close/counters/protocol wrappers, empty and 60 KB native
UDP datagrams, packet order across read APIs and reference wrappers, buffer
headroom/ownership, late deadline results, blocked reply shutdown, and loopback
Shadowsocks AEAD/2022 plus UoT v1/v2 with traffic counters. Snell regressions cover
v3-v6 batch capability through the production wrappers, empty packets, IPv4/IPv6,
HTTP/TLS obfuscation, v6 modes, partial progress, 64-record boundaries and blocked
write shutdown. The fork also tests both UDP directions and nonce preservation
after validation errors. Additional cases exercise SOCKS5/SSR framing, VMess and
VLESS plain/XUDP/packetaddr, WebSocket/early-data/gRPC carriers, sing-mux's three
transports, Sudoku, TrustTunnel h2/QUIC, Hysteria2, TUIC v4/v5 and concurrent QUIC
write/close. Mieru is tested against its SDK framing, without a live remote Mieru
service. Virtual VPN/mesh stacks and ShadowQUIC/Hysteria v1 have interface and
build coverage, not a newly performed live-network end-to-end run.
The Test workflow runs
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
