# sing fork integration

`github.com/metacubex/sing` is replaced with the fixed Herta-Network revision
`d9b3220a312b269d4b2bf435fccb8bd232364ffb` in `go.mod`. This also selects the fork
for MetaCubeX protocol dependencies that import that module path. The cronet-go, sing-snell, sing-vmess, sing-mux, smux and sing-quic forks use this
same module path. Their public
package paths remain unchanged, but their sources are selected with fixed fork
replacements. There is no separate `github.com/sagernet/sing` in the production
module graph, including `with_naive` builds.
No local checkout or moving branch is needed to build the pinned dependency graph.
The [mechanism review](benchmarks/mechanism-review-2026-11-01.md) documents
the retained fixes, measured tradeoffs and removed complexity. The fixed
versions below include those published simplifications; ordinary builds no
longer need the development workspace used during measurement.

| Module identity | Fixed source commit |
| --- | --- |
| `github.com/metacubex/sing` | Herta-Network/sing `d9b3220a312b` |
| `github.com/sagernet/cronet-go` | Herta-Network/cronet-go `511fbbf40a07` |
| `github.com/sagernet/sing-snell` | Herta-Network/sing-snell `75ebe9d28505` |
| `github.com/metacubex/sing-vmess` | Herta-Network/sing-vmess `7cef310d10c4` |
| `github.com/metacubex/sing-mux` | Herta-Network/sing-mux `aacdbf4dd064` |
| `github.com/metacubex/smux` | Herta-Network/smux `175a5e6183d0` |
| `github.com/metacubex/sing-quic` | Herta-Network/sing-quic `6fa503843736` |

## Data paths

TCP relay uses `CopyWithOptions` and preserves the native copy path for
replaceable raw socket pairs, including counter wrappers. Normal protocol
wrappers use checked single-buffer reads up to 32 KiB. Concurrency does not
shrink that buffer or enable speculative vector read-ahead: full-process tests
showed throughput regression from the former adaptive 16 KiB/four-buffer policy.
`with_low_memory` still requests one 16 KiB buffer. Required protocol headroom
can exceed the requested cap. Linux raw socket pairs retain splice, with its
native transfer cap in normal builds and a 16 KiB cap in low-memory builds.
The copy engine continues to own protocol framing, cached bytes, handshake
reporting and traffic counters.

UDP socket reads, sing packet connections and the sing Shadowsocks listener use
bounded four-packet read batches when the source provides a batch waiter.
Complete-chain ablation found about 7% lower CPU on the smux burst workload;
the same benefit was not established for every protocol. Low-memory builds keep
scalar reads. An advertised MTU or full UDP datagram capacity is preserved.

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
The reviewed counted vector writer uses local descriptors, retaining exact
partial-write counts without a shared descriptor cache or additional mutex.
Joined fallbacks use pooled bytes without a temporary `Buffer` object.

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
The per-connection sender uses a bounded 128-slot channel. FIFO order,
full-queue drops and Close/Send ownership are unchanged. Deadline refreshes
stop and drain the old timer before creating a new one; closing the owner
prevents rearming. The deque/wakeup layer and retained-timer state were removed.

The downstream loop uses `WaitReadFrom`/`WriteBack` and releases borrowed data
after each write, including errors and short success. It counts plaintext
download once and restores NAT source addresses before replying. The extra
owned-buffer forwarding interfaces and their reply wrappers were removed.
Deadline result types remain
interchangeable after timeout, and read admission is serialized across APIs to
avoid stealing a cached result.

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
replayed. `WriteBack` reports application payload bytes, excluding protocol
headers. SOCKS5, legacy Shadowsocks and Mieru normalize complete framed writes
to that payload count so a successfully sent reply does not close its NAT flow. Send/Close synchronization prevents enqueueing after the final drain.

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

The reviewed Snell packet writer creates local address storage, counted sinks
and record encoders for each operation. It no longer caches these adapters or
single-packet descriptor arrays in the connection. Packet encryption still
preserves nonce, record-window, shaping and partial-write rules.

## Protocol capability coverage

Adapters expose a counted capability when their protocol or backend implements
it. Other transports retain scalar writes; no generic wrapper advertises a
sequential loop as a new batch capability. Exact progress does not imply a
kernel batch or a throughput improvement.

ShadowQUIC UDP-over-stream and VLESS Reverse retain a scalar terminal-error
guard: a failed or incomplete datagram closes the stream and rejects later
writes. It preserves the original read waiter and does not advertise batching;
Close can interrupt a blocked write without taking the writer mutex.

| Adapters | Send operation |
| --- | --- |
| DIRECT / standard unconnected UDP | Native packet batching where available; atomic scalar packet fallback on other platforms. The loopback close callback preserves the capability. |
| Shadowsocks none/stream/AEAD/2022, SOCKS5, SSR | Run the existing address/protocol/cipher encoder, then batch the resulting wire datagrams through the underlying counted packet transport. An opaque proxy-chain transport retains scalar writes. |
| Snell v3-v6 | Count complete encrypted records, including v6 shaping and obfuscation. |
| Trojan, VLESS, VMess, XUDP, packetaddr, Gost Relay, Sudoku, TrustTunnel, Mieru | Encode complete logical datagrams and join stream writes. Progress includes lazy headers and excludes any incomplete packet. Mieru retains its SDK packet-over-stream framing. |
| sing-mux bound and full-cone UDP | Native framed batches over h2mux/smux/yamux; both client and server expose progress. |
| Shadowsocks UoT, AnyTLS, Naive UoT, TUIC UoT | Preserve the existing counted UoT path. |
| Hysteria, Hysteria2, TUIC native/QUIC packet modes, ShadowQUIC, VLESS Reverse | Use the original scalar protocol API unless the backend already provides counted writes. Fragmentation, channel ownership, routing and errors remain in that engine. |
| WireGuard, Tailscale, EasyTier, MASQUE, OpenVPN, ZeroTier | Use the existing network stack's scalar packet operations and native deadlines. ZeroTier's destination/stack validation runs for every packet. |

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

Sing-mux uses local counted writers and single-packet descriptors under its
existing write mutex. Address serialization writes directly into header storage. VMess's length
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

## Receive and native packet follow-up

XUDP transfers received datagrams through bounded owned queues and encodes
reply headers in place. Sing-mux shares packet framing state across scalar and
owned reads. The reviewed smux uses its standard heap/list scheduler and local
socket vectors; it still clears consumed receive descriptors so payloads are
not retained. Large `ChunkWriter` inputs retain ownership through independent chunks
and reuse the original buffer for the final chunk. Public buffer descriptors
remain unpooled to preserve their reference and release contracts.

The reviewed Hysteria2 serializer uses one local pooled fragment buffer, without
a session-owned scratch buffer or an extra write mutex. TUIC native datagrams use checked
byte parsing, exact-sized reassembly and association-scoped fragment IDs.
The [receive/native packet report](benchmarks/protocol-receive-native-2026-11-01.md)
records allocation changes, p95/p99 observations, failed baseline trials and the
existing XHTTP concurrent-test limitation. It does not claim uniform throughput
or tail-latency improvement.

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
