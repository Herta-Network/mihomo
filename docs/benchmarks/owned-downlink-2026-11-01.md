# Shared packet I/O and owned downstream measurements

Baseline: mihomo `6a0c0e3c`, sing `a402b0b490ba`, sing-snell `42f9d87468f7`.
The modified implementation is the change accompanying this report, together
with the fixed fork revisions listed in [sing-fork.md](../sing-fork.md).
Measurements ran on Apple M5, macOS arm64, Go 1.27.3, `GOMAXPROCS=2`.
Each case has three one-second samples from separate processes. No builds or
tests ran concurrently with these measurements. The tables report medians;
all 60 selected samples are in [the JSON](owned-downlink-2026-11-01.json).

Both protocol fixtures use the fixed test PSK
`mihomo-fixed-benchmark-psk-2026-11-01`. The previous fixture used a random UUID
per process, but v6 derives its shaping profile from the PSK. That made timing
comparisons use different workloads. Exploratory random-PSK samples are excluded
from this report; both revisions were rebuilt with the corrected fixture.

## Changes

- Sing's counted TCP vector writer reuses descriptors and serializes concurrent
  calls. Both vector fallbacks join into pooled bytes without allocating a
  temporary buffer descriptor. Failed writes clear all retained payload slices.
- `CachedPacketConn` exposes an owned read waiter that consumes its handshake
  cache first, including empty packets. Scalar, cached and waiter reads share
  ownership. Closing interrupts the transport before releasing pending buffers
  and waiter storage; returned buffers remain owned by their caller.
- Snell v4/v5/v6 reuse their address-header workspace, counted sink and record
  encoder. Packet record vectors reuse the consumed input slice. Readers reuse
  encrypted-header storage and use pooled padding bytes without a descriptor.
  Scalar packet writes also reuse their one-element descriptor workspace.
  Cipher, padding/shaping, nonce and complete-prefix counting remain in the
  original protocol engines.
- Mihomo's optional `ReadPacketBuffer` crosses the packet, deadline, reference,
  outbound and traffic wrappers without unwrapping cached readers. Download
  accounting observes plaintext once. Legacy readers copy before releasing
  borrowed storage, and failures do not transfer a buffer.
- The downstream loop restores NAT addresses as values and transfers the owned
  payload to `WriteBackBuffer`. Sing and legacy Snell replies retain headroom
  and only copy when their encoder requires more. Unsupported reply endpoints
  use their original `WriteBack`, followed by release and short-write checking.
- NAT retains an independent reply endpoint, with a value snapshot of the
  optional nil-address fallback. It no longer retains the packet adapter and
  metadata for the converted inbounds. Flow setup snapshots metadata before
  asynchronous dialing; packet metadata and flow/tracker metadata have distinct
  lifetimes. Context additions preserve override order without building a new
  list per packet. Packet and metadata objects are not globally pooled.
- Deadline read APIs consume each other's cached results after a timeout.
  Concurrent readers serialize admission and consumption so a pending result
  cannot be mistaken for an empty-channel admission token. Close remains able
  to interrupt an active reader.

## Steady stages

The vector fixtures reuse four 1200 B input buffers. The TCP case writes to a
real loopback connection, while the joined case uses a discard writer. The
downstream fixture runs the actual downstream loop, traffic tracker, NAT map
and reply proxy with an already-owned input buffer and a consuming reply sink.
It excludes obtaining input storage, socket reads and protocol encoding.

| Stage | Packets/op | Before ns/op | After ns/op | Before B/op | After B/op | Before allocs/op | After allocs/op |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| Joined counted vector | 4 | 77.20 | 73.12 | 64 | 0 | 1 | 0 |
| TCP counted vector | 4 | 2319 | 2311 | 120 | 0 | 2 | 0 |
| Owned downstream loop | 1 | 85.15 | 67.96 | 120 | 0 | 5 | 0 |

These are warmed steady paths, not a guarantee of zero allocations during
handshakes, pool misses, insufficient-headroom copies or complete relaying.
The TCP timing is essentially unchanged; its measured benefit is allocation.

## Real protocol round trips

Each operation sends four 1200 B requests and receives four replies through
real TCP connections, inbounds, outbounds, deadline/reference wrappers and
traffic trackers. The same fixture compiles on the baseline: it uses owned
interfaces when available and otherwise preserves the legacy read/write path.
Both payload/source equality and exact upload/download counts are checked.
The workload does not traverse tunnel routing or the sender queue.

| Protocol | Before ns/op | After ns/op | Before B/op | After B/op | Before allocs/op | After allocs/op |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Snell v3 | 35761 | 34274 | 3894 | 3521 | 56 | 36 |
| Snell v4 | 35465 | 34517 | 7378 | 4321 | 155 | 60 |
| Snell v6, default shaping | 47361 | 45865 | 7923 | 4324 | 164 | 60 |

Allocation events fell by 35.7%, 61.3% and 63.4% respectively. Timings are local
three-sample observations, not a general latency or throughput guarantee.
The round trips still allocate; they are not full-relay zero-allocation results.

## Upload-only control

The existing `BenchmarkSnellPacketWrite` still verifies delivery and upload
accounting at both ends. It was rebuilt with the same fixed PSK on both revisions
and sampled in alternating order. Timing is retained in the JSON; this table
focuses on allocation traffic rather than claiming a general throughput gain.
Each row uses 1200 B packets; the JSON also contains the 64 B controls.

| Protocol | Before B/packet | After B/packet | Before allocs/packet | After allocs/packet |
| --- | ---: | ---: | ---: | ---: |
| Snell v4 | 1252.50 | 856.00 | 20.75 | 11 |
| Snell v6 | 1323.75 | 857.00 | 22 | 11 |

An allocation profile exposed `CachedPacketConn.ReadPacket` as an extra payload
allocation/copy after the handshake, and repeated context-key/addition work in
the listener. Those findings drove the owned-cache waiter and context changes.
Remaining costs include input descriptors, packet/metadata construction,
address objects and deadline pipe dispatch. This report does not measure
retained heap/RSS, WAN behavior or non-macOS socket execution.

## Correctness and reproduction

Coverage includes mixed read APIs after timeout, concurrent readers, empty and
large packets, headroom identity/copying, failed and late buffer release,
prefetched ownership, blocked reply shutdown, nil-address fallback snapshots,
NAT source restoration, traffic counting and context override order. Snell
loopbacks exercise v3-v6, all v6 modes, and HTTP/TLS obfuscation with both owned
and legacy replies. Protocol tests retain invalid-batch nonce and partial-write
prefix regressions.

Sing's full race suite and affected low-memory/debug checks pass. Sing-snell's
race suite and low-memory/debug checks pass. Mihomo's affected race and
low-memory/debug integration checks, repository compilation with selected
regressions, and `go vet ./...` pass. Sing's affected package passes vet; its
full vet run still reports the pre-existing `common/cond.go:177` unsafe-pointer
warning, unchanged from `a402b0b490ba`.
The fixed-dependency `with_naive` race regressions also pass against a local
sing-box 1.13.15 server, including HTTP/2, QUIC and UoT paths. These are local
interoperability checks, not production endpoint or device-VPN evidence.
The changed sing packet-I/O and mihomo deadline test packages also compile for
Linux/amd64 and Windows/amd64. Their socket behavior was not executed on those
platforms during this run.

For the baseline, copy `tunnel/connection_downlink_benchmark_test.go`,
`listener/inbound/snell_benchmark_test.go` and
`listener/inbound/snell_owned_benchmark_test.go` into a detached checkout of
`6a0c0e3c` and compile test binaries for `./tunnel` and `./listener/inbound`.
Compile the same binaries for this change with its fixed dependencies. For
sing, copy `common/bufio/vectorised_count_benchmark_test.go` into the parent
revision and compile `./common/bufio`. Alternate old/new binaries with
`GOMAXPROCS=2`, `-test.run '^$'`, `-test.benchmem`, `-test.benchtime=1s`, and one
of these benchmark selectors:

```text
^BenchmarkCountedVectorWriter$
^BenchmarkPacketOwnedDownlink$
^BenchmarkSnellOwnedRoundTrip$
^BenchmarkSnellPacketWrite$/v(4|6)
```
