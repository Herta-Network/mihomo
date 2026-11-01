# UDP buffer ownership and allocation measurements

Baseline: `13c4b155389077690915a9e2b9f9191f2c5786e1`. After: the working-tree
changes accompanying this report. Apple M5, macOS arm64, Go 1.27.3,
`GOMAXPROCS=2`. Measurements used separate compiled binaries, alternating
before/after order, with no concurrent builds or tests. Each sample ran for one
second in an independent process. Values below are medians; raw samples and
the sample counts are in [udp-allocation-2026-11-01.json](udp-allocation-2026-11-01.json).

The protocol samples in this historical report used the original per-process
random test PSK. Snell v6 derives its shaping profile from that key, so those
v6 timings do not compare an identical workload. The later
[owned downstream report](owned-downlink-2026-11-01.md) fixes the benchmark PSK
in both revisions. Do not directly compare timing across the two fixtures.

## Changes measured

- Sing and legacy Snell UDP inbounds can transfer their owned `buf.Buffer`
  through the existing packet adapter. The sender preserves its headroom and
  copies only when the destination requires more. `Drop` no longer releases a
  transferred payload; the reply endpoint remains usable after transfer.
- Destination lookups use an IP/host value key. The counted path carries
  `netip.AddrPort` directly instead of formatting IP strings and allocating
  `net.UDPAddr` values for each packet. Sender scratch arrays are reused.
- Updating the NAT reply target uses a short lock instead of allocating an
  atomic interface container per packet. Replies release that lock before I/O.
- Snell v3 validates a complete batch, writes address headers into owned
  headroom and encrypts records in place. Its input reader decrypts into an
  appropriately sized pooled buffer, removing the per-packet payload clone.
- Counted stream writes reuse vector descriptors. The ordinary-writer fallback
  still joins frames when necessary, using pooled bytes without allocating a
  temporary buffer descriptor. It preserves the original framing writer.
- The per-connection sender reuses the existing deque implementation as a
  bounded ring. Producers signal the transition from empty to nonempty; the
  consumer drains up to four packets under one lock and rearms one wakeup when
  work remains. Capacity stays at 128 packets, and low-memory builds drain one.
- Read deadlines retain one timer and use a boolean to record whether a deadline
  is set. Refreshing an unexpired deadline allocates nothing. Close stops timers
  and prevents concurrent refreshes from rearming them.

## Steady sender and encoding paths

Each operation handles four packets. Sender inputs, packet descriptors and
metadata are prepared outside the timer, modeling buffers already owned by an
ingress. These benchmarks include mapping, reply-target updates, headroom
preparation and release, but exclude acquiring input buffers and socket I/O.
The queue case additionally runs the real `Send`/`Process` goroutines and waits
for every `Drop` before reusing input storage. Each sender case has three
process samples per revision.

| Path | Before ns/op | After ns/op | Before B/op | After B/op | Before allocs/op | After allocs/op |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Borrowed sender, 1200 B payload | 600.4 | 401.1 | 1120 | 256 | 30 | 4 |
| Owned sender, sufficient headroom, 1200 B | 632.5 | 231.5 | 1120 | 0 | 30 | 0 |
| Owned sender including channel/worker, 1200 B | 1322.0 | 751.6 | 1120 | 0 | 30 | 0 |

The Snell v3 encoding benchmark reuses caller-owned input buffers with sufficient
headroom and performs real address framing, AES-GCM encryption and the joined
write fallback into a discard connection. Lazy initialization and pool warm-up
precede the timer. Three after-only samples measured:

| Payload per packet | ns/op (four packets) | B/op | allocs/op |
| --- | ---: | ---: | ---: |
| 64 B | 427.7 | 0 | 0 |
| 1200 B | 908.3 | 0 | 0 |

These zero-allocation results describe the measured steady stages. They do not
include packet/metadata creation, insufficient-headroom copies, pool misses,
handshakes, or the full connection lifecycle. The channel measurements above
preceded the ring change. They establish that the allocation improvement does
not depend on replacing the queue.

## Ring queue and deadline follow-ups

After the buffer-allocation changes, a separate five-sample A/B compared the
original channel with the bounded deque ring, using the same queue benchmark:

| Queue | Median ns/op (four packets) | Sample range ns/op | B/op | allocs/op |
| --- | ---: | --- | ---: | ---: |
| Channel | 766.9 | 751.1–776.6 | 0 | 0 |
| Ring with coalesced wakeup | 665.3 | 657.6–698.1 | 0 | 0 |

This is 13.2% less time in the measured queue/worker cycle. It is a single
producer workload; concurrent send/close is covered by correctness tests, not
a measured high-contention throughput claim. Tests also cover ring wrap, full
queue drops, FIFO order, repeated empty wakeups and reentrant release callbacks.

Deadline samples compare the original implementation with timer reuse, in three
alternating process samples per revision. One operation refreshes a deadline:

| Operation | Before ns/op | After ns/op | Before B/op | After B/op | Before allocs/op | After allocs/op |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Packet read deadline | 86.78 | 42.52 | 152 | 0 | 3 | 0 |
| Pipe deadline | 81.58 | 40.58 | 128 | 0 | 2 | 0 |
| Clear then refresh pipe deadline | 82.47 | 44.45 | 128 | 0 | 2 | 0 |

Expired deadlines still require a fresh cancellation channel. Tests exercise
refresh after expiry, repeated clear/refresh, concurrent changes and closing
with an active timer. The loopback samples below preceded the ring/deadline
follow-ups and do not measure their contribution to full forwarding.

## Real Snell v3 TCP loopback

The existing `BenchmarkSnellPacketWrite` sends four UDP datagrams per operation
through real outbound, reference, deadline and traffic wrappers. The Go inbound
drains all packets before the timer stops. It includes allocations on both
endpoints and validates exact delivery count and plaintext upload accounting.
It does not pass through the tunnel's routing/sender queue, so its improvements
must not be added to the sender microbenchmark as an end-to-end estimate.
Five process samples were collected per revision.

| Payload | Before MB/s | After MB/s | Before B/packet | After B/packet | Before allocs/packet | After allocs/packet |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| 64 B | 56.09 | 60.94 | 1145.50 | 736.00 | 14 | 6 |
| 1200 B | 681.66 | 843.71 | 3568.25 | 736.00 | 14 | 6 |

For 1200 B packets this run measured 57.1% fewer allocation events, 79.4% fewer
allocated bytes and 23.8% higher loopback throughput. These are local workload
measurements, not WAN or production guarantees. The 64 B throughput increase
was 8.6%.

V4 and v6 were also sampled as controls. Their allocation counts remained
20.75 and 22 per packet respectively. Timing, especially v6 shaping, varied
substantially; no throughput improvement is attributed to those protocols.
Their raw samples are retained in the JSON.

## Correctness and remaining scope

Regression coverage includes owned-buffer identity and release, empty packets,
IPv4/IPv6/domain framing, maximum records, truncated and unauthenticated input,
scalar/owned read interleaving, invalid-batch nonce preservation, partial batch
progress, blocked-write shutdown, concurrent reply updates and low-memory paths.
Switching from `WriteTo` after EOF to owned reads also preserves EOF and the
cipher sequence; a fully consumed workspace is never emitted as an empty packet.
The domain-frame parser now accepts an empty UDP payload, matching the IP-frame
path. Ordinary borrowed writes still leave caller data unchanged.

The full relay is not zero allocation: packet/metadata descriptors and some
protocol/transport wrappers still allocate. Downstream `WriteBack` remains
scalar. The NAT reply proxy intentionally retains its latest packet/metadata
adapter after upload, so those objects cannot safely enter a pool on `Drop`.
Reducing their allocations requires separating the persistent reply handle
first. An owned downstream API must also preserve the deadline wrapper's cached
read results and the traffic tracker's accounting, including prefetched packets.
Queue contention tuning, end-to-end downstream batching, adaptive byte
budgets and further protocol-specific allocation cleanup remain separate work.
No production endpoints, WAN throughput, retained-heap/RSS improvement or
real Linux/Windows socket behavior were measured by this report.

## Reproduction

Build the parent revision in a detached worktree, copy
`tunnel/connection_benchmark_test.go` from this change into that worktree, and
build test binaries with `go test -c` for `./tunnel` and `./listener/inbound`.
Build the same packages in the modified tree. Alternate the old and new binaries
with `GOMAXPROCS=2` and these arguments:

```sh
-test.run '^$' -test.bench '^BenchmarkPacketSender' -test.benchmem -test.benchtime=1s -test.count=1
-test.run '^$' -test.bench '^BenchmarkSnellPacketWrite$' -test.benchmem -test.benchtime=1s -test.count=1
```

The after-only protocol microbenchmark is:

```sh
GOMAXPROCS=2 go test ./transport/snell -run '^$' \
  -bench '^BenchmarkSnellV3OwnedEncoding$' -benchmem -benchtime=1s -count=3
```
