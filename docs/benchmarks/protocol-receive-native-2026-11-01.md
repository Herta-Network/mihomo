# Receive paths, owned chunks and native QUIC packets

This round continues the framing work in [the preceding report](protocol-framing-2026-11-01.md).
The implementation spans mihomo and five dependency forks. `go.mod` selects
immutable Herta-Network revisions, so `GOWORK=off` builds include these changes
without sibling checkouts. Measurements below were collected with the same
source changes through an isolated development workspace.

Published dependency source revisions:

| Repository | Commit |
| --- | --- |
| sing | [`1260cbaedd25`](https://github.com/Herta-Network/sing/commit/1260cbaedd252f9abb8d23576c862f64f50c0c98) |
| sing-vmess | [`7cef310d10c4`](https://github.com/Herta-Network/sing-vmess/commit/7cef310d10c44e3232244f6c7340ffa0dee0fb49) |
| sing-mux | [`d06b28e91440`](https://github.com/Herta-Network/sing-mux/commit/d06b28e91440b1a68ad1157d2d311a3f0fbd2c72) |
| smux | [`10aa72a7535c`](https://github.com/Herta-Network/smux/commit/10aa72a7535ccdfff207dbd78c2a815e82b71932) |
| sing-quic | [`9d7d4312a27b`](https://github.com/Herta-Network/sing-quic/commit/9d7d4312a27b3ff6bce45ade73d0603afd58edba) |

Baseline source revisions:

| Repository | Baseline |
| --- | --- |
| mihomo | `b50c68e4960526f6591212723f1dc1e39ee99993` |
| sing | `1d1b6994cf4aac99fe90d39831b8525233d744a1` |
| sing-vmess | `83a9fdf3e38e61efe2bdefad7aa6c5b6532013ab` |
| sing-mux | `9a83d0dd7caa8eba1ce0e05ef33cb97a5b095833` |
| smux | `d0c8756d3141ce2c9aa1046df6a93985441c6033` |
| sing-quic | `1c242664697a441b01cfaf67bdbb7bb0d532745e` |

## Changes, in the requested order

1. **XUDP reception and replies.** A bounded queue transfers owned UDP buffers
   instead of serializing the already parsed address and length into `io.Pipe`.
   Limits are four packets per stream and sixteen per session, or one/four with
   `with_low_memory`. Backpressure does not discard packets. Accepted packets
   drain before EOF cancels the handler context; local cancellation releases
   queued buffers and closes the transport. Stream identity prevents a late
   close from removing a new stream with the same wire ID. Receive scratch,
   direct reply headers, serialized cipher writes and zero-padding read guards
   remove additional allocations. Boundary read timeouts remain retryable;
   partial-frame failures remain terminal.
2. **Sing-mux and smux.** All packet read APIs share scratch and framing state;
   server connections now expose owned reads too. Short destinations consume
   the complete datagram, and the fixed-target owned reader returns its target.
   Smux reuses drained receive descriptor slots, uses a typed priority heap and
   bounded session-local scheduler node cache, and reuses native socket write
   vectors. Round-robin fairness, class priority and sequence wrap behavior are
   preserved. The scheduler cache retains at most sixteen idle nodes and drops
   heap storage larger than sixty-four requests.
3. **Large owned chunks.** `ChunkWriter` sends independently owned chunks and
   transfers the original buffer as the last chunk. Earlier chunks need their
   own framing space because encryption can modify both header and tail, and
   an owned writer can retain a chunk after returning. With a 16 KiB chunk size,
   a 60,000-byte input copies 49,152 bytes instead of all 60,000 bytes. Borrowed
   short writes return `io.ErrShortWrite`. Actual VMess AES-GCM, ChaCha20 and CFB
   chains are checked through 128 KiB without bypassing the stream chunk limit.
4. **Buffer descriptor reuse was evaluated.** A synchronous, exclusive-owner
   experiment removes the remaining 64-byte descriptor allocation. General
   owned handoff does not promise that lifetime: retained references and an
   existing caller's second `Release` would act on a reused packet. The public
   descriptor is therefore not pooled. Regression tests preserve those
   ownership contracts. This item does not claim zero-allocation owned handoff.
5. **Native Hysteria2/TUIC/QUIC.** CPU, allocation and blocking profiles were
   collected before changes. Hysteria2 uses its existing wire serializer with
   one connection-owned fragment workspace and a checked direct decoder.
   TUIC v4/v5 parse native datagrams without copying their address and payload
   into a temporary packet; stream headers use one fixed-size read/write.
   TUIC v5 allocates the exact assembled size once and checks fragment limits.
   QUIC `SendDatagram` still copies into its send queue. Congestion control and
   QUIC packet scheduling were not changed: local CPU profiles are dominated
   by UDP socket system calls, not evidence for changing their algorithms.

Long-loop testing exposed one TUIC v5 receive timeout before the final fix.
The audit found that random 16-bit fragment IDs can collide within an active
association. IDs now advance atomically per association, including distinct
server reply wrappers. Tests cover concurrent replies, wraparound and
interleaved fragments. Five subsequent 10,000-round runs delivered 200,000
packets without timeout. This does not prove that collision caused the original
observed timeout, and the failed run is retained in the measurement data.

## Measurement method

Apple M5, macOS arm64, Go 1.27.3. Loopback round trips use `GOMAXPROCS=2`.
VMess and native QUIC fixtures send four 1,200-byte packets per operation,
check payload/address and exact traffic totals, and include production inbound,
outbound and packet wrappers. Sing-mux fixtures use real h2mux/smux/yamux TCP
sessions with one or eight packets. Setup is excluded. Reported byte rates count
request payload bytes, not both network directions. The fixtures exclude routing
and the tunnel sender queue.

Tail samples cover complete round trips, including writes and all replies.
Samples are stored before timing stops and sorted afterward; their storage is
allocated outside the timed section. No packet retry is hidden in the fixture.
This is a shared workstation, not an isolated network throughput experiment.
Allocation reductions are the repeatable result; throughput and tail latency
vary and are not claimed as production improvements.

Final serial measurements use three alternating before/after trials: 10,000
operations for XUDP and 5,000 for each native/mux case. The table reports medians
of completed trials. [Raw samples, profiles and validation](protocol-receive-native-2026-11-01.json)
include failed trials and the earlier diagnostic runs.

| Case | Packets/op | Completed before/after | Allocations/op | B/op | Mean µs/op | p95 µs | p99 µs |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| XUDP | 4 | 3/3 | 224 → 100 | 6568 → 5091 | 46.67 → 42.54 | 56.12 → 51.12 | 73.12 → 61.17 |
| Hysteria2 | 4 | 3/3 | 752 → 492 | 55476 → 50205 | 132.45 → 127.37 | 205.83 → 196.79 | 268.62 → 260.71 |
| TUIC v4 | 4 | 3/3 | 376 → 316 | 57604 → 46903 | 103.40 → 95.90 | 182.04 → 154.12 | 252.08 → 215.21 |
| TUIC v5* | 4 | 2/3 | 799 → 577 | 77996 → 66703 | 194.85 → 142.51 | 327.02 → 228.46 | 458.81 → 296.46 |
| h2mux/batch=1 | 1 | 3/3 | 14 → 6 | 341 → 318 | 26.81 → 27.58 | 34.04 → 38.04 | 40.50 → 45.67 |
| h2mux/batch=8 | 8 | 3/3 | 111 → 47 | 2336 → 2148 | 73.50 → 70.71 | 86.08 → 82.12 | 136.71 → 95.46 |
| smux/batch=1 | 1 | 3/3 | 42 → 8 | 1472 → 449 | 24.23 → 21.96 | 31.21 → 28.79 | 35.88 → 34.92 |
| smux/batch=8 | 8 | 3/3 | 219 → 48 | 7375 → 2789 | 49.95 → 48.27 | 58.75 → 57.08 | 66.33 → 65.54 |
| yamux/batch=1 | 1 | 3/3 | 10 → 2 | 153 → 129 | 21.84 → 21.85 | 28.54 → 28.75 | 33.58 → 36.38 |
| yamux/batch=8 | 8 | 3/3 | 80 → 16 | 1224 → 1033 | 39.16 → 38.71 | 47.17 → 45.67 | 59.67 → 51.75 |

\* The third TUIC v5 **baseline** trial timed out, so its before figures have
only two completed trials. All three final updated trials completed. Its apparent
timing change must not be treated as a controlled throughput improvement.
Single-packet h2mux p99 also rose from 40.50 to 45.67 µs despite fewer allocations;
allocation gains do not imply uniformly lower tail latency.


The isolated `ChunkWriter` experiment measured 60 KB at **9 -> 4 allocations**
and 128 KiB at **18 -> 9**; the latter includes an unpooled backing allocation.
A 1,200-byte owned input remains at one allocation. Descriptor-only tests measure
**1 allocation / 64 B** for owned transfer and **0 / 0 B** for exclusive synchronous
reuse. The Hysteria2 4 KiB fragmentation workspace uses zero allocations after
warmup. TUIC native byte parsing uses zero allocations versus five in its
updated stream-reader comparison; it borrows immutable datagram storage.

## Validation and reproduction

Focused ordinary, race and low-memory tests cover queue bounds, EOF, cancellation,
late closes, empty and maximum-size packets, mixed read APIs, short buffers,
retryable boundary deadlines, partial-frame errors, oversized target names, retained chunk ownership,
cipher compatibility, native address parsing, malformed datagrams, fragment
reordering/duplication and packet-ID wrap. Actual mihomo protocol integration
covers VMess/VLESS carriers, all three mux transports, Hysteria2 and TUIC v4/v5.

Smux's complete ordinary suite passed, including both 8 GiB transfers and both
1 GiB random transfers. Focused smux concurrency tests and vet passed. Sing-quic,
sing-vmess and sing-mux vet and race checks passed; sing's changed buffer,
metadata, network and bufio packages passed vet/race/low-memory checks. Mihomo's
focused vet/race/low-memory checks and Linux/Windows amd64 cross-builds passed.
Cross-builds do not establish Linux/Windows runtime or WAN performance.

The complete mihomo ordinary suite was run with `SKIP_INTEROP_TEST=1` (external
Xray/V2Ray interoperability binaries were not included). All other packages
passed; `listener/inbound` failed its existing
`TestInboundVless_XHTTP_H1_Encryption/stream-up/nosplit/single/Concurrent` case
with `closed pipe` / connection resets. A fresh `GOWORK=off` baseline using the
published fork revisions reproduced the same failures and subsequently timed
out; baseline race repeats passed, whereas current race repeats also reproduced the
failure. This
unrelated XHTTP stress failure is retained as a validation limitation. The
full suite is **not** reported as green, and no test was disabled to make it pass.

After pinning the published forks, `GOWORK=off` checks passed for the single-sing
source guard, actual protocol race/low-memory integration, ordinary builds and
`with_naive,with_purego` builds. Downloaded fork Go sources match the validated
working-tree sources byte for byte. These dependency checks do not replace or
change the full-suite limitation above.

Development measurements used sibling source checkouts. Published dependency
validation disables the temporary workspace with `GOWORK=off`.

Representative commands from the mihomo checkout:

```sh
GOWORK=off go test -race ./listener/inbound -run TestProtocolPacketBatchIntegration -count=2
GOWORK=off go test -c -o /tmp/protocol-roundtrip.test ./listener/inbound
GOMAXPROCS=2 /tmp/protocol-roundtrip.test -test.run='^$' -test.bench='^BenchmarkVMessOwnedRoundTrip/xudp$' -test.benchmem -test.benchtime=10000x
GOMAXPROCS=2 /tmp/protocol-roundtrip.test -test.run='^$' -test.bench='^BenchmarkNativeQUICOwnedRoundTrip$' -test.benchmem -test.benchtime=10000x
```

The pinned revisions are source commits, not a new release tag. Solaris was
not modified in this round.
