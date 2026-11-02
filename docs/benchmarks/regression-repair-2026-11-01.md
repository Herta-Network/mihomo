# UDP reply and TCP relay regression repair

This repairs the regressions found in the [upstream comparison](upstream-comparison-2026-11-01.md).
The base is Herta-Network `71141626`; the control remains upstream v1.19.33,
`3f27ef39`. Fixes are in the local working tree. Dependency versions are unchanged.
[Raw reruns, diagnostics, source hashes and checks](regression-repair-2026-11-01.json)
are retained, including exploratory failures and timing variation.

## UDP: preserve the payload-count contract

SOCKS5, the legacy Shadowsocks listener and Mieru returned framed wire bytes
from `WriteBack`. The owned fallback correctly requires application payload
bytes, so the mismatch produced `io.ErrShortWrite` after an otherwise successful
reply and closed the NAT flow. Each affected callback now returns the payload
length only after the complete encoded packet was accepted. A short success
becomes `io.ErrShortWrite`, partial datagrams report no complete payload, and an
existing transport error is preserved. The shared short-write guard remains.

New regression tests failed on all three old callbacks, then passed after the
repair. They cover IPv4, IPv6, names, empty/1/1200/60000-byte payloads, the owned
fallback's release behavior, short writes and full/partial writes with errors.

The fixed complete SOCKS5 → routing/NAT → DIRECT process passed all **20/20 UDP
runs**. All **210,000 timed request/reply pairs** matched, with zero missing,
duplicate or corrupted replies. An earlier focused four-packet-burst run also
completed 40,000/40,000 pairs. Previously, all 20 full-process runs failed.

## TCP: remove the regressing adaptive read policy

At low concurrency the old policy selected four-buffer vector reads; its
single-connection allocation profile attributed about 60% of sampled allocation
events to `SyscallVectorisedWriter.WriteVectorised`. At eight connections the
policy reduced every stream to 16 KiB, versus upstream's normal 32 KiB; CPU
profiles were dominated by socket system calls and allocations by the read
buffer descriptors in `copyControlled`.

Temporary controls tested the native path, fixed single-buffer 32 KiB reads,
and raw-socket-only native selection. The production repair now:

- preserves native copying for explicitly replaceable raw socket pairs;
- uses checked, single-buffer 32 KiB copying for opaque protocol wrappers;
- retains a 16 KiB cap for low-memory builds and low-memory Linux splice;
- preserves native Linux splice in ordinary builds;
- removes concurrency-driven buffer shrinking and speculative vector read-ahead.

Counter wrappers, cached handshake bytes and protocol transformations remain
owned by the copy engine. Regression tests exercise half-close, bidirectional
traffic counts, an opaque transforming wrapper, cached-prefix transfer/release
and short-success write progress. Ordinary and low-memory race checks pass.
Profile samples are hotspot evidence; their absolute allocation counts are not
compared as equal-byte workloads.

## Complete rerun

Apple M5/macOS 27.0.1, Go 1.27.3, `CGO_ENABLED=0`, `GOWORK=off`, `GOMAXPROCS=2`.
The same process driver and unchanged protocol fixture were used on both sides.
Five alternating before/after trials covered the seven process workloads and
all 26 protocol cases. All **80 run invocations** completed successfully. TCP
bulk cases transferred 4 GiB of request payload per run, plus matching replies.
No other build or test was launched by this task during measurement.

The driver now checks a complete TCP round trip before warming clients: a bound
listener alone can precede initial routing configuration. Only setup EOF is
retried, before warmup; application data is never replayed. Setup retry counts
are recorded. This applies equally to upstream and the repair.

### Full process, five-trial medians

Rates below count request payload only, in decimal MB/s. CPU and RSS data for
the target proxy are in the JSON. Timing drift on this shared desktop was large;
absolute rates must not be compared with rates from an earlier session.

| Workload | Upstream MB/s | Fixed MB/s | Ratio of medians | p99 µs, upstream → fixed |
| --- | ---: | ---: | ---: | ---: |
| tcp_bulk_c1 | 650.504 | 608.848 | -6.4% | 200.08 → 193.58 |
| tcp_bulk_c8 | 1551.251 | 1548.669 | -0.2% | 520.33 → 514.88 |
| tcp_small_c1 | 0.876 | 1.029 | +17.5% | 145.17 → 126.17 |
| udp64_single | 0.795 | 0.794 | -0.1% | 134.38 → 138.92 |
| udp1200_single | 16.625 | 14.247 | -14.3% | 128.75 → 149.79 |
| udp1200_burst4 | 36.636 | 36.131 | -1.4% | 225.25 → 255.12 |
| udp1200_c4 | 33.835 | 33.561 | -0.8% | 245.62 → 250.58 |

Eight-connection bulk TCP is back near upstream: the median rates differ by
about **-0.2%**, versus the previously observed -21.1%. The CPU-time medians are
also effectively equal. The mixed-matrix single-connection medians still differ,
but individual pairs swing in both directions, so an additional focused run
was performed rather than interpreting that aggregate difference as a stable
regression or speedup.

### Focused single-connection confirmation

Eight additional alternating pairs each transferred 4 GiB. All sixteen runs
passed exact-payload validation. Their aggregate medians are **951.8 → 920.4 MB/s**
(upstream → fixed), a -3.3% difference, while the median within-pair rate ratio
is **1.016 (about +1.6%). Both positive and negative differences occur.
A descriptive paired-resampling interval is approximately **0.953–1.058**;
its small, host-shared sample is not a production confidence guarantee.

The previous all-pairs-negative single-connection pattern did not recur. These
results support recovery into upstream's measurement variation, **not** a claim
of exact equality or stable single-connection speedup. Low-level policy changes
and correctness are verified independently of this noisy timing comparison.

### Shared protocols, four 1200-byte packets per operation

Both endpoints use the selected version. This layer checks the actual protocol,
deadline/reference and traffic wrappers, but bypasses routing/NAT and the sender
queue. Each of five trials runs 2000 timed operations. The JSON also includes
64-byte controls, p95/p99 and all samples. Allocation reductions remain; timings
are not a universal speedup claim.

| Protocol | µs/op, upstream → fixed | Allocations/op | B/op |
| --- | ---: | ---: | ---: |
| vmess-aes | 51.56 → 57.62 | 151 → 88 | 6387 → 4659 |
| vmess-xudp | 72.27 → 73.55 | 303 → 96 | 8195 → 4836 |
| vless | 84.67 → 91.61 | 263 → 80 | 7425 → 4805 |
| ss-udp | 97.22 → 103.03 | 462 → 386 | 45974 → 48731 |
| ss-uot | 60.91 → 55.83 | 179 → 66 | 6973 → 4648 |
| snell3 | 64.20 → 46.89 | 67 → 32 | 8888 → 3265 |
| anytls-uot | 87.66 → 71.75 | 184 → 64 | 6967 → 4486 |
| mux-smux | 151.86 → 74.01 | 374 → 74 | 17432 → 5018 |
| mux-h2mux | 156.07 → 79.62 | 210 → 80 | 7832 → 4864 |
| mux-yamux | 100.68 → 58.33 | 159 → 56 | 5745 → 4075 |
| hysteria2 | 190.49 → 204.09 | 779 → 491 | 56191 → 50296 |
| tuic4 | 169.19 → 142.91 | 359 → 312 | 56794 → 46658 |
| tuic5 | 218.96 → 214.71 | 782 → 572 | 77159 → 66444 |

## Validation and reproduction

- Race and vet checks passed for changed network, packet/deadline, constant,
  NAT and listener packages; the same package tests passed with low-memory mode.
- The actual protocol packet integration matrix passed under race detection.
- Linux/amd64 and Windows/amd64 builds passed. These are build results, not
  measurements of their socket/splice behavior or device VPN performance.
- Existing XHTTP concurrent-test failures and other documented broad-suite
  limitations were not expanded into this repair; no full-suite green claim is
  made. Runtime source contains no profiling endpoint added by these experiments.

Use the build instructions from the preceding comparison, with the current
working tree as the repaired executable and the unchanged upstream checkout as
the control. Then run:

```sh
python3 scripts/benchmark-upstream/run.py --driver /tmp/regression-process-driver \
  --original /tmp/upstream-comparison-original --fork /tmp/mihomo-regressions-fixed \
  --original-tests /tmp/upstream-comparison-original.test \
  --fork-tests /tmp/mihomo-regressions-fixed.test \
  --tcp-bytes 4294967296 --trials 5 --output /tmp/repair-comparison.json
GOWORK=off go test -race ./common/net/... ./constant ./component/nat \
  ./listener/socks ./listener/shadowsocks ./listener/mieru -count=1
```

The preceding comparison remains the historical result for published `71141626`.
This report describes the repair, not a new release tag or a production rollout.
