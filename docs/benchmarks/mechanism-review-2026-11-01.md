# Performance mechanism review and simplification

This review separates correctness requirements from performance claims. Its
reference is Herta-Network `71141626` plus the preceding UDP reply/TCP relay
repairs. The historical upstream reference is MetaCubeX v1.19.33, `3f27ef39`.
Measurements preceded publication. The reviewed fork revisions are now pinned
in `go.mod`; the raw report retains the original measurement snapshots.

The complete-chain driver runs SOCKS5 → client routing/NAT → selected protocol
→ server routing/NAT → echo backend. Both proxies use the selected binary.
This exercises the sender queue, destination mapping, reply callbacks, traffic
wrappers and protocol implementation together. It supplements the existing
protocol-only fixture, which intentionally bypasses the main routing/NAT loops.

## Measurement method

Apple M5, macOS 27.0.1, Go 1.27.3, `CGO_ENABLED=0`, `GOMAXPROCS=2` per proxy.
One mechanism is disabled at a time, using immutable source overlays. Correctness
changes remain enabled unless a control is explicitly labelled as historical
attribution only. Such a control is never applied wholesale to production code.
Trials alternate baseline/variant order; the same-binary controls measure host
variation. No build, test or other benchmark was started by this task during a
measurement series. Desktop load outside this task was not controlled.

Rates count request payload only; every response is checked for exact content,
identity and destination. Application data is never retried. Initial routing
readiness retries are outside warmup/timing and remain recorded. CPU is the sum
of both proxy processes, including setup and shutdown. RSS is the sum of their
individual peak RSS values: an upper bound, **not** a simultaneous combined peak.
Neither allocation counts nor isolated codec timings prove lower process RSS.
An additional 80 invocations isolate mux/Snell adapter caches across three mux
carriers and Snell v4/v5/v6 modes, with 24,000 datagrams per invocation.

The first screen contains 234 successful invocations: three paired trials for
39 mechanism/workload combinations. Extended confirmation adds 80 successful
invocations with five paired trials and 48,000 datagrams per invocation.
The extended same-binary controls differ by about 2–3% in median rate/CPU, and
p99 varies more. Differences of that scale are not treated as optimization wins.

## Mechanism decisions

Ratios below are disabled mechanism / current reference. CPU ratios greater
than 1 mean retaining the mechanism saves CPU for the same work. Values are
medians of paired ratios, not ratios of unrelated aggregate medians.

| Mechanism | Evidence | Decision |
| --- | --- | --- |
| Four-packet receive prefetch | Initial DIRECT/XUDP screens were inconclusive. In the simplified complete smux chain, restoring prefetch reduces CPU to 0.929 of the scalar-reader version; all five long pairs improve. | Retain bounded prefetch and its ownership checks for the measured burst benefit. Low-memory builds remain scalar. This has a native UDP single-packet cost, quantified below; it is not a universal throughput/RSS improvement. |
| Group already queued sends | Extended CPU ratios: SS-UoT 1.078, smux 1.134; throughput is effectively unchanged. DIRECT's 1.042 is close to host variation. | Retain bounded grouping. The supported benefit is about 7% and 12% CPU reduction on the two stream-carried paths, not a throughput/RSS claim. |
| Deque plus coalesced wakeup | Replacing it with a channel changes screen CPU by only 0.5–2.3%. | Restore one bounded channel; keep admission/Close serialization and callbacks outside its mutex. |
| Owned downstream forwarding layer | Restoring the removed layer into the simplified version gives CPU ratios 1.038 for smux, 1.046 for Snell v4 and 0.994 for Hysteria2 in five long pairs; no benefit is established. | Remove the extra read/write forwarding interfaces, wrappers and reply scratch state. Keep the independent reply handle, metadata snapshot and payload-count contract. |
| Generic progress wrappers | Native QUIC controls show no stable benefit. They perform sequential calls rather than native batching. | Remove the batch wrapper and its automatic insertion. Preserve a scalar-only terminal-error guard for stream-carried ShadowQUIC UDP and VLESS Reverse. Test actual batch capability or the real scalar fallback, including counters and Close. |
| Reusable deadline timer | Screen rate/CPU changes are within variation. | Restore fresh timers with stop/drain ordering; retain permanent shutdown, wakeup and cached-result fixes. |
| Persistent iovec descriptors | Fresh descriptors show no CPU/rate advantage for retained storage. | Remove retained vectors and the extra sing descriptor mutex. Keep native writev, exact byte counts and caller-vector immutability. |
| smux typed heap, linked ring and idle-node cache | Historical scheduler control differs by about 2–3% in CPU/rate. | Restore standard heap/list scheduling. Keep fairness and concurrency tests. |
| smux receive descriptor clearing | No measurable speedup. | Keep: releasing references to consumed payloads is a retention/ownership fix, not an optimization claim. |
| XUDP demultiplexing/owned queue/reply path | Extended historical-demux CPU ratio 1.243; rate ratio 0.995. | Retain: about 20% CPU reduction for the tested burst workload. The control groups demux parsing, queue and reply encoding; it cannot attribute the gain to the queue alone. |
| Hysteria2 native packet handling | Historical-codec screen CPU ratio 1.033 is insufficient speed evidence. | Keep checked decoding, fragment limits and exact-size reads. Remove the persistent write scratch/mutex; use local pooled fragment storage. |
| TUIC byte decoding | Extended CPU ratios: v4 1.027, v5 1.008. | No speed claim. Retain explicit size/type/fragment validation, bounded reassembly and sequence IDs for correctness; no extra cache is added. |
| sing-mux cached writer and scalar descriptor arrays | Five-pair CPU ratios: smux 1.041, h2mux 0.978, yamux 0.982; no consistent advantage. | Remove retained adapter and scalar scratch state. |
| Snell address/sink/encoder/scalar caches | Five-pair CPU ratios for v4/v5/v6/default/unshaped/unsafe-raw remain between 1.005 and 1.045; RSS remains within variation. | Remove these caches. Keep the record cipher and its required nonce/shaping state. |
| Large owned stream chunks | Tested TCP path has no stable end-to-end gain and may not trigger the large-chunk branch. | Keep error/ownership guarantees and short-write checks; do not claim a measured throughput win for this branch. |
| In-place protocol headers | Fresh-copy controls have similar complete-chain results. | Keep the simpler direct serialization, which adds no independent cache/lifecycle mechanism. Do not translate fewer copies into a throughput or RSS claim. |

Global authentication, DNS publication, GEO/reload synchronization, packet-size
and empty-datagram rules, full-prefix write accounting, terminal stream failures,
post-close cleanup and cached-read ownership are correctness requirements.
They are not rolled back to improve benchmark numbers. Fixing the earlier UDP
regression is restoration of correctness, not an optimization achievement.

## Interaction checks

A first complete matrix exposed a roughly 7.5% smux CPU penalty in the proposed
simplification. Five longer paired trials (60,000 datagrams each) confirmed it.
Restoring the mux cache, smux scheduler, timer reuse or vector cache individually
changed CPU by only about 0–1%; none explained that penalty. The VLESS small-packet
and h2mux TCP differences seen in the mixed matrix did not persist in longer
confirmation, including 4 GiB TCP runs.

Restoring the removed owned downstream layer also did not help: CPU ratios were
1.038 for smux, 1.046 for Snell v4 and 0.994 for Hysteria2. A separate five-pair
control then restored receive prefetch (0.929 CPU ratio), the deque/wakeup queue
(1.006), or both (0.913). The same-binary control was 1.027. Prefetch alone recovers
the observed penalty; the small additional difference with the deque does not
justify retaining its extra queue/wakeup state. The final source therefore keeps
bounded prefetch and uses the simpler channel queue.

These checks illustrate why a result for DIRECT or XUDP does not establish the
same result for a mux chain. The 80 long restoration checks, 40 owned-path checks
and 40 read/queue checks all completed successfully; their raw samples remain in
the JSON, including host-noise outliers.

A further 40 long small-packet invocations checked VLESS and Snell v5/v6
against the pre-pruning reference after the mixed matrix showed worse medians.
With 30,000 datagrams per invocation, CPU ratios were 1.002, 0.999 and 1.028;
the same-binary Snell v5 control was 1.011. The mixed-matrix VLESS/Snell v5
regressions therefore did not persist. Snell v6 p99 remained variable and is
not claimed as a latency improvement. All 40 invocations passed.

### Native UDP prefetch tradeoff

The final upstream matrix showed a repeatable native UDP single-packet CPU
penalty. A further 60 invocations disabled only native socket prefetch while
keeping the protocol readers unchanged. All passed. Five-pair scalar/native
CPU ratios were 0.912 for DIRECT 64-byte singles and 0.943 for 1200-byte singles;
request rates improved to 1.071 and 1.053. DIRECT bursts were mixed (CPU 0.966),
and four-association throughput worsened to 0.929 despite CPU 0.965.

The same change increased complete smux burst CPU to 1.086, with all five pairs
above 1 (1.049, 1.036, 1.322, 1.092, 1.086). The same-binary CPU control was 0.985.
Native prefetch therefore has a real workload tradeoff. It remains enabled for
its demonstrated burst CPU benefit; the single-packet cost is **not** declared
fixed or hidden by an aggregate score. No new adaptive policy or user setting
was added. These macOS measurements do not decide the Linux batching policy.

## Validation and source state

Measurements used local changes in mihomo and five sibling forks: sing,
sing-mux, sing-snell, smux and sing-quic. A development workspace selected those
working trees. Publication pins the same runtime source in `go.mod`, so current
`GOWORK=off` builds include the reviewed changes without sibling checkouts.
The raw report records the original heads, source patches, changed-file hashes,
executable hashes and ablation overlays, plus the published fork revisions and
checks of the resulting dependency graph.

Publication verification found the same 213 module version selections, with
only the five intended replacements changed. All 445 Go source files in those
five downloaded modules match the validated working trees byte for byte.
With `GOWORK=off`, source-identity and module-checksum checks, protocol race and
low-memory tests, vet, and ordinary/Naive-purego/Linux/Windows builds passed.
The performance matrix was not rerun for these identical runtime sources;
the published build hashes are recorded separately from measurement hashes.

Focused root race, low-memory and vet checks passed, including protocol byte
counts, empty/large packets, cached-result API switches, close/cancellation,
queue capacity/order, traffic totals and packet release. Changed fork checks
passed. The full ordinary smux suite passed, including two 8 GiB and two 1 GiB
random-data transfers. Its non-bulk-random suite also passed under race detection;
the all-tests race attempt was stopped during the costly per-byte random stress
workload and is not reported as a full race-suite pass. Smux benchmarks were
smoke-tested separately from the performance comparison.

Ordinary, Naive/purego, Linux/amd64 and Windows/amd64 builds passed. Linux/Windows
socket behavior, production traffic and device VPN operation were not measured.
The previously documented broad-suite XHTTP concurrent-test limitation remains;
these focused checks are not a new full-root-suite green claim.

Use the [benchmark driver instructions](../../scripts/benchmark-upstream/README.md)
for the two complete matrices. Build both sides with the same Go toolchain and
copy the unchanged `BenchmarkUpstreamUDP` fixture into the upstream checkout. The
fixture is byte-identical on both sides. Current pinned builds do not require a
workspace. To reproduce the original local-source setup, use the following
module mapping with checkouts at the recorded source revisions (paths relative
to that workspace must be adjusted to the checkouts):

```go
go 1.27.3
use ./mihomo
replace (
    github.com/metacubex/sing => ./sing
    github.com/metacubex/sing-mux => ./sing-mux
    github.com/sagernet/sing-snell => ./sing-snell
    github.com/metacubex/smux => ./smux
    github.com/metacubex/sing-quic => ./sing-quic
)
```

Set `GOWORK` to that file for candidate builds/tests. Keep the workspace out of
committed module pins. The reference for the mechanism review is `71141626` with
the preceding relay/reply repair patch from the JSON; it is not the uncorrected
published executable. The upstream reference remains `3f27ef39`.

## Final complete comparison

The five-trial complete-chain matrix finished **940/940 successful invocations**:
19 paths, 94 workload combinations, 760 UDP runs totaling 5,700,000 checked
requests and 180 TCP runs totaling 45 GiB of request payload. UDP lost/invalid
counts were zero. These totals combine both reference and candidate runs.

Relative to the repaired pre-pruning version, the median across the 94
workload-specific paired medians was 1.000 for rate, 0.998 for CPU and 0.997
for RSS. These are descriptive, equally weighted workload summaries, not a
pooled traffic result. They support preserving the measured behavior while
removing **505 net lines of runtime source across six repositories**; they do
not establish an overall throughput or peak-RSS improvement.

The following burst table uses candidate / repaired pre-pruning reference.
Every row has five paired trials; the raw JSON also contains every single-packet,
four-association and TCP workload, including their ranges.

| Complete path, 1200-byte burst of four | Rate | CPU | Sum of peak RSS | p99 |
| --- | ---: | ---: | ---: | ---: |
| direct | 0.996 | 1.000 | 0.988 | 1.005 |
| vmess | 1.050 | 0.941 | 1.001 | 1.005 |
| vmess-xudp | 1.007 | 0.993 | 0.992 | 0.992 |
| vless | 0.991 | 0.996 | 0.995 | 1.001 |
| ss-udp | 1.021 | 0.978 | 0.995 | 0.985 |
| ss-uot | 1.036 | 0.968 | 0.999 | 0.975 |
| snell3 | 0.958 | 1.039 | 0.994 | 1.062 |
| snell4 | 0.996 | 1.007 | 1.003 | 1.052 |
| snell5 | 0.977 | 1.032 | 0.993 | 1.105 |
| snell6 | 0.997 | 1.016 | 0.999 | 1.017 |
| snell6-unshaped | 1.010 | 0.981 | 0.990 | 1.034 |
| snell6-unsafe-raw | 0.989 | 1.013 | 0.992 | 1.048 |
| anytls-uot | 1.016 | 0.984 | 0.990 | 1.010 |
| mux-smux | 1.025 | 0.977 | 0.997 | 1.004 |
| mux-h2mux | 0.984 | 1.026 | 1.004 | 1.010 |
| mux-yamux | 0.996 | 0.997 | 0.999 | 1.009 |
| hysteria2 | 1.026 | 0.974 | 0.989 | 0.949 |
| tuic4 | 0.978 | 1.020 | 0.990 | 1.022 |
| tuic5 | 1.040 | 0.958 | 0.996 | 0.971 |

### Shared upstream comparison

The repeated upstream matrix completed 70/70 process runs. All five candidate
protocol runs passed all 26 cases. The original's first TUIC v5/1200 protocol
case timed out; the other 25 cases in that run and the remaining four original
runs completed. Thus 79/80 invocations succeeded, with 259/260 individual
protocol results. Failed output is retained, not silently rerun or excluded.

The complete-process DIRECT results below use candidate / upstream, with five
paired trials. Bulk TCP transfers are 4 GiB per invocation. In particular,
UDP single-packet throughput is about 5–6% lower and CPU about 8–9% higher;
four-association UDP has median rate 0.908 and CPU 1.168. These remaining costs
prevent any general claim that the fork is faster than upstream. Small TCP
has a favorable median but very large order/host variation, so no stable gain
is claimed for that row either.

| Complete-process workload | Rate | CPU | Peak RSS | p99 |
| --- | ---: | ---: | ---: | ---: |
| tcp_bulk_c1 | 1.003 | 0.989 | 0.997 | 0.975 |
| tcp_bulk_c8 | 0.995 | 1.005 | 1.004 | 1.014 |
| tcp_small_c1 | 1.104 | 0.802 | 1.031 | 1.077 |
| udp64_single | 0.946 | 1.089 | 0.977 | 1.060 |
| udp1200_single | 0.944 | 1.082 | 0.994 | 1.050 |
| udp1200_burst4 | 0.968 | 0.971 | 1.005 | 1.092 |
| udp1200_c4 | 0.908 | 1.168 | 1.032 | 1.097 |

The protocol-only fixture below is a different layer: it bypasses the complete
routing/NAT/sender path and performs four datagrams per operation. Its isolated
rate and allocation gains cannot be promoted to full-process CPU/RSS claims.
Rate is upstream ns/op divided by candidate ns/op; p99 is candidate / upstream.

| Protocol fixture | Paired trials | Rate | p99 | Allocations/op, upstream → candidate |
| --- | ---: | ---: | ---: | ---: |
| vmess-aes/bytes=64 | 5 | 0.944 | 1.103 | 151 → 100 |
| vmess-aes/bytes=1200 | 5 | 0.981 | 1.016 | 151 → 100 |
| vmess-xudp/bytes=64 | 5 | 1.150 | 0.856 | 303 → 108 |
| vmess-xudp/bytes=1200 | 5 | 1.159 | 0.823 | 303 → 108 |
| vless/bytes=64 | 5 | 1.140 | 0.860 | 263 → 82 |
| vless/bytes=1200 | 5 | 1.146 | 0.772 | 263 → 82 |
| ss-udp/bytes=64 | 5 | 0.959 | 1.190 | 462 → 392 |
| ss-udp/bytes=1200 | 5 | 0.970 | 1.051 | 462 → 392 |
| ss-uot/bytes=64 | 5 | 1.184 | 0.857 | 179 → 88 |
| ss-uot/bytes=1200 | 5 | 1.143 | 0.830 | 179 → 88 |
| snell3/bytes=64 | 5 | 1.082 | 0.973 | 67 → 56 |
| snell3/bytes=1200 | 5 | 1.073 | 0.827 | 67 → 56 |
| anytls-uot/bytes=64 | 5 | 1.085 | 0.774 | 183 → 87 |
| anytls-uot/bytes=1200 | 5 | 1.062 | 0.887 | 183 → 87 |
| mux-smux/bytes=64 | 5 | 2.154 | 0.384 | 375 → 133 |
| mux-smux/bytes=1200 | 5 | 2.135 | 0.393 | 375 → 133 |
| mux-h2mux/bytes=64 | 5 | 2.032 | 0.462 | 203 → 108 |
| mux-h2mux/bytes=1200 | 5 | 1.901 | 0.500 | 210 → 115 |
| mux-yamux/bytes=64 | 5 | 1.535 | 0.646 | 159 → 91 |
| mux-yamux/bytes=1200 | 5 | 1.473 | 0.606 | 159 → 91 |
| hysteria2/bytes=64 | 5 | 1.010 | 0.998 | 424 → 288 |
| hysteria2/bytes=1200 | 5 | 1.022 | 0.953 | 782 → 493 |
| tuic4/bytes=64 | 5 | 1.044 | 0.898 | 360 → 299 |
| tuic4/bytes=1200 | 5 | 1.014 | 0.941 | 360 → 299 |
| tuic5/bytes=64 | 5 | 1.003 | 1.001 | 444 → 323 |
| tuic5/bytes=1200 | 4 | 1.026 | 0.913 | 784 → 560 |

### Anomalies and exact delivery boundary

The first, subsequently revised combined candidate had one SS-UoT/64 warmup
failure with zero timed requests. A 400-start reproduction series did not
repeat it, and the final 940-run matrix also did not repeat it. Its cause remains
undetermined; no production fix is claimed for that anomaly. The initial
upstream matrix also recorded a TUIC v5/1200 timeout. Both earlier series and
all diagnostics remain in the raw report.

The measured candidate executable has SHA-256
`0b1431be47123850b0d7aaee754aa4037903970b6895d422d494ede537f2da69`.
A late source audit preserved the scalar terminal-error guard for ShadowQUIC
UDP-over-stream and VLESS Reverse. Those paths are outside the 19-path matrix
and 26-case fixture. The exact measured/delivery runtime source delta is the two adapter
call sites plus `packetbatch/terminal.go` and its tests; all other measured
source hashes match. Focused race, low-memory and vet checks passed after that final delta, as did
ordinary, Naive/purego, Linux/amd64, Windows/amd64 and pinned-module builds.
This guard is covered separately for partial/full-error
progress, empty datagrams, close-once behavior, borrowed read lifetime and Close
interrupting a blocked writer. The delivery executable has its own hash in the
JSON and is not misidentified as the measured executable.

[Raw measurements, source patches, executable hashes and validation logs](mechanism-review-2026-11-01.json)
include every screening, confirmation, restoration, final and failed run.
