# Upstream mihomo v1.19.33 comparison

Follow-up: [UDP/TCP regression repair and rerun](regression-repair-2026-11-01.md).
This page preserves the results for the original tested commit.

The optimized fork does **not** provide a uniform performance improvement.
Protocol allocation events fall, mux round trips improve substantially, but
full-process bulk TCP throughput is lower in this test. More seriously, the
published fork has a reproducible **SOCKS5 UDP reply regression** and cannot
complete any of the four full-process UDP workloads below.

Compare upstream [v1.19.33 / `3f27ef39`](https://github.com/MetaCubeX/mihomo/commit/3f27ef39f8c96bbfbc8b0b9bb10f5c9ef60149d8)
with Herta-Network [`71141626`](https://github.com/Herta-Network/mihomo/commit/711416260a870fcbbb1a8780fb0c18fd6145af01).
The upstream release, current `Meta` head and the fork's common ancestor all
resolve to that same upstream commit, verified on 2026-11-01. This measures the
cumulative fork difference, not just the last optimization commit.

## Environment and interpretation

Apple M5, macOS 27.0.1 arm64, Go 1.27.3, `CGO_ENABLED=0`, `GOWORK=off`,
`GOMAXPROCS=2`, identical build flags and each revision's committed dependencies.
These are source builds, not a comparison between binaries built with different
release toolchains. Five alternating original/fork and fork/original trials were
run serially. This task launched no concurrent builds or tests while measuring;
the workstation was otherwise shared. No Linux/Windows runtime, TUN, device,
WAN, high-RTT or packet-loss-emulation result is implied.

[All raw trials, failures, hashes and diagnostics](upstream-comparison-2026-11-01.json)
are retained. Tables show medians. MB/s is decimal **request payload** rate;
replies and protocol headers are additional traffic. CPU time and peak RSS for
process tests come from the proxy child process, including startup and warmup.
They are not the driver's memory or allocation counts. The fork includes new
features as well as optimizations, so footprint changes cannot all be assigned
to one optimization.

## Critical full-process UDP finding

The standard SOCKS5 interface, actual tunnel routing, NAT state, packet sender
and direct outbound were exercised against a local UDP echo server. Every one
of the **20 original runs completed**. Every one of the **20 fork runs failed**
in warmup or after a prefix of measured packets. No failed prefix is presented
as a successful throughput sample.

| Workload | Original complete runs | Fork complete runs | Fork observation |
| --- | ---: | ---: | --- |
| 64 B, one connection, one packet per round | 5/5 | 0/5 | A packet eventually times out. |
| 1200 B, one connection, one packet per round | 5/5 | 0/5 | A packet eventually times out. |
| 1200 B, one connection, four-packet burst | 5/5 | 0/5 | Only one of the four replies arrives; three are lost. |
| 1200 B, four connections, one packet per round | 5/5 | 0/5 | Packet loss during warmup or the timed prefix. |

The cause is confirmed by a diagnostic build:

- [`listener/socks/utils.go`](../../listener/socks/utils.go) returns the number
  of bytes written on the wire, including the SOCKS5 UDP header: **1210** for
  a 1200-byte IPv4 payload.
- [`constant/packet_owned.go`](../../constant/packet_owned.go) compares that
  count with the payload length, **1200**, and returns `io.ErrShortWrite`.
- [`handleUDPToLocal`](../../tunnel/connection.go) closes the flow on that error.
  The first reply has already been sent, but subsequent responses are discarded
  and later requests can race flow teardown.

The echo server received all five requests in the diagnostic sequence (one
initial warmup packet plus a four-packet burst). Logs then showed the downlink
reporting `short write` after each first reply. A temporary source overlay that
normalizes a complete SOCKS5 wire write to the original payload count delivered
**4000/4000 packet round trips, zero loss or corruption**. This is a causal
control, **not** the published fork and **not** included in its performance
figures. Runtime source was not changed as part of this measurement task.

This regression must be repaired before treating the protocol microbenchmarks
as evidence of a faster working SOCKS5 UDP proxy. Its performance impact is not
limited to burst mode: single-packet tests also lose packets during flow churn.

## Full-process TCP

The path is SOCKS5 → mihomo routing/relay → DIRECT → loopback TCP echo. Initial
512 MiB runs suggested a bulk-throughput regression, so bulk cases were extended
to **4 GiB request payload per run**, with matching replies. Data is written in
64 KiB units. Eight-connection tests divide the same aggregate byte count across
eight flows. Every completed run validates all returned bytes.

| Workload | Original MB/s | Fork MB/s | Rate change | p99 µs, original → fork | Proxy CPU seconds | Peak RSS MiB |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| TCP 64 KiB, 1 connection | 1112.1 | 1029.6 | -7.4% | 130.0 → 96.3 | 2.643 → 2.739 | 29.81 → 30.17 |
| TCP 64 KiB, 8 connections | 2538.0 | 2002.1 | -21.1% | 352.6 → 443.2 | 2.788 → 3.750 | 29.89 → 30.28 |

The one-connection table uses matched trials 1, 2, 3, 5 and supplementary trial 6.
Fork trial 4 failed with EOF during connection setup, before any measured
request; its cause was not established. The failed run and its unused original
partner remain in the raw data. The supplementary driver also checked that the
UDP port was free before starting the combined TCP/UDP SOCKS listener; timed I/O
was unchanged. Eight-connection results use trials 1–5. The fork was slower in
all successful matched bulk pairs. The initial shorter runs are retained too.

The 64-byte, single-connection TCP control was essentially unchanged: **1.692 →
1.694 MB/s**, with p99 **63.50 → 63.33 µs**. Thus the bulk regression should not
be generalized to every TCP latency workload. Its implementation-level cause
has not been profiled or attributed in this task. Peak RSS also does not show
an across-the-board memory reduction.

## Shared protocol round trips

The exact same benchmark source was compiled in both revisions. Each operation
sends four owned producer buffers, then receives four replies; unique packet IDs,
payload bytes, source addresses and traffic counters are checked. Both sides
have identical 512-byte front space and 1024-byte rear capacity. The fork uses
explicit counted/owned interfaces when available; upstream retains its actual
legacy `WriteTo`/`WaitReadFrom` path. Capability discovery happens outside timing.

Both protocol endpoints run the selected revision in one process. The actual
inbound/outbound, reference, deadline and traffic wrappers are included; **tunnel
routing, NAT and the packet sender are bypassed**. That distinction explains why
these checks pass despite the full SOCKS5 UDP defect. Setup and ten warmup rounds
are excluded. Each of five trials runs 2000 timed operations per case. All 26
cases completed on both versions in all final trials with exact data/accounting.

Only common features are compared: no Naive, Snell v4/v6, or VLESS reverse row
is invented for upstream. Encryption, credentials, TLS fixtures and mux choices
are identical. These are bounded four-packet round trips, not saturated
single-endpoint WAN throughput tests.

### 1200-byte packets, four packets per operation

| Protocol | µs/op, original → fork | Time change | Allocations/op | Allocation reduction | B/op | p99 µs |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| vmess-aes | 36.85 → 39.54 | +7.3% | 151 → 88 | 41.7% | 6382 → 4661 | 64.21 → 64.79 |
| vmess-xudp | 49.55 → 44.38 | -10.4% | 303 → 96 | 68.3% | 8196 → 4836 | 83.96 → 70.46 |
| vless | 61.51 → 55.41 | -9.9% | 263 → 80 | 69.6% | 7426 → 4805 | 105.88 → 82.04 |
| ss-udp | 74.00 → 70.82 | -4.3% | 462 → 386 | 16.5% | 45953 → 48708 | 158.75 → 160.42 |
| ss-uot | 39.66 → 39.40 | -0.7% | 179 → 66 | 63.1% | 6967 → 4649 | 65.92 → 59.54 |
| snell3 | 37.85 → 34.49 | -8.9% | 67 → 32 | 52.2% | 8896 → 3265 | 72.38 → 53.00 |
| anytls-uot | 55.72 → 54.45 | -2.3% | 183 → 64 | 65.0% | 6966 → 4483 | 99.92 → 87.21 |
| mux-smux | 131.61 → 47.95 | -63.6% | 374 → 74 | 80.2% | 17428 → 5016 | 306.12 → 77.75 |
| mux-h2mux | 115.87 → 59.91 | -48.3% | 210 → 80 | 61.9% | 7836 → 4864 | 207.08 → 99.71 |
| mux-yamux | 70.48 → 50.66 | -28.1% | 159 → 56 | 64.8% | 5761 → 4073 | 125.33 → 100.96 |
| hysteria2 | 133.10 → 131.94 | -0.9% | 781 → 494 | 36.7% | 56232 → 50370 | 282.67 → 271.62 |
| tuic4 | 134.66 → 105.92 | -21.3% | 359 → 313 | 12.8% | 56781 → 46658 | 330.21 → 254.46 |
| tuic5 | 149.39 → 146.51 | -1.9% | 784 → 573 | 26.9% | 77191 → 66473 | 333.83 → 294.12 |

At this packet size allocation events fall by about **13–80%**. This is not the
same as lower retained memory: Shadowsocks native UDP allocates fewer objects
but more total bytes per operation. Smux and h2mux show the clearest round-trip
gains. VMess AES becomes slightly slower; UoT, AnyTLS and native QUIC timings do
not improve in proportion to their allocation reductions. These measurements
cannot be added to the percentage changes from earlier reports because those
reports used different baselines and producer/fixture details.

### 64-byte control, four packets per operation

| Protocol | µs/op, original → fork | Time change | Allocations/op | Allocation reduction | B/op | p99 µs |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| vmess-aes | 37.25 → 39.12 | +5.0% | 151 → 88 | 41.7% | 6384 → 4664 | 65.08 → 63.62 |
| vmess-xudp | 44.70 → 41.55 | -7.0% | 303 → 96 | 68.3% | 8200 → 4838 | 83.04 → 68.62 |
| vless | 58.28 → 54.46 | -6.6% | 263 → 80 | 69.6% | 7424 → 4806 | 97.50 → 78.54 |
| ss-udp | 63.02 → 70.74 | +12.2% | 462 → 386 | 16.5% | 45953 → 48826 | 146.00 → 165.08 |
| ss-uot | 39.70 → 36.41 | -8.3% | 179 → 66 | 63.1% | 6966 → 4650 | 64.96 → 74.62 |
| snell3 | 34.02 → 31.00 | -8.9% | 67 → 32 | 52.2% | 4003 → 3265 | 60.58 → 47.29 |
| anytls-uot | 50.95 → 51.45 | +1.0% | 183 → 64 | 65.0% | 6957 → 4483 | 96.00 → 87.58 |
| mux-smux | 101.80 → 46.53 | -54.3% | 374 → 73 | 80.5% | 17415 → 5006 | 257.75 → 81.46 |
| mux-h2mux | 102.66 → 51.36 | -50.0% | 203 → 73 | 64.0% | 7704 → 4732 | 183.88 → 86.92 |
| mux-yamux | 66.82 → 44.22 | -33.8% | 159 → 56 | 64.8% | 5743 → 4070 | 124.17 → 70.50 |
| hysteria2 | 80.63 → 86.81 | +7.7% | 425 → 290 | 31.8% | 15421 → 12894 | 168.38 → 180.58 |
| tuic4 | 88.25 → 86.95 | -1.5% | 360 → 313 | 13.1% | 13368 → 12970 | 187.29 → 160.54 |
| tuic5 | 88.88 → 87.59 | -1.5% | 444 → 337 | 24.1% | 14484 → 14164 | 183.96 → 176.75 |

## Reproduction

Keep the upstream production source and dependency files unchanged. Copy only
`listener/inbound/upstream_comparison_benchmark_test.go` into its detached checkout.

```sh
git worktree add --detach ../mihomo-upstream-perf 3f27ef39f8c96bbfbc8b0b9bb10f5c9ef60149d8
cp listener/inbound/upstream_comparison_benchmark_test.go ../mihomo-upstream-perf/listener/inbound/
GOWORK=off CGO_ENABLED=0 go build -o /tmp/mihomo-fork .
GOWORK=off CGO_ENABLED=0 go test -c -o /tmp/mihomo-fork.test ./listener/inbound
(cd ../mihomo-upstream-perf && GOWORK=off CGO_ENABLED=0 go build -o /tmp/mihomo-original .)
(cd ../mihomo-upstream-perf && GOWORK=off CGO_ENABLED=0 go test -c -o /tmp/mihomo-original.test ./listener/inbound)
GOWORK=off CGO_ENABLED=0 go build -o /tmp/mihomo-bench-driver ./scripts/benchmark-upstream
python3 scripts/benchmark-upstream/run.py --driver /tmp/mihomo-bench-driver \
  --original /tmp/mihomo-original --fork /tmp/mihomo-fork \
  --original-tests /tmp/mihomo-original.test --fork-tests /tmp/mihomo-fork.test \
  --output /tmp/mihomo-comparison.json
python3 scripts/benchmark-upstream/run.py --driver /tmp/mihomo-bench-driver \
  --original /tmp/mihomo-original --fork /tmp/mihomo-fork \
  --bulk-only --tcp-bytes 4294967296 --output /tmp/mihomo-tcp-long.json
```

To reproduce the UDP regression alone:

```sh
GOMAXPROCS=2 /tmp/mihomo-bench-driver -binary /tmp/mihomo-fork \
  -mode udp -size 1200 -connections 1 -batch 4 -rounds 1000
```

The driver binds only loopback listeners, uses fresh temporary configurations,
checks every reply, stops a failed UDP workload without retrying lost data, and
terminates its proxy child before removing its own temporary files. The runner
preserves failed runs instead of converting them into performance scores.
