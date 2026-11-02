# Complete-process and protocol comparisons

Build this driver with `go build -o /tmp/mihomo-bench-driver ./scripts/benchmark-upstream`.
The driver launches disposable proxy processes and local echo backends. It checks
exact payloads, sequence identity and destinations, reports latency/rate/CPU/RSS,
and never retries application data. Only initial routing readiness can be retried;
those retries are reported separately. Temporary test certificates are generated
inside the disposable server home. TLS verification is disabled only in these
loopback benchmark configurations.

`run.py` repeats the shared upstream comparison: seven complete-process DIRECT
workloads plus the 26-case `BenchmarkUpstreamUDP` protocol fixture. Build the same
fixture into both test binaries. Each protocol operation contains four packets.

`run-chain.py` compares two complete configurations, each using the selected
binary at both ends: SOCKS5 → routing/NAT → protocol → routing/NAT → echo. The
matrix includes 64/1200-byte single packets, 1200-byte bursts of four, four
concurrent UDP associations, and 256 MiB TCP transfers. It covers DIRECT,
VMess/XUDP, VLESS, Shadowsocks native/UoT, AnyTLS-UoT, three mux backends,
Hysteria2, TUIC v4/v5 and Snell v3–v6 modes. One shared desktop benchmark series
should run at a time, without concurrent builds/tests.

```sh
python3 scripts/benchmark-upstream/run-chain.py \
  --driver /tmp/mihomo-bench-driver --base /tmp/mihomo-reference \
  --candidate /tmp/mihomo-candidate --trials 5 \
  --output /tmp/chain-comparison.json

python3 scripts/benchmark-upstream/run.py \
  --driver /tmp/mihomo-bench-driver --original /tmp/mihomo-upstream \
  --fork /tmp/mihomo-candidate --original-tests /tmp/upstream.test \
  --fork-tests /tmp/candidate.test --tcp-bytes 4294967296 --trials 5 \
  --output /tmp/upstream-comparison.json
```

Every run, including failures, is retained in JSON. Orders alternate by trial.
`run-chain.py` labels the versions `base`/`candidate`; `run.py` uses
`original`/`fork`. CPU includes process setup/shutdown. For two-proxy chains, sum
the client and server CPU values. The sum of their individual RSS peaks is only
an upper bound on simultaneous combined RSS. The process driver and protocol
fixture measure different layers and their absolute rates must not be mixed.

The [mechanism review](../../docs/benchmarks/mechanism-review-2026-11-01.md)
records ablations, retained fixes, removed mechanisms, exact source/binary hashes
and the development-workspace configuration used during measurement. The
reviewed revisions are now pinned in `go.mod`; current candidate builds can use
`GOWORK=off` without sibling checkouts.
