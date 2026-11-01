# UoT, sing-mux and VMess framing measurements

Baseline revisions are sing `b5e7e17f97a0`, sing-mux `c0be7c45926a` and
sing-vmess `12f108ffaf84`. Updated revisions are `1d1b6994cf4a`, `9a83d0dd7caa`
and `83a9fdf3e38e`. Mihomo's stream encoder is unchanged from `73ff2c28929a`;
its added benchmark fixtures exercise those codecs through the production stream encoder.

Apple M5, macOS arm64, Go 1.27.3, `GOMAXPROCS=2`. Each case has three 500 ms
samples. Baseline and updated binaries use the same fixtures; source overlays
select baseline protocol files. Binaries were built first, then run sequentially,
with no other builds or tests launched by this task during measurement. Tables
report medians; [all samples](protocol-framing-2026-11-01.json) are retained.
This is a shared workstation and short local runs, not a controlled network
throughput study. Allocation reductions are the primary result; timing changes
should not be extrapolated to production.

## Owned encoding

Each operation includes allocation of the producer's owned 1200-byte packet
buffers, protocol encoding and a discard transport write. Headroom is reserved
before payload production. VMess uses AES-128-GCM except the explicit legacy
CFB case; normal protocol length masking remains enabled. Packet creation and
encoding are included, while connection setup is outside the timed loop.

| Case | Packets/op | Before ns/op | After ns/op | Before B/op | After B/op | Before allocs/op | After allocs/op |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| UoT connect=true | 1 | 56.47 | 29.82 | 128 | 64 | 2 | 1 |
| UoT connect=true | 8 | 524.3 | 361.2 | 1025 | 512 | 16 | 8 |
| UoT connect=false | 1 | 89.87 | 65.96 | 128 | 64 | 2 | 1 |
| UoT connect=false | 8 | 812.8 | 803.2 | 1025 | 512 | 16 | 8 |
| sing-mux address mode | 1 | 105.9 | 89.75 | 152 | 64 | 4 | 1 |
| sing-mux address mode | 8 | 583.4 | 518.8 | 1041 | 512 | 17 | 8 |
| VMess udp | 1 | 238.9 | 230 | 66 | 64 | 2 | 1 |
| VMess udp | 8 | 2128 | 1832 | 528 | 512 | 16 | 8 |
| VMess xudp | 1 | 282.4 | 694.3 | 130 | 64 | 3 | 1 |
| VMess xudp | 8 | 2223 | 2407 | 1041 | 512 | 24 | 8 |
| VMess packetaddr | 1 | 284.9 | 283 | 130 | 64 | 3 | 1 |
| VMess packetaddr | 8 | 2231 | 2243 | 1041 | 512 | 24 | 8 |
| VMess legacy | 1 | 2305 | 2631 | 194 | 64 | 4 | 1 |
| VMess legacy | 8 | 16912 | 16682 | 1555 | 513 | 32 | 8 |

One allocation per packet is the producer's buffer descriptor. This is not
zero-allocation end-to-end relaying. UoT's no-headroom fallback cases, included
in the JSON, still allocate separate storage. Oversized XUDP frames use VMess
chunk workspaces. Opaque joined transports still copy encoded batch bytes and
allocate a joined block when it exceeds the pool limit.

## Real TCP round trips

UoT uses a loopback TCP echo. Sing-mux uses actual h2mux/smux/yamux sessions and
both packet endpoints. VMess uses mihomo's actual inbound, outbound, reference,
deadline and traffic wrappers plus owned reply delivery. It excludes routing
and the tunnel sender queue. VMess checks payloads, addresses and exact upload/
download totals. Each request packet contains 1200 bytes; the fixed benchmark
UUID is `00000000-0000-0000-0000-000000000001`.

| Case | Packets/op | Before ns/op | After ns/op | Before B/op | After B/op | Before allocs/op | After allocs/op |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| UoT TCP echo | 1 | 15940 | 15537 | 232 | 112 | 6 | 2 |
| h2mux | 1 | 28216 | 27047 | 505 | 328 | 20 | 14 |
| h2mux | 8 | 131702 | 81831 | 3558 | 2312 | 144 | 111 |
| smux | 1 | 26481 | 27676 | 5473 | 5297 | 46 | 40 |
| smux | 8 | 52310 | 59090 | 40119 | 38872 | 242 | 209 |
| yamux | 1 | 24175 | 26358 | 328 | 152 | 16 | 10 |
| yamux | 8 | 39445 | 43448 | 2453 | 1219 | 113 | 80 |
| VMess udp-aes | 4 | 40408 | 38920 | 5134 | 5106 | 108 | 100 |
| VMess udp-cfb | 4 | 90538 | 102603 | 13862 | 5111 | 112 | 92 |
| VMess xudp | 4 | 60847 | 75676 | 6818 | 6549 | 236 | 224 |
| VMess packetaddr | 4 | 58360 | 96684 | 5644 | 5102 | 116 | 100 |

The remaining allocations include both endpoints and wrapper/read-side work.
For example, the four-packet VMess XUDP loop still allocates substantially more
than its isolated sender. Native QUIC/Hysteria2/TUIC optimization is outside
this framing change; these TCP figures say nothing about their performance.

## Timing follow-up

The sequential run contained large slow samples, including a 694 ns XUDP
sender median. A follow-up alternated baseline and updated binaries for each
case and reversed their order on the second repeat. All 30 additional samples
are included in the JSON. Allocation counts stayed unchanged.

| Case | Before median ns/op | After median ns/op | Before range | After range |
| --- | ---: | ---: | --- | --- |
| Encoding xudp | 275.1 | 260.7 | 273.9–303.7 | 254.7–271.9 |
| Encoding legacy | 2154 | 2134 | 2146–2307 | 2071–2544 |
| Round trip xudp | 52399 | 55089 | 45542–92191 | 51704–62561 |
| Round trip packetaddr | 45799 | 58026 | 43244–119784 | 44371–101339 |
| Round trip udp-cfb | 82000 | 81824 | 71299–121802 | 66837–83190 |

The XUDP sender slowdown did not repeat. Round-trip times varied widely in
both revisions, and some updated medians remained slower. These samples do
not establish a latency or throughput improvement; the demonstrated result is
reduced allocation and preserved protocol/ownership behavior.

## Reproduce

Run each command in the named repository with `GOMAXPROCS=2`, adding
`-benchmem -benchtime=500ms -count=3`. Avoid concurrent builds/tests.

```sh
# sing
go test ./common/uot -run '^$' -bench '^BenchmarkOwned'
# sing-mux
go test . -run '^$' -bench '^BenchmarkPacket'
# mihomo
go test ./common/net/packetbatch -run '^$' -bench '^BenchmarkVMessOwnedFrames/.*/^size=1200$/'
go test ./listener/inbound -run '^$' -bench '^BenchmarkVMessOwnedRoundTrip'
```

Regression coverage preserves empty/large packets, IPv4/IPv6/domain headers,
owned-buffer release, borrowed input and payload counts, partial-write prefix
counts, failed-stream poisoning, concurrent writes and blocked close. Cipher
round trips include legacy, AES-GCM and ChaCha20, masked/padded and authenticated
length records. The legacy baseline reproduced a transfer to ordinary `Write`,
XUDP validation retained its input, and packetaddr returned address-header bytes
in the borrowed API's count; dedicated regressions now pass.
