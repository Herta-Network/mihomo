# Configuration and GEO concurrency

Configuration changes publish immutable inbound policy and GEO settings snapshots.
The HTTP, SOCKS and mixed listeners decide admission and authentication bypass
from one policy version. Setters and getters copy caller-owned slices. Queue
empty checks and reads/removals share their lock; consumed entries are cleared.
Log level, IPv6 disable, HTTP user agent and resource ETag settings use synchronized
access. GEO loader registration is synchronized as well.

Controller recreation serializes ownership of the HTTP, TLS, Unix and named-pipe
servers. Listening happens before returning; closing explicitly releases the
listener and joins its serving goroutine, including a close before `Serve` starts.
Each router captures its configuration, CORS origins, UI path and embed mode.
TLS validation happens before opening its listener.

## GEO publication

1. A compare-and-swap admits one updater, including single-database requests.
   It captures URLs, mode, loader and home paths once, then downloads using the
   caller's context without holding the publication lock.
2. Data is validated and written to a same-directory temporary file, flushed and
   closed. A canceled download or invalid file leaves the current database intact.
3. Publication checks settings and home generation, checks cancellation, then
   renames the file and switches its reader/cache. After a successful rename,
   publication completes even if cancellation arrives immediately afterward.
4. MMDB queries hold a reader lease for their entire lookup. Replacement waits
   for leases before closing the old mapping. On Windows, a validated memory
   copy preserves the old database while its mapping is closed for replacement.
   If reopening a committed file fails, validated new bytes remain usable.

Initial GEO downloads use the same publication path. They do not overwrite a
valid database installed by another operation during the download. Existing
rule matchers remain immutable; new loads use the refreshed cache. Path queries
preserve actual filename casing without rewriting global filenames.

Embedders should cancel and join their updater during stop or scheduling changes.
`UpdateGeoDatabasesContext` propagates cancellation to HTTP body reads and waiting
publication. The updater must never acquire the embedder's lifecycle lock: the
stopping caller may hold that lock while waiting for the updater to exit.

## Regressions

| Boundary | Regression |
| --- | --- |
| Inbound policy publication and caller aliasing | `adapter/inbound/access_policy_test.go` |
| Public queue pop/last during concurrent mutation | `common/queue/concurrent_public_test.go` |
| Controller recreation, final stop, TLS failure and configuration ownership | `hub/route/server_lifecycle_test.go` |
| GEO settings, case-preserving paths and stale publication | `component/geodata/settings_concurrency_test.go`, `constant/path_concurrency_test.go` |
| Real HTTP update/configuration parsing overlap and duplicate admission | `component/updater/update_geo_concurrency_test.go` |
| MMDB lookup/reload, retained handles, failed commit and post-rename cancellation | `component/mmdb/reader_concurrency_test.go` |
| Initial download versus a newer publication and refreshed matchers | `component/geodata/files_test.go` |
| Log level and retained singleflight result concurrency | `log/log_concurrency_test.go`, `common/singleflight/store_test.go` |

```sh
go test -race ./adapter/inbound ./common/queue ./common/singleflight ./log ./constant \
  ./component/geodata/... ./component/mmdb ./component/updater ./hub/route ./hub/executor \
  -count=1 -timeout=120s
```

These regressions reproduce policy races/aliasing, a queue panic, controller races
and leaked listeners, GEO settings/path races, MMDB reload races and a missing
ASN-record panic on the previous implementation. Real download/configuration
overlap additionally exposed IPv6-setting and retained-singleflight races.
The focused suite passes locally on macOS arm64. CI runs it on Linux, macOS and
Windows; adding that CI step is not evidence that those remote jobs have run.

Solaris uses the same lifecycle for ordinary and race builds. Its controller
test no longer skips race builds, latency reads overlap background URL tests,
and real HTTP downloads are canceled by stop, disable and interval changes.
There is no GC/finalizer wait before applying a configuration. These changes
establish the tested lifecycle boundaries, not a claim that every upstream
protocol or third-party dependency is race-free.
