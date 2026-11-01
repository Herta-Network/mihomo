These are synthetic MMDB fixtures, with records only for 1.2.3.0/24. They contain
no third-party geolocation data. `generate.go.txt` uses
`github.com/maxmind/mmdbwriter v1.2.0` with a fixed build epoch of 1.

To reproduce, copy it to `main.go` in a temporary Go module, require that exact
writer version, and run `go run . /absolute/path/to/component/mmdb/testdata`.
