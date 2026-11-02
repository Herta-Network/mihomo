#!/usr/bin/env python3
"""Alternate two prebuilt mihomo versions; preserve every success and failure."""
import argparse
import json
import os
from pathlib import Path
import signal
import subprocess
import time


def execute(command, environment):
    started = time.monotonic()
    process = subprocess.Popen(command, env=environment, text=True,
                               stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                               start_new_session=True)
    try:
        output, _ = process.communicate(timeout=185)
    except subprocess.TimeoutExpired:
        os.killpg(process.pid, signal.SIGTERM)
        try:
            output, _ = process.communicate(timeout=5)
        except subprocess.TimeoutExpired:
            os.killpg(process.pid, signal.SIGKILL)
            output, _ = process.communicate()
        return {"exit_code": process.returncode, "wall_run_seconds": time.monotonic() - started,
                "timeout": True, "output": output}
    return {"exit_code": process.returncode, "wall_run_seconds": time.monotonic() - started,
            "output": output}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ("driver", "original", "fork", "output"):
        parser.add_argument("--" + name, required=True)
    parser.add_argument("--original-tests")
    parser.add_argument("--fork-tests")
    parser.add_argument("--trials", type=int, default=5)
    parser.add_argument("--protocol-iterations", type=int, default=2000)
    parser.add_argument("--tcp-bytes", type=int, default=512 * 1024 * 1024)
    parser.add_argument("--bulk-only", action="store_true")
    args = parser.parse_args()
    if args.trials < 1 or args.tcp_bytes < 8 * 65536 or args.tcp_bytes % (8 * 65536):
        parser.error("trials must be positive; tcp-bytes must be a multiple of 8 * 65536")
    if not args.bulk_only and not (args.original_tests and args.fork_tests):
        parser.error("protocol test binaries are required unless --bulk-only is used")
    environment = os.environ.copy()
    environment["GOMAXPROCS"] = "2"
    cases = [("tcp_bulk_c1", "tcp", 65536, 1, 1, args.tcp_bytes // 65536),
             ("tcp_bulk_c8", "tcp", 65536, 8, 1, args.tcp_bytes // (8 * 65536))]
    if not args.bulk_only:
        cases += [("tcp_small_c1", "tcp", 64, 1, 1, 10000),
                  ("udp64_single", "udp", 64, 1, 1, 10000),
                  ("udp1200_single", "udp", 1200, 1, 1, 10000),
                  ("udp1200_burst4", "udp", 1200, 1, 4, 3000),
                  ("udp1200_c4", "udp", 1200, 4, 1, 2500)]
    destination = Path(args.output)
    destination.parent.mkdir(parents=True, exist_ok=True)
    records = []

    def save(record):
        records.append(record)
        destination.write_text(json.dumps(records, indent=2) + "\n")
        print(record["trial"], record["suite"], record.get("case", "all"),
              record["side"], "exit", record["exit_code"], flush=True)

    for trial in range(1, args.trials + 1):
        order = ("original", "fork") if trial % 2 else ("fork", "original")
        for name, mode, size, connections, batch, rounds in cases:
            for side in order:
                command = [args.driver, "-binary", getattr(args, side), "-mode", mode,
                           "-size", str(size), "-connections", str(connections),
                           "-batch", str(batch), "-rounds", str(rounds)]
                record = execute(command, environment)
                try:
                    record["data"] = json.loads(record["output"])
                    del record["output"]
                except (json.JSONDecodeError, KeyError):
                    record["data"] = {"error": "driver did not return a result"}
                record.update(suite="process_long_tcp" if args.bulk_only else "process",
                              case=name, side=side, trial=trial)
                save(record)
        if not args.bulk_only:
            for side in order:
                command = [getattr(args, side + "_tests"), "-test.run=^$",
                           "-test.bench=^BenchmarkUpstreamUDP$", "-test.benchmem",
                           "-test.benchtime=" + str(args.protocol_iterations) + "x",
                           "-test.timeout=180s"]
                record = execute(command, environment)
                record.update(suite="protocol", side=side, trial=trial)
                save(record)


if __name__ == "__main__":
    main()
