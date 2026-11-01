#!/usr/bin/env python3
"""Reject a second sing module/type identity in the production dependency graph."""
import json
from pathlib import Path
import subprocess

ROOT = Path(__file__).resolve().parent.parent
EXPECTED = "github.com/metacubex/sing"
FORK = "github.com/Herta-Network/sing"
IDENTITIES = {EXPECTED, FORK, "github.com/sagernet/sing"}


def go(*args):
    return subprocess.check_output(["go", *args], cwd=ROOT, text=True)


def json_stream(raw):
    decoder = json.JSONDecoder()
    while raw.strip():
        item, end = decoder.raw_decode(raw.lstrip())
        yield item
        raw = raw.lstrip()[end:]


modules = [m for m in json_stream(go("list", "-mod=readonly", "-m", "-json", "all"))
           if m["Path"] in IDENTITIES]
if len(modules) != 1 or modules[0]["Path"] != EXPECTED:
    raise SystemExit(f"Expected one sing module identity, found: {modules}")
replacement = modules[0].get("Replace", {})
if replacement.get("Path") != FORK or not replacement.get("Version"):
    raise SystemExit(f"sing must use a fixed remote fork revision: {replacement}")
imports = go("list", "-mod=readonly", "-tags", "with_naive,with_purego", "-deps",
             "-f", "{{.ImportPath}}", "./...").splitlines()
foreign = [p for p in imports if any(p == identity or p.startswith(identity + "/")
                                   for identity in IDENTITIES - {EXPECTED})]
if foreign:
    raise SystemExit("Unexpected sing package identity: " + ", ".join(foreign))
print(f"Single sing source: {EXPECTED} => {FORK}@{replacement['Version']}")
