#!/usr/bin/env python3
"""Mark findings fixed: mark.py <suite> <seed> <note>. Marks every open record of that seed."""
import json, sys, pathlib
path = pathlib.Path(__file__).with_name("dst.jsonl")
suite, seed, note = sys.argv[1], int(sys.argv[2]), sys.argv[3]
rows = [json.loads(line) for line in path.read_text().splitlines() if line.strip()]
n = 0
for row in rows:
    if row["suite"] == suite and row["seed"] == seed and row["status"] == "open":
        row["status"], row["note"] = "fixed", note
        n += 1
path.write_text("".join(json.dumps(row) + "\n" for row in rows))
print(f"marked {n}")
