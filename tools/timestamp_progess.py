import sys
import time
import csv

start = time.perf_counter()
cur = {}

fields = [
    "wall_s",
    "frame",
    "fps",
    "out_time_ms",
    "dup_frames",
    "drop_frames",
    "speed",
    "progress",
]

writer = csv.DictWriter(sys.stdout, fieldnames=fields)
writer.writeheader()

for raw in sys.stdin:
    line = raw.strip()
    if "=" not in line:
        continue

    k, v = line.split("=", 1)
    cur[k] = v

    if k == "progress":
        row = {f: cur.get(f, "") for f in fields}
        row["wall_s"] = f"{time.perf_counter() - start:.3f}"
        writer.writerow(row)
        sys.stdout.flush()
        cur = {}import sys
import time
import csv

start = time.perf_counter()
cur = {}

fields = [
    "wall_s",
    "frame",
    "fps",
    "out_time_ms",
    "dup_frames",
    "drop_frames",
    "speed",
    "progress",
]

writer = csv.DictWriter(sys.stdout, fieldnames=fields)
writer.writeheader()

for raw in sys.stdin:
    line = raw.strip()
    if "=" not in line:
        continue

    k, v = line.split("=", 1)
    cur[k] = v

    if k == "progress":
        row = {f: cur.get(f, "") for f in fields}
        row["wall_s"] = f"{time.perf_counter() - start:.3f}"
        writer.writerow(row)
        sys.stdout.flush()
        cur = {}
