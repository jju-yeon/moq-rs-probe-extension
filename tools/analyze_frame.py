import sys
import time
import csv
import argparse

parser = argparse.ArgumentParser()
parser.add_argument("--period", type=float, default=1.0)
parser.add_argument("--stall-threshold", type=float, default=1.5)
args = parser.parse_args()

start = time.perf_counter()
cur = {}
prev = None

writer = csv.writer(sys.stdout, lineterminator="\n")
writer.writerow([
    "wall_s",
    "frame",
    "fps",
    "out_time_s",
    "dup_frames",
    "drop_frames",
    "speed",
    "wall_delta_s",
    "frame_delta",
    "out_delta_s",
    "gap_stall",
    "no_progress_stall",
    "stall_flag",
])

def to_int(v, default=0):
    try:
        return int(v)
    except:
        return default

def to_float(v, default=0.0):
    try:
        return float(str(v).replace("x", ""))
    except:
        return default

for raw in sys.stdin:
    line = raw.strip()
    if "=" not in line:
        continue

    k, v = line.split("=", 1)
    cur[k] = v

    if k != "progress":
        continue

    now = time.perf_counter()
    wall_s = now - start

    frame = to_int(cur.get("frame"))
    fps = to_float(cur.get("fps"))

    # ffmpeg progress에서 out_time_us가 있으면 우선 사용
    if "out_time_us" in cur:
        out_time_s = to_int(cur.get("out_time_us")) / 1_000_000
    elif "out_time_ms" in cur:
        # ffmpeg progress의 out_time_ms는 실제로 microsecond 값처럼 나오는 경우가 있음
        out_time_s = to_int(cur.get("out_time_ms")) / 1_000_000
    else:
        out_time_s = 0.0

    dup_frames = to_int(cur.get("dup_frames"))
    drop_frames = to_int(cur.get("drop_frames"))
    speed = to_float(cur.get("speed"))

    if prev is None:
        wall_delta_s = 0.0
        frame_delta = 0
        out_delta_s = 0.0
        gap_stall = 0
        no_progress_stall = 0
        stall_flag = 0
    else:
        wall_delta_s = wall_s - prev["wall_s"]
        frame_delta = frame - prev["frame"]
        out_delta_s = out_time_s - prev["out_time_s"]

        # progress 로그 간격이 비정상적으로 길면 stall 후보
        gap_stall = 1 if wall_delta_s >= args.stall_threshold else 0

        # 시간이 지났는데 frame/out_time이 안 늘면 stall
        no_progress_stall = 1 if frame_delta <= 0 or out_delta_s <= 0 else 0

        stall_flag = 1 if gap_stall or no_progress_stall else 0

    writer.writerow([
        f"{wall_s:.3f}",
        frame,
        f"{fps:.3f}",
        f"{out_time_s:.6f}",
        dup_frames,
        drop_frames,
        f"{speed:.3f}",
        f"{wall_delta_s:.3f}",
        frame_delta,
        f"{out_delta_s:.6f}",
        gap_stall,
        no_progress_stall,
        stall_flag,
    ])
    sys.stdout.flush()

    prev = {
        "wall_s": wall_s,
        "frame": frame,
        "out_time_s": out_time_s,
    }

    cur = {}