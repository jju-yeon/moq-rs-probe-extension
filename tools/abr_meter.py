import sys
import time
import csv
import argparse

parser = argparse.ArgumentParser()
parser.add_argument("--read-size", type=int, default=4096)
args = parser.parse_args()

read_size = args.read_size

start = time.perf_counter()

writer = csv.writer(sys.stderr, lineterminator="\n")
writer.writerow([
    "fragment_id",
    "start_wall_s",
    "end_wall_s",
    "download_time_s",
    "fragment_bytes",
    "throughput_mbps",
    "gap_from_prev_fragment_s",
])

header = bytearray()
current_box_type = None
current_box_size = None
current_box_remaining = 0
current_box_start_time = None
current_box_header_len = 0

fragment_id = 0
fragment_open = False
fragment_start_time = None
fragment_bytes = 0
prev_fragment_end_time = None

def parse_box_header(buf):
    if len(buf) < 8:
        return None

    size = int.from_bytes(buf[0:4], "big")
    box_type = bytes(buf[4:8]).decode("ascii", errors="replace")

    if size == 1:
        if len(buf) < 16:
            return None
        size = int.from_bytes(buf[8:16], "big")
        header_len = 16
    elif size == 0:
        # streaming top-level box에서 보통 쓰지 않음
        raise RuntimeError("unsupported box size 0")
    else:
        header_len = 8

    if size < header_len:
        raise RuntimeError(f"invalid MP4 box size: {size}, type={box_type}")

    return size, box_type, header_len

def finish_box(box_type, box_size, box_start_time, box_end_time):
    global fragment_id
    global fragment_open
    global fragment_start_time
    global fragment_bytes
    global prev_fragment_end_time

    # fragment 시작: moof
    if box_type == "moof":
        fragment_open = True
        fragment_start_time = box_start_time
        fragment_bytes = box_size
        return

    # fragment 내부 box
    if fragment_open:
        fragment_bytes += box_size

        # fragment 종료: mdat 끝
        if box_type == "mdat":
            end_time = box_end_time
            download_time = end_time - fragment_start_time

            if download_time <= 0:
                throughput_mbps = 0.0
            else:
                throughput_mbps = fragment_bytes * 8 / download_time / 1_000_000

            if prev_fragment_end_time is None:
                gap = 0.0
            else:
                gap = fragment_start_time - prev_fragment_end_time

            writer.writerow([
                fragment_id,
                f"{fragment_start_time - start:.6f}",
                f"{end_time - start:.6f}",
                f"{download_time:.6f}",
                fragment_bytes,
                f"{throughput_mbps:.6f}",
                f"{gap:.6f}",
            ])
            sys.stderr.flush()

            fragment_id += 1
            prev_fragment_end_time = end_time
            fragment_open = False
            fragment_start_time = None
            fragment_bytes = 0

while True:
    chunk = sys.stdin.buffer.read(read_size)
    if not chunk:
        break

    now = time.perf_counter()

    # 원본 media stream은 그대로 전달
    sys.stdout.buffer.write(chunk)
    sys.stdout.buffer.flush()

    offset = 0
    n = len(chunk)

    while offset < n:
        # 새 box header 읽기
        if current_box_type is None:
            if not header:
                current_box_start_time = now

            need = 16 if len(header) >= 8 and int.from_bytes(header[0:4], "big") == 1 else 8
            take = min(need - len(header), n - offset)

            header.extend(chunk[offset:offset + take])
            offset += take

            parsed = parse_box_header(header)
            if parsed is None:
                continue

            current_box_size, current_box_type, current_box_header_len = parsed
            current_box_remaining = current_box_size - current_box_header_len

            # large-size header였으면 이미 16바이트까지 읽은 상태여야 함
            if len(header) > current_box_header_len:
                raise RuntimeError("internal parser error")

            header.clear()

            # payload 없는 box
            if current_box_remaining == 0:
                finish_box(
                    current_box_type,
                    current_box_size,
                    current_box_start_time,
                    now,
                )
                current_box_type = None
                current_box_size = None
                current_box_start_time = None
                current_box_header_len = 0

        # 현재 box payload 읽기
        if current_box_type is not None and current_box_remaining > 0:
            take = min(current_box_remaining, n - offset)
            offset += take
            current_box_remaining -= take

            if current_box_remaining == 0:
                finish_box(
                    current_box_type,
                    current_box_size,
                    current_box_start_time,
                    now,
                )
                current_box_type = None
                current_box_size = None
                current_box_start_time = None
                current_box_header_len = 0