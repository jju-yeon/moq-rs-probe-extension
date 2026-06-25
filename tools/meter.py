import sys
import time

interval = 1.0
total = 0
window = 0

start = time.perf_counter()
last = start

while True:
    chunk = sys.stdin.buffer.read(65536)
    if not chunk:
        break

    n = len(chunk)
    total += n
    window += n

    sys.stdout.buffer.write(chunk)
    sys.stdout.buffer.flush()

    now = time.perf_counter()
    if now - last >= interval:
        inst_mbps = window * 8 / (now - last) / 1_000_000
        avg_mbps = total * 8 / (now - start) / 1_000_000

        print(
            f"time={now-start:.3f}s bytes={total} inst_mbps={inst_mbps:.3f} avg_mbps={avg_mbps:.3f}",
            file=sys.stderr,
            flush=True,
        )

        window = 0
        last = now
