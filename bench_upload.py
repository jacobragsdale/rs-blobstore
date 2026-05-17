#!/usr/bin/env python3
"""Send 100 50MB pickle blobs to rs-blobstore in parallel and time responses."""
import pickle
import time
import urllib.request
import urllib.error
from concurrent.futures import ThreadPoolExecutor, as_completed

URL_BASE = "http://localhost:8080/blobs"
NUM_FILES = 100
TARGET_SIZE = 50 * 1024 * 1024  # 50 MiB
PARALLELISM = 16


def make_pickle_payload(target_bytes: int) -> bytes:
    """Build a pickle whose serialized form is ~target_bytes."""
    # Pickle of a bytes object adds ~20 bytes of framing; subtract a small pad.
    raw = b"\x00" * (target_bytes - 32)
    blob = pickle.dumps(raw, protocol=pickle.HIGHEST_PROTOCOL)
    # Pad/trim to exactly target_bytes by wrapping in a tuple with filler if needed.
    if len(blob) < target_bytes:
        blob = blob + b"\x00" * (target_bytes - len(blob))
    return blob[:target_bytes]


def upload(i: int, payload: bytes, t_zero: float) -> tuple[int, int, float, float, float, str]:
    url = f"{URL_BASE}/bench/{i:04d}.pkl"
    req = urllib.request.Request(
        url,
        data=payload,
        method="POST",
        headers={"Content-Type": "application/octet-stream"},
    )
    t_start = time.perf_counter()
    try:
        with urllib.request.urlopen(req, timeout=120) as resp:
            status = resp.status
            body = resp.read().decode("utf-8", errors="replace")
    except urllib.error.HTTPError as e:
        status = e.code
        body = e.read().decode("utf-8", errors="replace")
    t_end = time.perf_counter()
    return i, status, t_start - t_zero, t_end - t_zero, t_end - t_start, body


def main() -> None:
    print(f"Building {TARGET_SIZE / 1024 / 1024:.1f} MiB pickle payload...")
    payload = make_pickle_payload(TARGET_SIZE)
    print(f"Payload size: {len(payload):,} bytes")

    print(f"Posting {NUM_FILES} files with parallelism={PARALLELISM}...")
    records: list[tuple[int, int, float, float, float]] = []  # (i, status, start, end, latency)
    statuses: dict[int, int] = {}

    wall_start = time.perf_counter()
    with ThreadPoolExecutor(max_workers=PARALLELISM) as pool:
        futures = [pool.submit(upload, i, payload, wall_start) for i in range(NUM_FILES)]
        for fut in as_completed(futures):
            i, status, t_start, t_end, dt, body = fut.result()
            records.append((i, status, t_start, t_end, dt))
            statuses[status] = statuses.get(status, 0) + 1
            if status >= 400:
                print(f"  [{i}] HTTP {status} after {dt * 1000:.0f} ms: {body.strip()}")
    wall = time.perf_counter() - wall_start
    latencies = sorted(r[4] for r in records)

    n = len(latencies)

    def pct(p: float) -> float:
        return latencies[min(n - 1, int(p / 100 * n))]

    total_bytes = len(payload) * NUM_FILES
    print()
    print(f"Status codes:     {statuses}")
    print(f"Wall time:        {wall:.2f} s")
    print(f"Total uploaded:   {total_bytes / 1024 / 1024:.1f} MiB")
    print(f"Throughput:       {total_bytes / 1024 / 1024 / wall:.1f} MiB/s")
    print()
    print("Per-request latency (POST -> 202):")
    print(f"  min   = {latencies[0] * 1000:.0f} ms")
    print(f"  p50   = {pct(50) * 1000:.0f} ms")
    print(f"  p90   = {pct(90) * 1000:.0f} ms")
    print(f"  p99   = {pct(99) * 1000:.0f} ms")
    print(f"  max   = {latencies[-1] * 1000:.0f} ms")
    print(f"  mean  = {sum(latencies) / n * 1000:.0f} ms")

    # Trend check: average latency per quartile of *start time*.
    by_start = sorted(records, key=lambda r: r[2])
    print()
    print("Latency by start-order quartile (Q1 = first 25 to start):")
    for q in range(4):
        chunk = by_start[q * n // 4 : (q + 1) * n // 4]
        lats = [r[4] for r in chunk]
        print(
            f"  Q{q + 1}  start={chunk[0][2]:.2f}-{chunk[-1][2]:.2f}s  "
            f"mean={sum(lats) / len(lats) * 1000:.0f} ms  "
            f"max={max(lats) * 1000:.0f} ms"
        )

    slowest = max(records, key=lambda r: r[4])
    print(
        f"\nSlowest request: i={slowest[0]}  started @ {slowest[2]:.2f}s  "
        f"finished @ {slowest[3]:.2f}s  latency={slowest[4] * 1000:.0f} ms"
    )


if __name__ == "__main__":
    main()
