#!/usr/bin/env python3
"""Send pickle blobs to rs-blobstore in parallel and time responses."""
import http.client
import os
import pickle
import shutil
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from urllib.parse import urlsplit

URL_BASE = os.environ.get("URL_BASE", "http://localhost:8080/blobs").rstrip("/")
STORAGE_ROOT = Path(os.environ.get("STORAGE_ROOT", "./data"))
BLOB_PREFIX = os.environ.get("BLOB_PREFIX", "bench").strip("/")
NUM_FILES = int(os.environ.get("NUM_FILES", "100"))
TARGET_SIZE = int(os.environ.get("TARGET_SIZE_BYTES", str(50 * 1024 * 1024)))
PARALLELISM = int(os.environ.get("PARALLELISM", "16"))
CLEANUP_WRITTEN_DATA = os.environ.get("CLEANUP_WRITTEN_DATA", "1") != "0"
WRITE_WAIT_TIMEOUT = float(os.environ.get("WRITE_WAIT_TIMEOUT_SECONDS", "120"))
REQUEST_TIMEOUT = float(os.environ.get("REQUEST_TIMEOUT_SECONDS", "120"))
UPLOAD_CHUNK_SIZE = int(os.environ.get("UPLOAD_CHUNK_SIZE_BYTES", str(1024 * 1024)))

URL = urlsplit(URL_BASE)
if URL.scheme not in {"http", "https"} or not URL.hostname:
    raise ValueError(f"URL_BASE must be an http(s) URL, got {URL_BASE!r}")


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
    path = f"{URL.path.rstrip('/')}/{BLOB_PREFIX}/{i:04d}.pkl"
    conn_cls = http.client.HTTPSConnection if URL.scheme == "https" else http.client.HTTPConnection
    t_start = time.perf_counter()
    conn = conn_cls(URL.hostname, URL.port, timeout=REQUEST_TIMEOUT)
    try:
        conn.putrequest("POST", path)
        conn.putheader("Content-Type", "application/octet-stream")
        conn.putheader("Content-Length", str(len(payload)))
        conn.putheader("Connection", "close")
        conn.endheaders()

        view = memoryview(payload)
        for offset in range(0, len(payload), UPLOAD_CHUNK_SIZE):
            conn.send(view[offset : offset + UPLOAD_CHUNK_SIZE])

        resp = conn.getresponse()
        status = resp.status
        body = resp.read().decode("utf-8", errors="replace")
    except (http.client.HTTPException, OSError, TimeoutError) as e:
        status = 0
        body = str(e)
    finally:
        conn.close()
    t_end = time.perf_counter()
    return i, status, t_start - t_zero, t_end - t_zero, t_end - t_start, body


def bench_dir() -> Path:
    root = STORAGE_ROOT.resolve()
    path = (root / BLOB_PREFIX).resolve()
    if path == root or root not in path.parents:
        raise ValueError(f"refusing unsafe cleanup path: {path}")
    return path


def wait_for_writes(successful_ids: list[int], payload_size: int) -> None:
    if not CLEANUP_WRITTEN_DATA or not successful_ids:
        return

    deadline = time.perf_counter() + WRITE_WAIT_TIMEOUT
    pending = set(successful_ids)
    path = bench_dir()

    while pending and time.perf_counter() < deadline:
        for i in list(pending):
            blob = path / f"{i:04d}.pkl"
            try:
                if blob.stat().st_size == payload_size:
                    pending.remove(i)
            except FileNotFoundError:
                pass
        if pending:
            time.sleep(0.1)

    if pending:
        print(f"Warning: {len(pending)} accepted writes were not visible before cleanup timeout")


def cleanup_written_data() -> None:
    if not CLEANUP_WRITTEN_DATA:
        return

    path = bench_dir()
    for _ in range(50):
        if path.exists():
            shutil.rmtree(path, ignore_errors=True)
        time.sleep(0.2)
        if not path.exists():
            break
    if path.exists():
        print(f"Warning: cleanup path still exists after retries: {path}")
    print(f"Cleaned up written data: {path}")


def main() -> None:
    print(f"Building {TARGET_SIZE / 1024 / 1024:.1f} MiB pickle payload...")
    payload = make_pickle_payload(TARGET_SIZE)
    print(f"Payload size: {len(payload):,} bytes")

    print(f"Posting {NUM_FILES} files with parallelism={PARALLELISM} to {URL_BASE}/{BLOB_PREFIX}/...")
    records: list[tuple[int, int, float, float, float]] = []  # (i, status, start, end, latency)
    statuses: dict[int, int] = {}

    try:
        wall_start = time.perf_counter()
        with ThreadPoolExecutor(max_workers=PARALLELISM) as pool:
            futures = [pool.submit(upload, i, payload, wall_start) for i in range(NUM_FILES)]
            for fut in as_completed(futures):
                i, status, t_start, t_end, dt, body = fut.result()
                records.append((i, status, t_start, t_end, dt))
                statuses[status] = statuses.get(status, 0) + 1
                if status == 0 or status >= 400:
                    print(f"  [{i}] HTTP {status} after {dt * 1000:.0f} ms: {body.strip()}")
        wall = time.perf_counter() - wall_start
    finally:
        successful_ids = [r[0] for r in records if r[1] == 202]
        wait_for_writes(successful_ids, len(payload))
        cleanup_written_data()

    latencies = sorted(r[4] for r in records)

    n = len(latencies)
    if n == 0:
        print("No completed requests.")
        return

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
