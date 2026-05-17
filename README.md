# rs-blobstore

Tiny, fast Rust HTTP service that stores arbitrary binary blobs on disk. Content-agnostic: bytes in, identical bytes out. POST returns `202` as soon as the body is in RAM — disk writes happen in background workers, so slow disks don't slow the API.

## Endpoints

- `POST /blobs/<path>` — body = raw bytes. Returns `202 Accepted`. `503` if the write queue is full, `400` for unsafe paths, `413` for oversized bodies.
- `GET /blobs/<path>` — streams the file back. `404` if missing.
- `GET /healthz` — `200 ok`.

The `<path>` can have any extension (`.parquet.gz`, `.pkl`, `.npy`, `.bin`, …) — the server never inspects it.

## Config (env vars)

| var | default |
|---|---|
| `STORAGE_ROOT` | `/data` |
| `BIND_ADDR` | `0.0.0.0:8080` |
| `WRITE_QUEUE_CAPACITY` | `1024` |
| `WRITE_WORKERS` | `4` |
| `MAX_BODY_BYTES` | `1073741824` (1 GiB) |
| `RUST_LOG` | `info` |

## Run

```sh
# local
STORAGE_ROOT=./data cargo run --release

# docker
docker build -t rs-blobstore .
docker run --rm -p 8080:8080 -v "$(pwd)/data:/data" rs-blobstore
```

## Example

```sh
# gzipped parquet
curl -X POST --data-binary @sample.parquet.gz http://localhost:8080/blobs/foo/bar.parquet.gz
curl http://localhost:8080/blobs/foo/bar.parquet.gz -o roundtrip.parquet.gz

# pickle (or anything else)
curl -X POST --data-binary @model.pkl http://localhost:8080/blobs/models/v1.pkl
curl http://localhost:8080/blobs/models/v1.pkl -o roundtrip.pkl
```

## Durability

Fire-and-forget: a process crash can drop writes still in the in-memory queue. By design, in exchange for fast POSTs against a slow disk. Run behind a load balancer and replicate at the storage layer if you need stronger guarantees.
