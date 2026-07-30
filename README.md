# Sluice

An exactly-once ingest gateway for high-volume edge device fleets.

**Live:** https://sluice.levelbrook.com

A fleet of cameras or sensors delivering over cellular links has two failure modes that
matter and that a naive ingest endpoint handles wrong:

1. **It redelivers.** The device cannot know whether its upload succeeded before the link
   dropped, so it retries. The same physical capture arrives many times.
2. **It loses things.** Some captures never arrive at all, and nothing in the stored data
   says so. "We have 4,812 assets from this camera" is not an answer to "did we lose
   footage."

Sluice stores each asset exactly once regardless of redelivery, and tracks a per-device
contiguous delivery watermark so lost assets are *detectable* rather than merely absent.

Go 1.24, PostgreSQL 17, no other runtime dependencies.

---

## The two invariants

Both are enforced in the database rather than in application logic, and both have tests
that fail if the enforcement is removed.

### 1. Exactly-once storage under at-least-once delivery

Dedup is a unique constraint on `(device_uid, asset_uid)` plus `ON CONFLICT DO NOTHING`,
not a read-then-write. `asset_uid` is assigned once by the device and reused on every
retry, so it is the only thing that can identify a duplicate delivery of the same capture.

`RETURNING seq` reports exactly which rows were genuinely new, so `inserted + deduped`
always equals deliveries received. That makes the dedup rate an observable number instead
of an assumption.

A read-then-write dedup passes a single-threaded test and fails
`TestExactlyOnceUnderConcurrentDuplicateDelivery`, which fires 32 concurrent redeliveries
of 200 assets and asserts exactly 200 rows.

### 2. The contiguous watermark never passes a missing sequence

Each device stamps a monotonic `seq`. `device_watermarks.contiguous_through` is the highest
sequence for which every prior sequence has also arrived; anything missing below
`highest_seen` is recorded in `device_gaps` and stays open until the late asset actually
shows up.

The advance happens inside a transaction holding `SELECT ... FOR UPDATE` on the watermark
row. Without that lock two concurrent batches each read a stale `contiguous_through`, each
scan a different subset, and one writes a watermark computed from a stale read that steps
straight over a sequence that never arrived.

---

## The deadlock this found

The first working version inserted assets and *then* took the watermark lock. That is the
obvious order and it is wrong.

```
tx A: inserts uid1 (uncommitted, holds a unique-index lock on it)
tx B: inserts uid2 (uncommitted), takes the watermark lock for the device
tx A: tries to insert uid2  -> blocks on B's uncommitted row
tx B: tries to insert uid1  -> blocks on A's uncommitted row
                            -> Postgres kills one: SQLSTATE 40P01
```

Under burst traffic with redelivery this failed **15 of 16 concurrent batches**, and every
failure silently dropped assets that had already been acknowledged to the device. The only
symptom was a counter.

The fix is to acquire the per-device watermark lock **before any insert**, giving every
transaction that touches a device one identical first lock and therefore a total order.
Different devices still write fully in parallel, which is where the concurrency actually
is. `TestOverlappingConcurrentBatchesDoNotDeadlock` reproduces the original failure.

## The throughput fix

The same first version issued one `INSERT ... RETURNING` per asset. At a batch size of 128
that is 128 network round trips per transaction, and it measured **p50 1,007 ms** per batch.

Rewriting it as a single set-based statement over `unnest($2::uuid[], ...)` took the same
workload to **p50 30.6 ms**, a ~33x improvement, with gap records written the same way
instead of in a loop.

Measured on the same machine and workload (20 devices, 400 assets each, 35% redelivery):

| | one round trip per asset | single set-based statement |
|---|---|---|
| p50 batch write | 1,007 ms | **30.6 ms** |
| p95 batch write | 1,042 ms | **45.8 ms** |
| write errors | 15 of 16 batches | **0** |

## Backpressure

The queue is bounded. When it is full, `Submit` sheds immediately and the endpoint returns
**429 with `Retry-After`**, reporting exactly how many assets it accepted so the device
retries only the remainder. It is never a 500: nothing is broken, the pipeline is full, and
devices already retry. The alternative is unbounded memory growth followed by an OOM kill,
which loses every asset already accepted.

`TestSubmitShedsInsteadOfBlocking` asserts `Submit` returns rather than blocking, and that
`accepted + shed` accounts for every asset handed in.

---

## Running it

```bash
createdb sluice
DATABASE_URL=postgres://localhost/sluice go run .
# http://localhost:8080
```

The schema applies itself on boot and is idempotent, so a container restart is safe.

### Tests

The database tests run against a real PostgreSQL instance, because the properties under
test *are* Postgres behaviour: a unique constraint resolving a race, and `FOR UPDATE`
serializing concurrent watermark advances. A fake would assert nothing of value.

```bash
createdb sluice_test
SLUICE_TEST_DSN=postgres://localhost/sluice_test go test ./... -race
```

```
ok  github.com/tachyurgy/sluice/internal/store    7 tests
ok  github.com/tachyurgy/sluice/internal/ingest   4 tests
```

### Configuration

| Env | Default | Purpose |
|---|---|---|
| `DATABASE_URL` | required | Postgres DSN |
| `PORT` | `8080` | listen port |
| `QUEUE_DEPTH` | `8192` | assets buffered before shedding |
| `WORKERS` | `4` | concurrent DB writers |
| `BATCH_SIZE` | `128` | max assets per transaction |
| `FLUSH_MS` | `50` | max time an asset waits for a full batch |

---

## API

```
POST /v1/ingest              batch delivery; 202 accepted, 429 when full
GET  /v1/devices             per-device watermark and open gaps, worst first
GET  /v1/devices/{uid}       one device's delivery health
GET  /v1/metrics             counters and write-latency percentiles
GET  /v1/summary             fleet totals
GET  /up                     liveness, dependency-free
```

```bash
curl -X POST localhost:8080/v1/ingest -H 'Content-Type: application/json' -d '{
  "assets": [{
    "device_uid": "cam-0042",
    "asset_uid":  "9f1c0f8e-6b1a-4a1e-9f7e-2c3d4e5f6a7b",
    "seq":        1041,
    "kind":       "plate_read",
    "captured_at":"2026-07-29T21:14:03Z",
    "payload":    {"confidence": 0.97}
  }]
}'
```

Sending the same `asset_uid` again returns 202 and stores nothing further.

## Known limits

- **Gap detection is eager.** A sequence missing below `highest_seen` is recorded as an open
  gap immediately, so heavily out-of-order arrival opens and then closes gaps that were only
  ever in flight. The operator view converges correctly and the invariant holds, but a
  reorder-tolerance window (only open a gap once `seq < highest_seen - window`) would cut
  the write churn. Not implemented because the correct window is a property of a real
  fleet's link behaviour, and guessing it here would be fiction.
- **Watermark advance is O(highest - contiguous)** per batch in the worst case. Fine while a
  device stays roughly caught up; a device that has been offline for a long time and then
  floods will do one wide index scan.
- **Single Postgres.** Sharding by `device_uid` is the obvious next step and nothing in the
  schema prevents it, but it is not done here.
