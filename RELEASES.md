# Sluice release history

Newest first.

## 2026-07-30 — initial deploy of the exactly-once ingest gateway
- **What deployed:** https://sluice.levelbrook.com on Hetzner Box B (`5.78.227.227`) behind
  kamal-proxy, TLS via Let's Encrypt. Go 1.24 static binary in a bare `alpine:3.20` container,
  ~6MB resident (cap 192m). Postgres is the shared `lb-postgres` (PG17), database
  `sluice_production`.
- **Changed:** first release. HTTP batch ingest with exactly-once storage keyed on
  `(device_uid, asset_uid)`; per-device contiguous delivery watermark advanced under
  `SELECT ... FOR UPDATE`; open-gap records for sequences lost in transit that close on late
  arrival; bounded queue that sheds with 429 + Retry-After; metrics with write-latency
  percentiles; operator dashboard that re-checks both invariants against the live database.
- **Fixed before release:** a lock-order deadlock (SQLSTATE 40P01) that failed 15 of 16
  concurrent batches and silently dropped already-acknowledged assets, caused by inserting
  assets before taking the watermark lock. Also replaced one INSERT per asset with a single
  set-based `unnest()` statement, taking batch write p50 from 1007ms to 30.6ms.
- **How:** `CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -trimpath -ldflags="-s -w"`, scp the
  binary to `/root/sluice/sluice`, then
  `docker run -d --name sluice-web --network kamal --memory 192m -e DATABASE_URL=... -v /root/sluice/sluice:/app/sluice:ro --entrypoint /app/sluice alpine:3.20`,
  then `docker exec kamal-proxy kamal-proxy deploy sluice --target sluice-web:8080 --host sluice.levelbrook.com --tls`.
  DNS A record created pointing at Box B (note: `lb-dns-add.sh` hardcodes Box A's IP).
- **Verified:** `/up` returns 200 over HTTPS. 11 tests pass locally under `-race` against
  PostgreSQL 17. Against production: 5 redeliveries of one `asset_uid` produced exactly 1 stored
  row; withholding seq 3 left the watermark at 2 with `open_gaps:[3]`, and the late arrival closed
  it and advanced the watermark to 6; an 8-device/250-asset burst at 30% redelivery gave 1962
  inserted = 1962 distinct, inserted+deduped = 2556 = deliveries, 38 open gaps = 38 deliberate
  drops, 0 write errors.
