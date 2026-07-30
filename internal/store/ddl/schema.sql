-- Sluice: exactly-once ingest for high-volume edge device fleets.
--
-- Two invariants are enforced here in the schema rather than in application code,
-- because a fleet of devices retrying over flaky links will eventually deliver
-- every asset more than once, and application-level "check then insert" loses
-- that race under concurrency.

CREATE TABLE IF NOT EXISTS assets (
    id           BIGSERIAL   PRIMARY KEY,
    device_uid   TEXT        NOT NULL,
    -- Device-generated idempotency key. The device assigns it once and reuses it
    -- on every retry, so it is the only thing that can identify a duplicate
    -- delivery of the same physical capture.
    asset_uid    UUID        NOT NULL,
    -- Per-device monotonic counter. Lets us detect assets lost in transit
    -- without the device having to acknowledge anything.
    seq          BIGINT      NOT NULL,
    kind         TEXT        NOT NULL,
    captured_at  TIMESTAMPTZ NOT NULL,
    payload      JSONB       NOT NULL,
    ingested_at  TIMESTAMPTZ NOT NULL DEFAULT now(),

    -- INVARIANT 1: exactly-once storage under at-least-once delivery.
    CONSTRAINT assets_dedup UNIQUE (device_uid, asset_uid)
);

-- Serves the per-device gap scan and the "recent assets for device" read path.
-- Covering (device_uid, seq) means the gap scan is an index-only scan.
CREATE INDEX IF NOT EXISTS assets_device_seq_idx ON assets (device_uid, seq);

-- Serves the fleet-wide recency query on the dashboard.
CREATE INDEX IF NOT EXISTS assets_captured_idx ON assets (captured_at DESC);

-- Per-device delivery watermark.
--
-- contiguous_through is the highest sequence number for which every prior
-- sequence has also arrived. It only ever moves forward, and it is advanced
-- under a row lock so that concurrent batches for the same device cannot
-- interleave and skip a gap.
CREATE TABLE IF NOT EXISTS device_watermarks (
    device_uid         TEXT        PRIMARY KEY,
    contiguous_through BIGINT      NOT NULL DEFAULT 0,
    highest_seen       BIGINT      NOT NULL DEFAULT 0,
    updated_at         TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- INVARIANT 2: a sequence number that never arrived is recorded as an open gap
-- and stays open until the late asset actually shows up. An operator can read
-- this table to answer "did we lose footage from this camera", which is not
-- answerable from the assets table alone.
CREATE TABLE IF NOT EXISTS device_gaps (
    device_uid        TEXT        NOT NULL,
    seq               BIGINT      NOT NULL,
    first_detected_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    resolved_at       TIMESTAMPTZ,
    PRIMARY KEY (device_uid, seq)
);

CREATE INDEX IF NOT EXISTS device_gaps_open_idx
    ON device_gaps (device_uid) WHERE resolved_at IS NULL;
