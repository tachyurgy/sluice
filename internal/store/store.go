// Package store owns every SQL statement Sluice issues.
//
// The two properties the pipeline sells are enforced here:
//
//	1. An asset is stored exactly once no matter how many times a device
//	   redelivers it, because the dedup is a unique constraint plus
//	   ON CONFLICT DO NOTHING rather than a read-then-write.
//	2. A device's contiguous delivery watermark only advances over sequence
//	   numbers that actually arrived, because the advance happens inside a
//	   transaction that holds a row lock on the watermark.
package store

import (
	"context"
	"embed"
	"encoding/json"
	"fmt"
	"sort"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

//go:embed ddl/*.sql
var ddl embed.FS

// Asset is one capture delivered by a device.
type Asset struct {
	DeviceUID  string          `json:"device_uid"`
	AssetUID   string          `json:"asset_uid"`
	Seq        int64           `json:"seq"`
	Kind       string          `json:"kind"`
	CapturedAt time.Time       `json:"captured_at"`
	Payload    json.RawMessage `json:"payload"`
}

// BatchResult reports what a single WriteBatch call actually changed. Inserted
// plus Deduped always equals the number of assets handed in, which is what makes
// the dedup rate observable instead of guessed at.
type BatchResult struct {
	Inserted int `json:"inserted"`
	Deduped  int `json:"deduped"`
	// GapsOpened and GapsClosed are cumulative over the batch.
	GapsOpened int `json:"gaps_opened"`
	GapsClosed int `json:"gaps_closed"`
}

// DeviceHealth is the operator-facing answer to "is this device delivering".
type DeviceHealth struct {
	DeviceUID         string  `json:"device_uid"`
	ContiguousThrough int64   `json:"contiguous_through"`
	HighestSeen       int64   `json:"highest_seen"`
	OpenGaps          []int64 `json:"open_gaps"`
	StoredAssets      int64   `json:"stored_assets"`
}

type Store struct{ pool *pgxpool.Pool }

func Open(ctx context.Context, dsn string) (*Store, error) {
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, fmt.Errorf("parse dsn: %w", err)
	}
	// The ingest path is short transactions at high frequency, so a modest pool
	// with a hard cap keeps Postgres from being the thing that falls over first.
	cfg.MaxConns = 16
	cfg.MinConns = 2
	cfg.MaxConnLifetime = 30 * time.Minute
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("open pool: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("ping: %w", err)
	}
	return &Store{pool: pool}, nil
}

func (s *Store) Close() { s.pool.Close() }

// Migrate applies the schema. It is idempotent so a container restart is safe.
func (s *Store) Migrate(ctx context.Context) error {
	b, err := ddl.ReadFile("ddl/schema.sql")
	if err != nil {
		return err
	}
	_, err = s.pool.Exec(ctx, string(b))
	return err
}

// WriteBatch stores a batch of assets and updates each affected device's
// delivery watermark.
//
// Assets are grouped by device so that the watermark work for a device happens
// exactly once per batch and under a single lock, rather than once per asset.
func (s *Store) WriteBatch(ctx context.Context, assets []Asset) (BatchResult, error) {
	var res BatchResult
	if len(assets) == 0 {
		return res, nil
	}

	byDevice := make(map[string][]Asset)
	for _, a := range assets {
		byDevice[a.DeviceUID] = append(byDevice[a.DeviceUID], a)
	}

	for device, group := range byDevice {
		r, err := s.writeDeviceGroup(ctx, device, group)
		if err != nil {
			return res, err
		}
		res.Inserted += r.Inserted
		res.Deduped += r.Deduped
		res.GapsOpened += r.GapsOpened
		res.GapsClosed += r.GapsClosed
	}
	return res, nil
}

func (s *Store) writeDeviceGroup(ctx context.Context, device string, group []Asset) (BatchResult, error) {
	var res BatchResult

	// Collapse duplicates that arrived inside a single batch before touching the
	// database. Doing it here keeps the insert a clean set operation and makes
	// the inserted/deduped arithmetic exact rather than dependent on how
	// Postgres resolves an intra-statement conflict.
	unique := make(map[string]Asset, len(group))
	for _, a := range group {
		if _, seen := unique[a.AssetUID]; seen {
			res.Deduped++
			continue
		}
		unique[a.AssetUID] = a
	}
	batch := make([]Asset, 0, len(unique))
	for _, a := range unique {
		batch = append(batch, a)
	}
	// Ascending sequence order gives the index inserts locality and makes the
	// statement deterministic, which matters when reading a slow-query log.
	sort.Slice(batch, func(i, j int) bool { return batch[i].Seq < batch[j].Seq })

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return res, err
	}
	defer tx.Rollback(ctx) //nolint:errcheck // no-op once committed

	// LOCK ORDER MATTERS AND THIS IS WHY THE WATERMARK LOCK COMES FIRST.
	//
	// An earlier version inserted assets first and locked the watermark after.
	// Two concurrent batches for the same device that share any asset_uid then
	// each ended up holding an uncommitted insert the other needed, and Postgres
	// resolved it by killing one with SQLSTATE 40P01. Under burst traffic that
	// was 15 of 16 batches failing and silently losing already-acknowledged
	// assets. See TestOverlappingConcurrentBatchesDoNotDeadlock.
	//
	// Taking this row lock before any insert gives every transaction that touches
	// a device one identical first lock, so there is a total order and no cycle
	// can form. Work for DIFFERENT devices still proceeds fully in parallel,
	// which is where the real concurrency is.
	if _, err := tx.Exec(ctx, `
		INSERT INTO device_watermarks (device_uid) VALUES ($1)
		ON CONFLICT (device_uid) DO NOTHING`, device); err != nil {
		return res, fmt.Errorf("ensure watermark %s: %w", device, err)
	}
	var wm struct {
		Contiguous  int64
		HighestSeen int64
	}
	if err := tx.QueryRow(ctx, `
		SELECT contiguous_through, highest_seen
		FROM device_watermarks WHERE device_uid = $1
		FOR UPDATE`, device,
	).Scan(&wm.Contiguous, &wm.HighestSeen); err != nil {
		return res, fmt.Errorf("lock watermark %s: %w", device, err)
	}

	// One statement for the whole batch instead of one round trip per asset.
	// At a batch size of 128 that is the difference between 128 network
	// round trips and 1, and it was worth roughly an order of magnitude on
	// write latency in local measurement.
	//
	// ON CONFLICT DO NOTHING makes a device retry a no-op rather than an error,
	// and RETURNING seq reports exactly which sequence numbers are genuinely new
	// so the watermark logic only reacts to real arrivals.
	uids := make([]string, len(batch))
	seqs := make([]int64, len(batch))
	kinds := make([]string, len(batch))
	caps := make([]time.Time, len(batch))
	loads := make([][]byte, len(batch))
	for i, a := range batch {
		uids[i], seqs[i], kinds[i], caps[i] = a.AssetUID, a.Seq, a.Kind, a.CapturedAt
		loads[i] = a.Payload
	}

	rows, err := tx.Query(ctx, `
		INSERT INTO assets (device_uid, asset_uid, seq, kind, captured_at, payload)
		SELECT $1, u.asset_uid, u.seq, u.kind, u.captured_at, u.payload
		FROM unnest($2::uuid[], $3::bigint[], $4::text[], $5::timestamptz[], $6::jsonb[])
		     AS u(asset_uid, seq, kind, captured_at, payload)
		ON CONFLICT (device_uid, asset_uid) DO NOTHING
		RETURNING seq`,
		device, uids, seqs, kinds, caps, loads)
	if err != nil {
		return res, fmt.Errorf("insert assets %s: %w", device, err)
	}
	inserted := make([]int64, 0, len(batch))
	for rows.Next() {
		var seq int64
		if err := rows.Scan(&seq); err != nil {
			rows.Close()
			return res, err
		}
		inserted = append(inserted, seq)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return res, fmt.Errorf("insert assets %s: %w", device, err)
	}
	res.Inserted = len(inserted)
	res.Deduped += len(batch) - len(inserted)

	if len(inserted) == 0 {
		// Nothing new arrived, so the watermark cannot have moved. This is the
		// hot path when a device is redelivering.
		return res, tx.Commit(ctx)
	}

	highest := wm.HighestSeen
	for _, seq := range inserted {
		if seq > highest {
			highest = seq
		}
	}

	// A newly arrived asset may fill a gap we previously recorded.
	closed, err := tx.Exec(ctx, `
		UPDATE device_gaps SET resolved_at = now()
		WHERE device_uid = $1 AND resolved_at IS NULL AND seq = ANY($2)`,
		device, inserted)
	if err != nil {
		return res, fmt.Errorf("close gaps %s: %w", device, err)
	}
	res.GapsClosed = int(closed.RowsAffected())

	// Recompute the contiguous watermark by walking forward from where we were.
	// Only sequence numbers actually present in `assets` can advance it.
	newContig, missing, err := advanceWatermark(ctx, tx, device, wm.Contiguous, highest)
	if err != nil {
		return res, err
	}

	// Record every still-missing sequence below the highest seen as an open gap,
	// again as a single set-based statement rather than a loop of round trips.
	if len(missing) > 0 {
		ct, err := tx.Exec(ctx, `
			INSERT INTO device_gaps (device_uid, seq)
			SELECT $1, g FROM unnest($2::bigint[]) AS g
			ON CONFLICT (device_uid, seq) DO NOTHING`, device, missing)
		if err != nil {
			return res, fmt.Errorf("open gaps %s: %w", device, err)
		}
		res.GapsOpened = int(ct.RowsAffected())
	}

	if _, err := tx.Exec(ctx, `
		UPDATE device_watermarks
		SET contiguous_through = $2, highest_seen = $3, updated_at = now()
		WHERE device_uid = $1`, device, newContig, highest); err != nil {
		return res, fmt.Errorf("update watermark %s: %w", device, err)
	}

	return res, tx.Commit(ctx)
}

// advanceWatermark walks from contig+1 to highest and returns the new contiguous
// point plus every sequence number still absent below highest.
//
// It reads the sequence numbers in one query rather than probing per sequence,
// so a device that jumps far ahead costs one index scan instead of N round trips.
func advanceWatermark(ctx context.Context, tx pgx.Tx, device string, contig, highest int64) (int64, []int64, error) {
	if highest <= contig {
		return contig, nil, nil
	}
	rows, err := tx.Query(ctx, `
		SELECT seq FROM assets
		WHERE device_uid = $1 AND seq > $2 AND seq <= $3
		ORDER BY seq`, device, contig, highest)
	if err != nil {
		return contig, nil, fmt.Errorf("scan seqs %s: %w", device, err)
	}
	defer rows.Close()

	present := make(map[int64]bool)
	for rows.Next() {
		var s int64
		if err := rows.Scan(&s); err != nil {
			return contig, nil, err
		}
		present[s] = true
	}
	if err := rows.Err(); err != nil {
		return contig, nil, err
	}

	newContig := contig
	for newContig+1 <= highest && present[newContig+1] {
		newContig++
	}
	var missing []int64
	for s := newContig + 1; s <= highest; s++ {
		if !present[s] {
			missing = append(missing, s)
		}
	}
	return newContig, missing, nil
}

// Health returns the operator view for one device.
func (s *Store) Health(ctx context.Context, device string) (DeviceHealth, error) {
	h := DeviceHealth{DeviceUID: device, OpenGaps: []int64{}}
	err := s.pool.QueryRow(ctx, `
		SELECT contiguous_through, highest_seen
		FROM device_watermarks WHERE device_uid = $1`, device,
	).Scan(&h.ContiguousThrough, &h.HighestSeen)
	if err != nil && err != pgx.ErrNoRows {
		return h, err
	}
	if err := s.pool.QueryRow(ctx,
		`SELECT count(*) FROM assets WHERE device_uid = $1`, device).Scan(&h.StoredAssets); err != nil {
		return h, err
	}
	rows, err := s.pool.Query(ctx, `
		SELECT seq FROM device_gaps
		WHERE device_uid = $1 AND resolved_at IS NULL ORDER BY seq`, device)
	if err != nil {
		return h, err
	}
	defer rows.Close()
	for rows.Next() {
		var s int64
		if err := rows.Scan(&s); err != nil {
			return h, err
		}
		h.OpenGaps = append(h.OpenGaps, s)
	}
	return h, rows.Err()
}

// FleetSummary powers the dashboard's top line.
type FleetSummary struct {
	Devices      int64 `json:"devices"`
	StoredAssets int64 `json:"stored_assets"`
	OpenGaps     int64 `json:"open_gaps"`
}

func (s *Store) FleetSummary(ctx context.Context) (FleetSummary, error) {
	var f FleetSummary
	err := s.pool.QueryRow(ctx, `
		SELECT
		  (SELECT count(*) FROM device_watermarks),
		  (SELECT count(*) FROM assets),
		  (SELECT count(*) FROM device_gaps WHERE resolved_at IS NULL)`,
	).Scan(&f.Devices, &f.StoredAssets, &f.OpenGaps)
	return f, err
}

// Devices lists devices with their watermark state, worst-first so a device that
// is dropping assets surfaces at the top.
func (s *Store) Devices(ctx context.Context, limit int) ([]DeviceHealth, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT w.device_uid, w.contiguous_through, w.highest_seen,
		       (SELECT count(*) FROM assets a WHERE a.device_uid = w.device_uid),
		       COALESCE((SELECT count(*) FROM device_gaps g
		                 WHERE g.device_uid = w.device_uid AND g.resolved_at IS NULL), 0) AS open_gaps
		FROM device_watermarks w
		ORDER BY open_gaps DESC, w.device_uid
		LIMIT $1`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []DeviceHealth
	for rows.Next() {
		var d DeviceHealth
		var gaps int64
		if err := rows.Scan(&d.DeviceUID, &d.ContiguousThrough, &d.HighestSeen, &d.StoredAssets, &gaps); err != nil {
			return nil, err
		}
		d.OpenGaps = make([]int64, 0)
		if gaps > 0 {
			g, err := s.Health(ctx, d.DeviceUID)
			if err != nil {
				return nil, err
			}
			d.OpenGaps = g.OpenGaps
		}
		out = append(out, d)
	}
	return out, rows.Err()
}

// Reset clears all data. Used by the demo dashboard and the tests.
func (s *Store) Reset(ctx context.Context) error {
	_, err := s.pool.Exec(ctx, `TRUNCATE assets, device_watermarks, device_gaps`)
	return err
}
