package store

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
)

// These tests run against a real PostgreSQL instance because the properties
// under test ARE Postgres behaviour: a unique constraint resolving a race, and
// SELECT ... FOR UPDATE serializing concurrent watermark advances. A fake or an
// in-memory store would assert nothing of value here.
//
//	createdb sluice_test
//	SLUICE_TEST_DSN=postgres://localhost:5433/sluice_test go test ./...
func testStore(t *testing.T) *Store {
	t.Helper()
	dsn := os.Getenv("SLUICE_TEST_DSN")
	if dsn == "" {
		t.Skip("SLUICE_TEST_DSN not set; skipping database tests")
	}
	ctx := context.Background()
	st, err := Open(ctx, dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if err := st.Migrate(ctx); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	if err := st.Reset(ctx); err != nil {
		t.Fatalf("reset: %v", err)
	}
	t.Cleanup(st.Close)
	return st
}

func asset(device string, seq int64, uid string) Asset {
	return Asset{
		DeviceUID:  device,
		AssetUID:   uid,
		Seq:        seq,
		Kind:       "plate_read",
		CapturedAt: time.Now().UTC(),
		Payload:    json.RawMessage(`{"confidence":0.97}`),
	}
}

// TestExactlyOnceUnderConcurrentDuplicateDelivery is the property the whole
// pipeline exists to guarantee.
//
// A device on a flaky link redelivers the same batch many times, and several
// ingest workers can be mid-write on the same asset simultaneously. After all of
// it, the number of stored rows must equal the number of DISTINCT assets, and the
// inserted counts reported across all writers must sum to exactly that same
// number. A read-then-write dedup passes this at concurrency 1 and fails here.
func TestExactlyOnceUnderConcurrentDuplicateDelivery(t *testing.T) {
	st := testStore(t)
	ctx := context.Background()

	const (
		distinct  = 200
		redeliver = 32
	)
	device := "cam-" + uuid.NewString()[:8]

	batch := make([]Asset, distinct)
	for i := 0; i < distinct; i++ {
		batch[i] = asset(device, int64(i+1), uuid.NewString())
	}

	var (
		wg           sync.WaitGroup
		mu           sync.Mutex
		totalInsert  int
		totalDedup   int
		firstErr     error
	)
	for r := 0; r < redeliver; r++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			res, err := st.WriteBatch(ctx, batch)
			mu.Lock()
			defer mu.Unlock()
			if err != nil && firstErr == nil {
				firstErr = err
			}
			totalInsert += res.Inserted
			totalDedup += res.Deduped
		}()
	}
	wg.Wait()
	if firstErr != nil {
		t.Fatalf("concurrent write: %v", firstErr)
	}

	var stored int64
	if err := st.pool.QueryRow(ctx,
		`SELECT count(*) FROM assets WHERE device_uid = $1`, device).Scan(&stored); err != nil {
		t.Fatal(err)
	}

	if stored != distinct {
		t.Errorf("stored rows = %d, want %d (duplicate delivery leaked rows)", stored, distinct)
	}
	if totalInsert != distinct {
		t.Errorf("reported inserted = %d, want %d (a writer double-counted an insert)", totalInsert, distinct)
	}
	if want := distinct * (redeliver - 1); totalDedup != want {
		t.Errorf("reported deduped = %d, want %d", totalDedup, want)
	}
	if totalInsert+totalDedup != distinct*redeliver {
		t.Errorf("inserted+deduped = %d, want %d: every delivery must be accounted for",
			totalInsert+totalDedup, distinct*redeliver)
	}
}

// TestWatermarkNeverAdvancesPastAGap is the second invariant.
//
// Sequence 3 is deliberately never delivered. Many concurrent writers deliver
// everything else in randomised batches. No matter the interleaving, the
// contiguous watermark must stop at 2, sequence 3 must be recorded as an open
// gap, and highest_seen must still reflect reality.
//
// Without the row lock in writeDeviceGroup, two batches can each read
// contiguous_through=2, each scan a different subset, and one can write a
// watermark computed from a stale read that jumps the missing sequence.
func TestWatermarkNeverAdvancesPastAGap(t *testing.T) {
	st := testStore(t)
	ctx := context.Background()
	device := "cam-" + uuid.NewString()[:8]

	const highest = 60
	const missing = 3

	var batches [][]Asset
	for s := int64(1); s <= highest; s++ {
		if s == missing {
			continue
		}
		batches = append(batches, []Asset{asset(device, s, uuid.NewString())})
	}

	var wg sync.WaitGroup
	errs := make(chan error, len(batches))
	for _, b := range batches {
		wg.Add(1)
		go func(b []Asset) {
			defer wg.Done()
			if _, err := st.WriteBatch(ctx, b); err != nil {
				errs <- err
			}
		}(b)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatalf("write: %v", err)
	}

	h, err := st.Health(ctx, device)
	if err != nil {
		t.Fatal(err)
	}
	if h.ContiguousThrough != missing-1 {
		t.Errorf("contiguous_through = %d, want %d: watermark advanced past a missing sequence",
			h.ContiguousThrough, missing-1)
	}
	if h.HighestSeen != highest {
		t.Errorf("highest_seen = %d, want %d", h.HighestSeen, highest)
	}
	if len(h.OpenGaps) != 1 || h.OpenGaps[0] != missing {
		t.Errorf("open gaps = %v, want [%d]", h.OpenGaps, missing)
	}
	if h.StoredAssets != highest-1 {
		t.Errorf("stored = %d, want %d", h.StoredAssets, highest-1)
	}
}

// TestLateArrivalClosesGapAndAdvancesWatermark covers the recovery path: a device
// resends the asset that was lost, and the watermark must jump all the way to the
// end rather than only advancing by one.
func TestLateArrivalClosesGapAndAdvancesWatermark(t *testing.T) {
	st := testStore(t)
	ctx := context.Background()
	device := "cam-" + uuid.NewString()[:8]

	var first []Asset
	for _, s := range []int64{1, 2, 4, 5, 6} {
		first = append(first, asset(device, s, uuid.NewString()))
	}
	res, err := st.WriteBatch(ctx, first)
	if err != nil {
		t.Fatal(err)
	}
	if res.GapsOpened != 1 {
		t.Fatalf("gaps opened = %d, want 1", res.GapsOpened)
	}
	h, _ := st.Health(ctx, device)
	if h.ContiguousThrough != 2 {
		t.Fatalf("contiguous_through = %d, want 2", h.ContiguousThrough)
	}

	// The missing asset finally arrives.
	res, err = st.WriteBatch(ctx, []Asset{asset(device, 3, uuid.NewString())})
	if err != nil {
		t.Fatal(err)
	}
	if res.GapsClosed != 1 {
		t.Errorf("gaps closed = %d, want 1", res.GapsClosed)
	}

	h, err = st.Health(ctx, device)
	if err != nil {
		t.Fatal(err)
	}
	if h.ContiguousThrough != 6 {
		t.Errorf("contiguous_through = %d, want 6: watermark must skip forward over the "+
			"whole already-present run once the gap is filled", h.ContiguousThrough)
	}
	if len(h.OpenGaps) != 0 {
		t.Errorf("open gaps = %v, want none", h.OpenGaps)
	}
}

// TestMultiDeviceBatchIsolatesWatermarks guards against the grouping in
// WriteBatch leaking one device's sequence numbers into another's watermark.
func TestMultiDeviceBatchIsolatesWatermarks(t *testing.T) {
	st := testStore(t)
	ctx := context.Background()
	a := "cam-a-" + uuid.NewString()[:6]
	b := "cam-b-" + uuid.NewString()[:6]

	batch := []Asset{
		asset(a, 1, uuid.NewString()),
		asset(b, 1, uuid.NewString()),
		asset(a, 2, uuid.NewString()),
		asset(b, 7, uuid.NewString()), // b skips 2..6
	}
	if _, err := st.WriteBatch(ctx, batch); err != nil {
		t.Fatal(err)
	}

	ha, _ := st.Health(ctx, a)
	hb, _ := st.Health(ctx, b)
	if ha.ContiguousThrough != 2 || len(ha.OpenGaps) != 0 {
		t.Errorf("device a: contiguous=%d gaps=%v, want 2 and none", ha.ContiguousThrough, ha.OpenGaps)
	}
	if hb.ContiguousThrough != 1 {
		t.Errorf("device b: contiguous=%d, want 1", hb.ContiguousThrough)
	}
	if len(hb.OpenGaps) != 5 {
		t.Errorf("device b: open gaps = %v, want 5 (2..6)", hb.OpenGaps)
	}
}

// TestRedeliveryDoesNotReopenAClosedGap: once a gap is resolved, a device
// replaying the whole run must not resurrect it.
func TestRedeliveryDoesNotReopenAClosedGap(t *testing.T) {
	st := testStore(t)
	ctx := context.Background()
	device := "cam-" + uuid.NewString()[:8]

	uids := map[int64]string{}
	var full []Asset
	for s := int64(1); s <= 5; s++ {
		uids[s] = uuid.NewString()
		full = append(full, asset(device, s, uids[s]))
	}

	// Deliver everything except 3, then 3, then replay the entire run twice.
	var partial []Asset
	for _, a := range full {
		if a.Seq != 3 {
			partial = append(partial, a)
		}
	}
	if _, err := st.WriteBatch(ctx, partial); err != nil {
		t.Fatal(err)
	}
	if _, err := st.WriteBatch(ctx, []Asset{asset(device, 3, uids[3])}); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ {
		if _, err := st.WriteBatch(ctx, full); err != nil {
			t.Fatal(err)
		}
	}

	h, err := st.Health(ctx, device)
	if err != nil {
		t.Fatal(err)
	}
	if len(h.OpenGaps) != 0 {
		t.Errorf("open gaps = %v after full redelivery, want none", h.OpenGaps)
	}
	if h.ContiguousThrough != 5 {
		t.Errorf("contiguous_through = %d, want 5", h.ContiguousThrough)
	}
	if h.StoredAssets != 5 {
		t.Errorf("stored = %d, want 5", h.StoredAssets)
	}
}

// TestIndexOnlyScanForGapDetection pins the access path the gap scan depends on.
// If someone drops assets_device_seq_idx, throughput degrades to a sequential
// scan per batch and this test says so out loud instead of it showing up as a
// vague latency regression in production.
func TestIndexOnlyScanForGapDetection(t *testing.T) {
	st := testStore(t)
	ctx := context.Background()
	device := "cam-0000"

	// The table has to be representative or this test proves nothing. With 500
	// rows belonging to the only device present, a sequential scan genuinely IS
	// the cheaper plan and Postgres is right to choose it. A real fleet has many
	// devices, so any single device's assets are a small fraction of the table,
	// which is the regime where the composite index actually earns its keep.
	//
	// Seeded in bulk rather than through WriteBatch: this test is about the read
	// path's access plan, and 100k rows through the write path would be slow
	// without testing anything the other cases do not already cover.
	if _, err := st.pool.Exec(ctx, `
		INSERT INTO assets (device_uid, asset_uid, seq, kind, captured_at, payload)
		SELECT 'cam-' || lpad((d.n)::text, 4, '0'),
		       gen_random_uuid(),
		       s.n,
		       'plate_read',
		       now() - (s.n || ' seconds')::interval,
		       '{"confidence":0.97}'::jsonb
		FROM generate_series(0, 199) AS d(n),
		     generate_series(1, 500) AS s(n)`); err != nil {
		t.Fatal(err)
	}
	if _, err := st.pool.Exec(ctx, `ANALYZE assets`); err != nil {
		t.Fatal(err)
	}

	var total int64
	if err := st.pool.QueryRow(ctx, `SELECT count(*) FROM assets`).Scan(&total); err != nil {
		t.Fatal(err)
	}
	t.Logf("table seeded with %d rows across 200 devices", total)

	rows, err := st.pool.Query(ctx, `
		EXPLAIN (FORMAT TEXT)
		SELECT seq FROM assets WHERE device_uid = $1 AND seq > $2 AND seq <= $3 ORDER BY seq`,
		device, 0, 500)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	plan := ""
	for rows.Next() {
		var line string
		if err := rows.Scan(&line); err != nil {
			t.Fatal(err)
		}
		plan += line + "\n"
	}
	if !contains(plan, "assets_device_seq_idx") {
		t.Errorf("gap-detection query no longer uses assets_device_seq_idx.\nplan:\n%s", plan)
	}
	if contains(plan, "Seq Scan") {
		t.Errorf("gap-detection query fell back to a sequential scan.\nplan:\n%s", plan)
	}
}

func contains(hay, needle string) bool {
	return len(hay) >= len(needle) && (func() bool {
		for i := 0; i+len(needle) <= len(hay); i++ {
			if hay[i:i+len(needle)] == needle {
				return true
			}
		}
		return false
	})()
}

var _ = fmt.Sprintf

// TestOverlappingConcurrentBatchesDoNotDeadlock reproduces a lock-order
// deadlock that the earlier implementation hit under real burst traffic.
//
// The original writeDeviceGroup inserted assets FIRST and took the watermark
// row lock AFTERWARDS. Two concurrent batches for the same device that share
// some asset_uids then interleave like this:
//
//	tx A: inserts uid1 (uncommitted, holds a unique-index lock on it)
//	tx B: inserts uid2 (uncommitted), takes the watermark lock for the device
//	tx A: tries to insert uid2  -> blocks on B's uncommitted row
//	tx B: tries to insert uid1  -> blocks on A's uncommitted row
//	                            -> Postgres kills one with SQLSTATE 40P01
//
// Assets that had already been acknowledged to the device were then lost with
// nothing but a counter increment to show for it. The fix is to acquire the
// per-device watermark lock BEFORE any insert, which gives every transaction
// touching a device one identical first lock and therefore a total order.
func TestOverlappingConcurrentBatchesDoNotDeadlock(t *testing.T) {
	st := testStore(t)
	ctx := context.Background()
	device := "cam-" + uuid.NewString()[:8]

	// A pool of assets that many batches will draw from, so overlap is certain.
	const pool = 120
	assets := make([]Asset, pool)
	for i := 0; i < pool; i++ {
		assets[i] = asset(device, int64(i+1), uuid.NewString())
	}

	// Each writer submits a different rotation of the same pool. Rotations
	// guarantee that two writers acquire the same row locks in different orders,
	// which is exactly the condition a deadlock needs.
	const writers = 16
	var wg sync.WaitGroup
	errs := make(chan error, writers)
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func(off int) {
			defer wg.Done()
			rot := make([]Asset, 0, pool)
			for i := 0; i < pool; i++ {
				rot = append(rot, assets[(i+off*7)%pool])
			}
			if _, err := st.WriteBatch(ctx, rot); err != nil {
				errs <- err
			}
		}(w)
	}
	wg.Wait()
	close(errs)

	var failures []error
	for err := range errs {
		failures = append(failures, err)
	}
	if len(failures) > 0 {
		t.Fatalf("%d of %d concurrent batches failed; first: %v",
			len(failures), writers, failures[0])
	}

	var stored int64
	if err := st.pool.QueryRow(ctx,
		`SELECT count(*) FROM assets WHERE device_uid = $1`, device).Scan(&stored); err != nil {
		t.Fatal(err)
	}
	if stored != pool {
		t.Errorf("stored = %d, want %d", stored, pool)
	}
	h, err := st.Health(ctx, device)
	if err != nil {
		t.Fatal(err)
	}
	if h.ContiguousThrough != pool {
		t.Errorf("contiguous_through = %d, want %d", h.ContiguousThrough, pool)
	}
	if len(h.OpenGaps) != 0 {
		t.Errorf("open gaps = %v, want none: every sequence was delivered", h.OpenGaps)
	}
}
