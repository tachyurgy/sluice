package ingest

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/tachyurgy/sluice/internal/store"
)

func mkAssets(n int) []store.Asset {
	out := make([]store.Asset, n)
	for i := range out {
		out[i] = store.Asset{
			DeviceUID:  "cam-test",
			AssetUID:   "00000000-0000-0000-0000-000000000000",
			Seq:        int64(i + 1),
			Kind:       "plate_read",
			CapturedAt: time.Now(),
			Payload:    json.RawMessage(`{}`),
		}
	}
	return out
}

// TestSubmitShedsInsteadOfBlocking is the availability property.
//
// When the database cannot keep up, the queue fills. The pipeline must then
// reject fast and tell the caller exactly how many assets it took, so the device
// knows precisely what to retry. The failure mode being guarded against is
// Submit blocking: that would stall the HTTP handler, pile up goroutines, and
// turn a slow database into an outage.
//
// No workers are started here, so the queue can only fill.
func TestSubmitShedsInsteadOfBlocking(t *testing.T) {
	p := New(nil, Config{QueueDepth: 10, Workers: 1})

	done := make(chan struct{})
	var accepted int
	var err error
	go func() {
		accepted, err = p.Submit(mkAssets(25))
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Submit blocked on a full queue; it must shed and return immediately")
	}

	if err != ErrBackpressure {
		t.Fatalf("err = %v, want ErrBackpressure", err)
	}
	if accepted != 10 {
		t.Errorf("accepted = %d, want 10 (the queue's exact free capacity)", accepted)
	}

	m := p.Metrics()
	if m.Accepted != 10 {
		t.Errorf("metrics.Accepted = %d, want 10", m.Accepted)
	}
	if m.Shed != 15 {
		t.Errorf("metrics.Shed = %d, want 15", m.Shed)
	}
	if m.Accepted+m.Shed != 25 {
		t.Errorf("accepted+shed = %d, want 25: no asset may be silently dropped from the books",
			m.Accepted+m.Shed)
	}
}

// TestSubmitAcceptsWhenSpaceAvailable is the happy path, kept so a regression
// that makes Submit always shed cannot hide behind the test above.
func TestSubmitAcceptsWhenSpaceAvailable(t *testing.T) {
	p := New(nil, Config{QueueDepth: 100, Workers: 1})
	n, err := p.Submit(mkAssets(40))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if n != 40 {
		t.Errorf("accepted = %d, want 40", n)
	}
	if m := p.Metrics(); m.Shed != 0 {
		t.Errorf("shed = %d, want 0", m.Shed)
	}
}

// TestMetricsPercentilesAreOrdered guards the percentile arithmetic, which is
// easy to get subtly wrong with an off-by-one on the index.
func TestMetricsPercentilesAreOrdered(t *testing.T) {
	p := New(nil, Config{})
	for i := 1; i <= 100; i++ {
		p.observe(float64(i))
	}
	m := p.Metrics()
	if !(m.P50WriteMs <= m.P95WriteMs && m.P95WriteMs <= m.P99WriteMs) {
		t.Errorf("percentiles out of order: p50=%v p95=%v p99=%v",
			m.P50WriteMs, m.P95WriteMs, m.P99WriteMs)
	}
	if m.P50WriteMs < 40 || m.P50WriteMs > 60 {
		t.Errorf("p50 = %v, want near 50", m.P50WriteMs)
	}
}

// TestDedupPercentIsZeroWithNoTraffic: a fresh pipeline must not report NaN,
// which would poison a dashboard or an alerting rule.
func TestDedupPercentIsZeroWithNoTraffic(t *testing.T) {
	p := New(nil, Config{})
	if got := p.Metrics().DedupPercent; got != 0 {
		t.Errorf("dedup percent = %v on an idle pipeline, want 0", got)
	}
}
