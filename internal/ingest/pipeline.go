// Package ingest is the bounded, batching front half of the pipeline.
//
// The shape is deliberate. A fleet that grows faster than the database can
// absorb writes will, without a bound somewhere, turn into unbounded memory
// growth and then an OOM kill, which loses every asset already accepted. So the
// queue is bounded and a full queue is a fast, explicit 429 back to the device.
// Devices already retry, so shedding is cheaper than dying.
package ingest

import (
	"context"
	"errors"
	"log/slog"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/tachyurgy/sluice/internal/store"
)

// ErrBackpressure is returned when the queue is full. Callers should map it to
// HTTP 429 with a Retry-After, never 500: nothing is broken, we are just full.
var ErrBackpressure = errors.New("ingest queue full")

type Config struct {
	QueueDepth   int           // max assets buffered before shedding
	Workers      int           // concurrent DB writers
	BatchSize    int           // max assets per transaction
	FlushTimeout time.Duration // max time an asset waits for a full batch
}

func (c Config) withDefaults() Config {
	if c.QueueDepth <= 0 {
		c.QueueDepth = 8192
	}
	if c.Workers <= 0 {
		c.Workers = 4
	}
	if c.BatchSize <= 0 {
		c.BatchSize = 128
	}
	if c.FlushTimeout <= 0 {
		c.FlushTimeout = 50 * time.Millisecond
	}
	return c
}

// Metrics is a snapshot of pipeline counters. Everything is a plain counter so
// the numbers stay meaningful when scraped at any interval.
type Metrics struct {
	Accepted     uint64  `json:"accepted"`
	Shed         uint64  `json:"shed"`
	Inserted     uint64  `json:"inserted"`
	Deduped      uint64  `json:"deduped"`
	GapsOpened   uint64  `json:"gaps_opened"`
	GapsClosed   uint64  `json:"gaps_closed"`
	Batches      uint64  `json:"batches"`
	WriteErrors  uint64  `json:"write_errors"`
	QueueDepth   int     `json:"queue_depth"`
	QueueCap     int     `json:"queue_cap"`
	P50WriteMs   float64 `json:"p50_write_ms"`
	P95WriteMs   float64 `json:"p95_write_ms"`
	P99WriteMs   float64 `json:"p99_write_ms"`
	DedupPercent float64 `json:"dedup_percent"`
}

type Pipeline struct {
	cfg   Config
	st    *store.Store
	log   *slog.Logger
	queue chan store.Asset

	accepted, shed, inserted, deduped   atomic.Uint64
	gapsOpened, gapsClosed              atomic.Uint64
	batches, writeErrors                atomic.Uint64

	mu      sync.Mutex
	samples []float64 // ring of recent write durations in ms

	wg     sync.WaitGroup
	cancel context.CancelFunc
}

const sampleCap = 1024

func New(st *store.Store, cfg Config) *Pipeline {
	return NewWithLogger(st, cfg, nil)
}

func NewWithLogger(st *store.Store, cfg Config, log *slog.Logger) *Pipeline {
	cfg = cfg.withDefaults()
	return &Pipeline{
		cfg:     cfg,
		st:      st,
		log:     log,
		queue:   make(chan store.Asset, cfg.QueueDepth),
		samples: make([]float64, 0, sampleCap),
	}
}

// Start launches the writer pool. Cancelling the context or calling Stop drains
// what is already queued before returning, so a rolling deploy does not drop
// assets that were already acknowledged to a device.
func (p *Pipeline) Start(ctx context.Context) {
	ctx, cancel := context.WithCancel(ctx)
	p.cancel = cancel
	for i := 0; i < p.cfg.Workers; i++ {
		p.wg.Add(1)
		go p.worker(ctx)
	}
}

func (p *Pipeline) Stop() {
	if p.cancel != nil {
		p.cancel()
	}
	p.wg.Wait()
}

// Submit enqueues assets without blocking. It returns ErrBackpressure if the
// queue is full, having enqueued as many as would fit; the count of accepted
// assets is returned so the caller can tell the device exactly what to retry.
func (p *Pipeline) Submit(assets []store.Asset) (int, error) {
	n := 0
	for _, a := range assets {
		select {
		case p.queue <- a:
			n++
			p.accepted.Add(1)
		default:
			p.shed.Add(uint64(len(assets) - n))
			return n, ErrBackpressure
		}
	}
	return n, nil
}

func (p *Pipeline) worker(ctx context.Context) {
	defer p.wg.Done()
	batch := make([]store.Asset, 0, p.cfg.BatchSize)
	timer := time.NewTimer(p.cfg.FlushTimeout)
	defer timer.Stop()

	flush := func() {
		if len(batch) == 0 {
			return
		}
		p.write(batch)
		batch = batch[:0]
	}

	for {
		select {
		case <-ctx.Done():
			// Drain whatever is still buffered before exiting.
			for {
				select {
				case a := <-p.queue:
					batch = append(batch, a)
					if len(batch) >= p.cfg.BatchSize {
						flush()
					}
				default:
					flush()
					return
				}
			}
		case a := <-p.queue:
			batch = append(batch, a)
			if len(batch) >= p.cfg.BatchSize {
				flush()
				if !timer.Stop() {
					select {
					case <-timer.C:
					default:
					}
				}
				timer.Reset(p.cfg.FlushTimeout)
			}
		case <-timer.C:
			flush()
			timer.Reset(p.cfg.FlushTimeout)
		}
	}
}

func (p *Pipeline) write(batch []store.Asset) {
	start := time.Now()
	// Deliberately not the request context: a write that has been accepted must
	// finish even if the submitting device hung up.
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	res, err := p.st.WriteBatch(ctx, batch)
	p.batches.Add(1)
	p.observe(float64(time.Since(start).Microseconds()) / 1000.0)
	if err != nil {
		p.writeErrors.Add(1)
		// A silent counter increment is not enough to debug a write failure at
		// 3am, so the error itself is logged with the batch shape.
		if p.log != nil {
			p.log.Error("batch write failed", "err", err, "batch_size", len(batch),
				"first_device", batch[0].DeviceUID)
		}
		return
	}
	p.inserted.Add(uint64(res.Inserted))
	p.deduped.Add(uint64(res.Deduped))
	p.gapsOpened.Add(uint64(res.GapsOpened))
	p.gapsClosed.Add(uint64(res.GapsClosed))
}

func (p *Pipeline) observe(ms float64) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if len(p.samples) < sampleCap {
		p.samples = append(p.samples, ms)
		return
	}
	copy(p.samples, p.samples[1:])
	p.samples[len(p.samples)-1] = ms
}

// WriteInline bypasses the queue and writes synchronously. The tests use it to
// assert storage invariants without racing the flush timer.
func (p *Pipeline) WriteInline(ctx context.Context, batch []store.Asset) (store.BatchResult, error) {
	res, err := p.st.WriteBatch(ctx, batch)
	if err != nil {
		p.writeErrors.Add(1)
		return res, err
	}
	p.batches.Add(1)
	p.inserted.Add(uint64(res.Inserted))
	p.deduped.Add(uint64(res.Deduped))
	p.gapsOpened.Add(uint64(res.GapsOpened))
	p.gapsClosed.Add(uint64(res.GapsClosed))
	return res, nil
}

// Drain blocks until the queue is empty and in-flight batches have settled. Used
// by the tests and the dashboard's burst button so a reader sees final numbers.
func (p *Pipeline) Drain(ctx context.Context) error {
	for {
		if len(p.queue) == 0 {
			// Give workers a beat past the flush timeout to commit the tail.
			time.Sleep(p.cfg.FlushTimeout + 150*time.Millisecond)
			if len(p.queue) == 0 {
				return nil
			}
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(10 * time.Millisecond):
		}
	}
}

func (p *Pipeline) Metrics() Metrics {
	p.mu.Lock()
	s := make([]float64, len(p.samples))
	copy(s, p.samples)
	p.mu.Unlock()
	sort.Float64s(s)

	pct := func(q float64) float64 {
		if len(s) == 0 {
			return 0
		}
		i := int(q * float64(len(s)-1))
		return s[i]
	}
	ins, ded := p.inserted.Load(), p.deduped.Load()
	var dedupPct float64
	if ins+ded > 0 {
		dedupPct = float64(ded) / float64(ins+ded) * 100
	}
	return Metrics{
		Accepted:     p.accepted.Load(),
		Shed:         p.shed.Load(),
		Inserted:     ins,
		Deduped:      ded,
		GapsOpened:   p.gapsOpened.Load(),
		GapsClosed:   p.gapsClosed.Load(),
		Batches:      p.batches.Load(),
		WriteErrors:  p.writeErrors.Load(),
		QueueDepth:   len(p.queue),
		QueueCap:     cap(p.queue),
		P50WriteMs:   pct(0.50),
		P95WriteMs:   pct(0.95),
		P99WriteMs:   pct(0.99),
		DedupPercent: dedupPct,
	}
}

// ResetMetrics zeroes counters. The dashboard uses it alongside a data reset.
func (p *Pipeline) ResetMetrics() {
	p.accepted.Store(0)
	p.shed.Store(0)
	p.inserted.Store(0)
	p.deduped.Store(0)
	p.gapsOpened.Store(0)
	p.gapsClosed.Store(0)
	p.batches.Store(0)
	p.writeErrors.Store(0)
	p.mu.Lock()
	p.samples = p.samples[:0]
	p.mu.Unlock()
}
