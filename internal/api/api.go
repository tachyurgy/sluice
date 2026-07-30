// Package api is the HTTP surface: the ingest endpoint devices call, the
// operator read endpoints, and a dashboard that makes the two invariants
// visible without needing a database client.
package api

import (
	"embed"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"math/rand"
	"net/http"
	"strconv"
	"time"

	"github.com/google/uuid"
	"github.com/tachyurgy/sluice/internal/ingest"
	"github.com/tachyurgy/sluice/internal/store"
)

//go:embed static/*
var static embed.FS

type Server struct {
	st   *store.Store
	pipe *ingest.Pipeline
	log  *slog.Logger
	mux  *http.ServeMux
}

func New(st *store.Store, pipe *ingest.Pipeline, log *slog.Logger) *Server {
	s := &Server{st: st, pipe: pipe, log: log, mux: http.NewServeMux()}
	s.routes()
	return s
}

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) { s.mux.ServeHTTP(w, r) }

func (s *Server) routes() {
	// Health check kept trivial and dependency-free so the reverse proxy's probe
	// does not start failing merely because the database is briefly busy.
	s.mux.HandleFunc("GET /up", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})

	s.mux.HandleFunc("POST /v1/ingest", s.handleIngest)
	s.mux.HandleFunc("GET /v1/devices", s.handleDevices)
	s.mux.HandleFunc("GET /v1/devices/{uid}", s.handleDevice)
	s.mux.HandleFunc("GET /v1/metrics", s.handleMetrics)
	s.mux.HandleFunc("GET /v1/summary", s.handleSummary)

	// Demo-only controls, so a reader can reproduce both invariants from the
	// browser. Guarded by SLUICE_DEMO in main.
	s.mux.HandleFunc("POST /v1/demo/burst", s.handleBurst)
	s.mux.HandleFunc("POST /v1/demo/reset", s.handleReset)

	sub, err := fs.Sub(static, "static")
	if err != nil {
		panic(err) // embedded at build time; a failure here is a build bug
	}
	s.mux.Handle("GET /", http.FileServer(http.FS(sub)))
}

type ingestRequest struct {
	Assets []store.Asset `json:"assets"`
}

type ingestResponse struct {
	Accepted int    `json:"accepted"`
	Rejected int    `json:"rejected"`
	Message  string `json:"message,omitempty"`
}

func (s *Server) handleIngest(w http.ResponseWriter, r *http.Request) {
	var req ingestRequest
	// Cap the body so one device cannot exhaust memory with a single request.
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 8<<20)).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, ingestResponse{Message: "invalid json: " + err.Error()})
		return
	}
	if len(req.Assets) == 0 {
		writeJSON(w, http.StatusBadRequest, ingestResponse{Message: "no assets"})
		return
	}
	for i, a := range req.Assets {
		if a.DeviceUID == "" || a.AssetUID == "" {
			writeJSON(w, http.StatusBadRequest, ingestResponse{
				Message: fmt.Sprintf("asset %d: device_uid and asset_uid are required", i)})
			return
		}
		if len(a.Payload) == 0 {
			req.Assets[i].Payload = json.RawMessage(`{}`)
		}
		if a.CapturedAt.IsZero() {
			req.Assets[i].CapturedAt = time.Now().UTC()
		}
	}

	accepted, err := s.pipe.Submit(req.Assets)
	if errors.Is(err, ingest.ErrBackpressure) {
		// 429, not 500. Nothing is broken; we are full, and the device should
		// retry the remainder. Telling it exactly how many landed keeps the
		// retry precise instead of duplicating the whole batch.
		w.Header().Set("Retry-After", "1")
		writeJSON(w, http.StatusTooManyRequests, ingestResponse{
			Accepted: accepted,
			Rejected: len(req.Assets) - accepted,
			Message:  "queue full, retry the rejected remainder",
		})
		return
	}
	writeJSON(w, http.StatusAccepted, ingestResponse{Accepted: accepted})
}

func (s *Server) handleDevices(w http.ResponseWriter, r *http.Request) {
	limit := 50
	if v := r.URL.Query().Get("limit"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 && n <= 500 {
			limit = n
		}
	}
	d, err := s.st.Devices(r.Context(), limit)
	if err != nil {
		s.fail(w, err)
		return
	}
	if d == nil {
		d = []store.DeviceHealth{}
	}
	writeJSON(w, http.StatusOK, d)
}

func (s *Server) handleDevice(w http.ResponseWriter, r *http.Request) {
	h, err := s.st.Health(r.Context(), r.PathValue("uid"))
	if err != nil {
		s.fail(w, err)
		return
	}
	writeJSON(w, http.StatusOK, h)
}

func (s *Server) handleMetrics(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, s.pipe.Metrics())
}

func (s *Server) handleSummary(w http.ResponseWriter, r *http.Request) {
	f, err := s.st.FleetSummary(r.Context())
	if err != nil {
		s.fail(w, err)
		return
	}
	writeJSON(w, http.StatusOK, f)
}

type burstRequest struct {
	Devices       int `json:"devices"`
	AssetsPerDev  int `json:"assets_per_device"`
	RedeliverPct  int `json:"redeliver_pct"` // share of assets sent twice
	DropPct       int `json:"drop_pct"`      // share of sequences never sent, to open gaps
}

type burstResponse struct {
	Deliveries       int             `json:"deliveries"`
	DistinctAssets   int             `json:"distinct_assets"`
	IntentionalDrops int             `json:"intentional_drops"`
	Metrics          ingest.Metrics  `json:"metrics"`
	Summary          store.FleetSummary `json:"summary"`
}

// handleBurst synthesises fleet traffic that exercises both invariants at once:
// a share of assets is delivered twice (so dedup has to fire) and a share of
// sequence numbers is never delivered at all (so gap detection has to fire).
func (s *Server) handleBurst(w http.ResponseWriter, r *http.Request) {
	req := burstRequest{Devices: 5, AssetsPerDev: 200, RedeliverPct: 25, DropPct: 2}
	if r.Body != nil {
		_ = json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20)).Decode(&req)
	}
	if req.Devices <= 0 || req.Devices > 50 {
		req.Devices = 5
	}
	if req.AssetsPerDev <= 0 || req.AssetsPerDev > 2000 {
		req.AssetsPerDev = 200
	}
	req.RedeliverPct = clamp(req.RedeliverPct, 0, 100)
	req.DropPct = clamp(req.DropPct, 0, 50)

	// Randomised per burst so repeated clicks build a fleet with history rather
	// than replaying an identical scenario.
	run := uuid.NewString()[:6]
	var deliveries []store.Asset
	distinct, drops := 0, 0

	for d := 0; d < req.Devices; d++ {
		device := fmt.Sprintf("cam-%s-%02d", run, d)
		for seq := 1; seq <= req.AssetsPerDev; seq++ {
			if rand.Intn(100) < req.DropPct {
				drops++
				continue // this sequence number is lost in transit
			}
			a := store.Asset{
				DeviceUID:  device,
				AssetUID:   uuid.NewString(),
				Seq:        int64(seq),
				Kind:       "plate_read",
				CapturedAt: time.Now().UTC().Add(-time.Duration(seq) * time.Second),
				Payload:    json.RawMessage(`{"confidence":0.97,"synthetic":true}`),
			}
			deliveries = append(deliveries, a)
			distinct++
			if rand.Intn(100) < req.RedeliverPct {
				deliveries = append(deliveries, a) // same asset_uid: a retry
			}
		}
	}

	// Shuffle so redeliveries are not adjacent to their originals; this is what
	// concurrent workers would actually see.
	rand.Shuffle(len(deliveries), func(i, j int) {
		deliveries[i], deliveries[j] = deliveries[j], deliveries[i]
	})

	accepted := 0
	for i := 0; i < len(deliveries); i += 256 {
		end := min(i+256, len(deliveries))
		n, err := s.pipe.Submit(deliveries[i:end])
		accepted += n
		if err != nil {
			// Queue full: drain and continue rather than shedding the demo.
			if derr := s.pipe.Drain(r.Context()); derr != nil {
				break
			}
			n2, _ := s.pipe.Submit(deliveries[i+n : end])
			accepted += n2
		}
	}
	if err := s.pipe.Drain(r.Context()); err != nil {
		s.fail(w, err)
		return
	}

	sum, err := s.st.FleetSummary(r.Context())
	if err != nil {
		s.fail(w, err)
		return
	}
	writeJSON(w, http.StatusOK, burstResponse{
		Deliveries:       accepted,
		DistinctAssets:   distinct,
		IntentionalDrops: drops,
		Metrics:          s.pipe.Metrics(),
		Summary:          sum,
	})
}

func (s *Server) handleReset(w http.ResponseWriter, r *http.Request) {
	if err := s.st.Reset(r.Context()); err != nil {
		s.fail(w, err)
		return
	}
	s.pipe.ResetMetrics()
	writeJSON(w, http.StatusOK, map[string]string{"status": "reset"})
}

func (s *Server) fail(w http.ResponseWriter, err error) {
	s.log.Error("request failed", "err", err)
	writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

func clamp(v, lo, hi int) int {
	if v < lo {
		return lo
	}
	if v > hi {
		return hi
	}
	return v
}
