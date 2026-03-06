package monitor

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"sync"
	"time"
)

type Config struct {
	URL         string
	LogFile     string
	Interval    time.Duration
	HTTPTimeout time.Duration
}

type Runner struct {
	cancel   context.CancelFunc
	wg       sync.WaitGroup
	stopOnce sync.Once
	mu       sync.RWMutex
	summary  Summary
}

type Summary struct {
	SampleCount int64
	FailCount   int64
	AvgMem      float64
	MaxMem      int64
	AvgCPU      float64
	MaxCPU      float64
}

func StartVarzMonitor(parent context.Context, cfg Config) *Runner {
	cfg = normalizeConfig(cfg)

	ctx, cancel := context.WithCancel(parent)
	r := &Runner{cancel: cancel}
	r.wg.Add(1)

	go func() {
		defer r.wg.Done()
		run(ctx, cfg, r)
	}()

	return r
}

func (r *Runner) Stop() Summary {
	if r == nil {
		return Summary{}
	}

	r.stopOnce.Do(func() {
		r.cancel()
		r.wg.Wait()
	})

	return r.getSummary()
}

func normalizeConfig(cfg Config) Config {
	if cfg.URL == "" {
		cfg.URL = "http://localhost:8222/varz?js=true"
	}
	if cfg.LogFile == "" {
		cfg.LogFile = "loadResult.log"
	}
	if cfg.Interval <= 0 {
		cfg.Interval = 30 * time.Second
	}
	if cfg.HTTPTimeout <= 0 {
		cfg.HTTPTimeout = 5 * time.Second
	}

	return cfg
}

func run(ctx context.Context, cfg Config, r *Runner) {
	file, err := os.Create(cfg.LogFile)
	if err != nil {
		log.Printf("failed to create monitor log file: %v", err)
		r.setSummary(Summary{})
		return
	}
	defer file.Close()

	if _, err := fmt.Fprintf(file, "=== NATS JSZ Monitoring Started: %s ===\n", time.Now().Format(time.RFC3339)); err != nil {
		log.Printf("failed to write monitor header: %v", err)
		r.setSummary(Summary{})
		return
	}

	client := &http.Client{Timeout: cfg.HTTPTimeout}
	ticker := time.NewTicker(cfg.Interval)
	defer ticker.Stop()

	stats := monitorStats{}

	for {
		mem, cpu, err := fetchSnapshot(client, cfg.URL)
		if err != nil {
			stats.failCount++
			log.Printf("monitor snapshot failed: %v", err)
		} else {
			stats.add(mem, cpu)
		}

		select {
		case <-ctx.Done():
			summary := stats.toSummary()
			if err := writeSummary(file, summary); err != nil {
				log.Printf("failed to write monitor summary: %v", err)
			}
			r.setSummary(summary)
			_, _ = fmt.Fprintf(file, "=== NATS JSZ Monitoring Stopped: %s ===\n", time.Now().Format(time.RFC3339))
			return
		case <-ticker.C:
		}
	}
}

type monitorStats struct {
	sampleCount int64
	failCount   int64
	sumMem      int64
	maxMem      int64
	sumCPU      float64
	maxCPU      float64
}

func (s *monitorStats) add(mem int64, cpu float64) {
	s.sampleCount++
	s.sumMem += mem
	s.sumCPU += cpu

	if s.sampleCount == 1 || mem > s.maxMem {
		s.maxMem = mem
	}
	if s.sampleCount == 1 || cpu > s.maxCPU {
		s.maxCPU = cpu
	}
}

func (s monitorStats) toSummary() Summary {
	summary := Summary{
		SampleCount: s.sampleCount,
		FailCount:   s.failCount,
		MaxMem:      s.maxMem,
		MaxCPU:      s.maxCPU,
	}

	if s.sampleCount > 0 {
		summary.AvgMem = float64(s.sumMem) / float64(s.sampleCount)
		summary.AvgCPU = s.sumCPU / float64(s.sampleCount)
	}

	return summary
}

func fetchSnapshot(client *http.Client, url string) (int64, float64, error) {
	req, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		return 0, 0, err
	}

	resp, err := client.Do(req)
	if err != nil {
		return 0, 0, err
	}

	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return 0, 0, fmt.Errorf("varz status: %d", resp.StatusCode)
	}

	var payload struct {
		Mem int64   `json:"mem"`
		CPU float64 `json:"cpu"`
	}

	if err := decodePayload(resp, &payload); err != nil {
		return 0, 0, err
	}

	return payload.Mem, payload.CPU, nil
}

func decodePayload(resp *http.Response, payload interface{}) error {
	decoder := json.NewDecoder(resp.Body)
	if err := decoder.Decode(payload); err != nil {
		return err
	}

	return nil
}

func writeSummary(file *os.File, summary Summary) error {
	if _, err := fmt.Fprintln(file, "=== NATS JSZ Monitoring Summary ==="); err != nil {
		return err
	}

	if summary.SampleCount == 0 {
		if _, err := fmt.Fprintf(file, "sample_count=0 fail_count=%d avg_mem=0 max_mem=0 avg_cpu=0 max_cpu=0\n", summary.FailCount); err != nil {
			return err
		}
		return nil
	}

	if _, err := fmt.Fprintf(
		file,
		"sample_count=%d fail_count=%d avg_mem=%.2f max_mem=%d avg_cpu=%.6f max_cpu=%.6f\n",
		summary.SampleCount,
		summary.FailCount,
		summary.AvgMem,
		summary.MaxMem,
		summary.AvgCPU,
		summary.MaxCPU,
	); err != nil {
		return err
	}

	return nil
}

func (r *Runner) setSummary(summary Summary) {
	r.mu.Lock()
	r.summary = summary
	r.mu.Unlock()
}

func (r *Runner) getSummary() Summary {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.summary
}
