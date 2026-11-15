package telemetry

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

// Prometheus queries /api/v1/query and enforces freshness and completeness invariants.
type Prometheus struct {
	BaseURL string
	MaxAge  float64
	Client  *http.Client
	Now     func() float64
}

func NewPrometheus(baseURL string, maxAgeSeconds float64) *Prometheus {
	return &Prometheus{
		BaseURL: strings.TrimRight(baseURL, "/"),
		MaxAge:  maxAgeSeconds,
		Client:  &http.Client{Timeout: 10 * time.Second},
		Now:     func() float64 { return float64(time.Now().UnixNano()) / 1e9 },
	}
}

type promResponse struct {
	Status string `json:"status"`
	Data   struct {
		ResultType string `json:"resultType"`
		Result     []struct {
			Metric map[string]string `json:"metric"`
			Value  [2]any            `json:"value"`
		} `json:"result"`
	} `json:"data"`
}

// Sample holds a labeled instant vector reading.
type Sample struct {
	Metric map[string]string
	Value  float64
}

func parseSampleValue(raw any) (float64, error) {
	str, ok := raw.(string)
	if !ok {
		return 0, fmt.Errorf("invalid metric value type")
	}
	v, err := strconv.ParseFloat(str, 64)
	if err != nil || math.IsNaN(v) || math.IsInf(v, 0) {
		return 0, fmt.Errorf("non-finite metric value %q", str)
	}
	return v, nil
}

// Vector executes an instant query and checks staleness against MaxAge.
func (p *Prometheus) Vector(ctx context.Context, expr string) ([]Sample, error) {
	endpoint := p.BaseURL + "/api/v1/query?query=" + url.QueryEscape(expr)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return nil, err
	}
	resp, err := p.Client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("prometheus HTTP %d", resp.StatusCode)
	}
	var payload promResponse
	if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
		return nil, err
	}
	if payload.Status != "success" || payload.Data.ResultType != "vector" {
		return nil, fmt.Errorf("invalid prometheus response")
	}
	now := p.Now()
	samples := make([]Sample, 0, len(payload.Data.Result))
	for _, item := range payload.Data.Result {
		ts, ok := item.Value[0].(float64)
		if !ok || math.IsNaN(ts) || math.IsInf(ts, 0) {
			return nil, fmt.Errorf("invalid metric timestamp")
		}
		if now-ts > p.MaxAge {
			return nil, fmt.Errorf("stale metric sample (age=%.1fs > %.1fs)", now-ts, p.MaxAge)
		}
		val, err := parseSampleValue(item.Value[1])
		if err != nil {
			return nil, err
		}
		samples = append(samples, Sample{Metric: item.Metric, Value: val})
	}
	return samples, nil
}

// PerNode validates a 1:1 node-labeled vector covering all expected nodes.
func (p *Prometheus) PerNode(ctx context.Context, expr string, expectedNodes []string) (map[string]float64, error) {
	samples, err := p.Vector(ctx, expr)
	if err != nil {
		return nil, err
	}
	expected := make(map[string]bool, len(expectedNodes))
	for _, n := range expectedNodes {
		expected[n] = true
	}
	values := make(map[string]float64, len(expectedNodes))
	for _, s := range samples {
		node := s.Metric["node"]
		if node == "" {
			return nil, fmt.Errorf("missing node label")
		}
		if !expected[node] {
			return nil, fmt.Errorf("unexpected node %q in telemetry", node)
		}
		if _, dup := values[node]; dup {
			return nil, fmt.Errorf("duplicate telemetry series for node %q", node)
		}
		if s.Value < 0 {
			return nil, fmt.Errorf("negative metric value for node %q", node)
		}
		values[node] = s.Value
	}
	if len(values) != len(expectedNodes) {
		return nil, fmt.Errorf("missing eligible nodes in telemetry (got %d, want %d)", len(values), len(expectedNodes))
	}
	return values, nil
}
