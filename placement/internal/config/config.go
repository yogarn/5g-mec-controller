package config

import (
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"strings"

	"gopkg.in/yaml.v3"
)

// Settings mirrors config.yaml for the Go scheduler and descheduler components.
type Settings struct {
	Manifest                  string             `yaml:"manifest"`
	Namespace                 string             `yaml:"namespace"`
	PrometheusURL             string             `yaml:"prometheus_url"`
	Mode                      string             `yaml:"mode"`
	Mutate                    bool               `yaml:"mutate"`
	EligibleNodes             []string           `yaml:"eligible_nodes"`
	CPUUtilizationQuery       string             `yaml:"cpu_utilization_query"`
	MobilityQuery             string             `yaml:"mobility_query"`
	CPUSaturation             float64            `yaml:"cpu_saturation"`
	LocalityWeight            float64            `yaml:"locality_weight"`
	PodShareScoring           bool               `yaml:"pod_share_scoring"`
	MaxMetricAgeSeconds       float64            `yaml:"max_metric_age_seconds"`
	ResourceWeights           map[string]float64 `yaml:"resource_weights"`
	ZeroLocalityNodes         []string           `yaml:"zero_locality_nodes"`
	DescheduleIntervalSeconds float64            `yaml:"deschedule_interval_seconds"`
	SchedulerName             string             `yaml:"scheduler_name"`
	SchedulerIntervalSeconds  float64            `yaml:"scheduler_interval_seconds"`
}

// DefaultResourceWeights defines the 2D (CPU, memory) resource headroom weights.
func DefaultResourceWeights() map[string]float64 {
	return map[string]float64{"cpu": 0.5, "memory": 0.5}
}

// ValidateResourceWeights checks that cpu and memory weights are non-negative and sum to 1.
func ValidateResourceWeights(weights map[string]float64) error {
	cpu, okCPU := weights["cpu"]
	mem, okMem := weights["memory"]
	if !okCPU || !okMem || cpu < 0 || mem < 0 || math.Abs(cpu+mem-1.0) > 1e-9 {
		return fmt.Errorf("resource_weights must contain non-negative cpu and memory summing to 1.0")
	}
	return nil
}

// LoadSettings decodes repo-authored config.yaml directly into Settings with defaults.
func LoadSettings(path string) (*Settings, error) {
	absPath, err := filepath.Abs(path)
	if err != nil {
		return nil, err
	}
	raw, err := os.ReadFile(absPath)
	if err != nil {
		return nil, err
	}
	s := &Settings{
		CPUSaturation:             0.8,
		LocalityWeight:            0.4,
		MaxMetricAgeSeconds:       30,
		DescheduleIntervalSeconds: 30,
		SchedulerName:             "mec-placement-scheduler",
		SchedulerIntervalSeconds:  1.0,
		ResourceWeights:           DefaultResourceWeights(),
	}
	if err := yaml.Unmarshal(raw, s); err != nil {
		return nil, err
	}
	if !filepath.IsAbs(s.Manifest) {
		s.Manifest = filepath.Join(filepath.Dir(absPath), s.Manifest)
	}
	s.PrometheusURL = strings.TrimRight(s.PrometheusURL, "/")
	if err := ValidateResourceWeights(s.ResourceWeights); err != nil {
		return nil, err
	}
	return s, nil
}

// WorkloadIdentity extracts (namespace, name) from the single Deployment in manifest.
func WorkloadIdentity(manifestPath, configuredNamespace string) (string, string, error) {
	f, err := os.Open(manifestPath)
	if err != nil {
		return "", "", err
	}
	defer f.Close()

	dec := yaml.NewDecoder(f)
	for {
		var doc map[string]any
		err := dec.Decode(&doc)
		if err == io.EOF {
			break
		}
		if err != nil {
			return "", "", err
		}
		if doc != nil && doc["apiVersion"] == "apps/v1" && doc["kind"] == "Deployment" {
			meta, _ := doc["metadata"].(map[string]any)
			name, _ := meta["name"].(string)
			ns, _ := meta["namespace"].(string)
			if configuredNamespace != "" {
				ns = configuredNamespace
			}
			return ns, name, nil
		}
	}
	return "", "", fmt.Errorf("manifest must contain an apps/v1 Deployment")
}
