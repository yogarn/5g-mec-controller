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
	LatencyQuery              string             `yaml:"latency_query"`
	SLOSeconds                float64            `yaml:"slo_seconds"`
	MinReplicas               int                `yaml:"min_replicas"`
	MaxReplicas               int                `yaml:"max_replicas"`
	IntervalSeconds           float64            `yaml:"interval_seconds"`
	Mode                      string             `yaml:"mode"`
	Mutate                    bool               `yaml:"mutate"`
	EligibleNodes             []string           `yaml:"eligible_nodes"`
	CPUUtilizationQuery       string             `yaml:"cpu_utilization_query"`
	MobilityQuery             string             `yaml:"mobility_query"`
	CPUSaturation             float64            `yaml:"cpu_saturation"`
	LocalityWeight            float64            `yaml:"locality_weight"`
	CooldownSeconds           float64            `yaml:"cooldown_seconds"`
	MaxMetricAgeSeconds       float64            `yaml:"max_metric_age_seconds"`
	ScaleDownSeconds          float64            `yaml:"scale_down_seconds"`
	ResourceWeights           map[string]float64 `yaml:"resource_weights"`
	GPUNodes                  []string           `yaml:"gpu_nodes"`
	GPUUtilizationQuery       string             `yaml:"gpu_utilization_query"`
	VRAMUtilizationQuery      string             `yaml:"vram_utilization_query"`
	ZeroLocalityNodes         []string           `yaml:"zero_locality_nodes"`
	DescheduleIntervalSeconds float64            `yaml:"deschedule_interval_seconds"`
	SchedulerName             string             `yaml:"scheduler_name"`
	SchedulerIntervalSeconds  float64            `yaml:"scheduler_interval_seconds"`
}

// DefaultResourceWeights matches controller/policy.py DEFAULT_RESOURCE_WEIGHTS.
func DefaultResourceWeights() map[string]float64 {
	return map[string]float64{
		"cpu":    0.5,
		"memory": 0.5,
		"gpu":    0.0,
		"vram":   0.0,
	}
}

// ValidateResourceWeights checks that cpu, memory, gpu, and vram weights are finite, non-negative, and sum to 1.
func ValidateResourceWeights(weights map[string]float64) error {
	expected := map[string]bool{"cpu": true, "memory": true, "gpu": true, "vram": true}
	if len(weights) != len(expected) {
		return fmt.Errorf("resource_weights must contain cpu, memory, gpu, and vram keys")
	}
	sum := 0.0
	for k, v := range weights {
		if !expected[k] {
			return fmt.Errorf("unknown resource_weight key %q", k)
		}
		if math.IsNaN(v) || math.IsInf(v, 0) || v < 0 {
			return fmt.Errorf("invalid resource_weight for %s", k)
		}
		sum += v
	}
	if math.Abs(sum-1.0) > 1e-9 {
		return fmt.Errorf("resource_weights must sum to 1.0 (got %.9f)", sum)
	}
	return nil
}

// LoadSettings parses and strictly validates config.yaml identically to controller/config.py.
func LoadSettings(path string) (*Settings, error) {
	absPath, err := filepath.Abs(path)
	if err != nil {
		return nil, err
	}
	raw, err := os.ReadFile(absPath)
	if err != nil {
		return nil, err
	}
	var rawMap map[string]any
	if err := yaml.Unmarshal(raw, &rawMap); err != nil {
		return nil, err
	}
	if rawMap == nil {
		return nil, fmt.Errorf("configuration must be a mapping")
	}
	allowed := map[string]bool{
		"manifest": true, "namespace": true, "prometheus_url": true, "latency_query": true,
		"slo_seconds": true, "min_replicas": true, "max_replicas": true, "interval_seconds": true,
		"mode": true, "mutate": true, "eligible_nodes": true, "cpu_utilization_query": true,
		"mobility_query": true, "cpu_saturation": true, "locality_weight": true,
		"cooldown_seconds": true, "max_metric_age_seconds": true, "scale_down_seconds": true,
		"resource_weights": true, "gpu_nodes": true, "gpu_utilization_query": true,
		"vram_utilization_query": true, "zero_locality_nodes": true,
		"deschedule_interval_seconds": true, "scheduler_name": true, "scheduler_interval_seconds": true,
	}
	for k := range rawMap {
		if !allowed[k] {
			return nil, fmt.Errorf("unknown configuration key: %s", k)
		}
	}

	s := &Settings{
		IntervalSeconds:           15,
		CPUSaturation:             0.8,
		LocalityWeight:            0.4,
		CooldownSeconds:           120,
		MaxMetricAgeSeconds:       120,
		ScaleDownSeconds:          300,
		DescheduleIntervalSeconds: 120,
		SchedulerName:             "mec-placement-scheduler",
		SchedulerIntervalSeconds:  1.0,
		ResourceWeights:           DefaultResourceWeights(),
	}
	if err := yaml.Unmarshal(raw, s); err != nil {
		return nil, err
	}
	if strings.TrimSpace(s.Manifest) == "" {
		return nil, fmt.Errorf("manifest must be a nonempty string")
	}
	if !filepath.IsAbs(s.Manifest) {
		s.Manifest = filepath.Join(filepath.Dir(absPath), s.Manifest)
	}
	if strings.TrimSpace(s.PrometheusURL) == "" {
		return nil, fmt.Errorf("prometheus_url must be a nonempty string")
	}
	s.PrometheusURL = strings.TrimRight(s.PrometheusURL, "/")
	if s.Mode != "scaling-only" && s.Mode != "mobility-aware" {
		return nil, fmt.Errorf("mode must be scaling-only or mobility-aware")
	}
	if err := ValidateResourceWeights(s.ResourceWeights); err != nil {
		return nil, err
	}
	if math.IsNaN(s.CPUSaturation) || math.IsInf(s.CPUSaturation, 0) || s.CPUSaturation <= 0 || s.CPUSaturation > 1 {
		return nil, fmt.Errorf("invalid cpu_saturation")
	}
	if math.IsNaN(s.LocalityWeight) || math.IsInf(s.LocalityWeight, 0) || s.LocalityWeight < 0 || s.LocalityWeight > 1 {
		return nil, fmt.Errorf("invalid locality_weight")
	}
	if s.MaxMetricAgeSeconds <= 0 || s.DescheduleIntervalSeconds <= 0 || s.SchedulerIntervalSeconds <= 0 {
		return nil, fmt.Errorf("intervals and max_metric_age_seconds must be positive")
	}
	if strings.TrimSpace(s.SchedulerName) == "" {
		return nil, fmt.Errorf("scheduler_name must be a nonempty string")
	}

	eligibleSet := make(map[string]bool, len(s.EligibleNodes))
	for _, n := range s.EligibleNodes {
		if n == "" || eligibleSet[n] {
			return nil, fmt.Errorf("eligible_nodes must contain unique nonempty names")
		}
		eligibleSet[n] = true
	}
	zeroSet := make(map[string]bool, len(s.ZeroLocalityNodes))
	for _, n := range s.ZeroLocalityNodes {
		if strings.TrimSpace(n) == "" || zeroSet[n] || !eligibleSet[n] {
			return nil, fmt.Errorf("zero_locality_nodes must be a unique subset of eligible_nodes")
		}
		zeroSet[n] = true
	}
	if len(s.EligibleNodes) > 0 && len(s.ZeroLocalityNodes) == len(s.EligibleNodes) {
		return nil, fmt.Errorf("at least one eligible node must require locality telemetry")
	}
	gpuSet := make(map[string]bool, len(s.GPUNodes))
	for _, n := range s.GPUNodes {
		if strings.TrimSpace(n) == "" || gpuSet[n] || !eligibleSet[n] {
			return nil, fmt.Errorf("gpu_nodes must be a unique subset of eligible_nodes")
		}
		gpuSet[n] = true
	}
	if s.Mode == "mobility-aware" {
		if len(s.EligibleNodes) == 0 || strings.TrimSpace(s.CPUUtilizationQuery) == "" || strings.TrimSpace(s.MobilityQuery) == "" {
			return nil, fmt.Errorf("mobility-aware mode requires eligible_nodes, cpu_utilization_query, and mobility_query")
		}
		if _, ok := rawMap["gpu_nodes"]; !ok {
			return nil, fmt.Errorf("mobility-aware mode requires an explicit gpu_nodes inventory")
		}
		if len(s.GPUNodes) > 0 && (s.ResourceWeights["gpu"] > 0 || s.ResourceWeights["vram"] > 0) {
			if strings.TrimSpace(s.GPUUtilizationQuery) == "" || strings.TrimSpace(s.VRAMUtilizationQuery) == "" {
				return nil, fmt.Errorf("GPU-capable nodes with GPU/VRAM weights require GPU and VRAM queries")
			}
		}
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

	type deploymentMeta struct {
		name      string
		namespace string
	}
	dec := yaml.NewDecoder(f)
	var deployments []deploymentMeta
	for {
		var doc map[string]any
		err := dec.Decode(&doc)
		if err == io.EOF {
			break
		}
		if err != nil {
			return "", "", err
		}
		if doc == nil {
			continue
		}
		if doc["apiVersion"] == "apps/v1" && doc["kind"] == "Deployment" {
			meta, _ := doc["metadata"].(map[string]any)
			name, _ := meta["name"].(string)
			ns, _ := meta["namespace"].(string)
			deployments = append(deployments, deploymentMeta{name: name, namespace: ns})
		}
	}
	if len(deployments) != 1 {
		return "", "", fmt.Errorf("manifest must contain exactly one apps/v1 Deployment")
	}
	name := deployments[0].name
	declared := deployments[0].namespace
	if name == "" {
		return "", "", fmt.Errorf("Deployment requires metadata.name")
	}
	if configuredNamespace != "" && declared != "" && configuredNamespace != declared {
		return "", "", fmt.Errorf("configured namespace conflicts with manifest")
	}
	ns := configuredNamespace
	if ns == "" {
		ns = declared
	}
	if ns == "" {
		return "", "", fmt.Errorf("namespace must be provided in Deployment or configuration")
	}
	return ns, name, nil
}
