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
	PreferClose               bool               `yaml:"prefer_close"`
	MaxMetricAgeSeconds       float64            `yaml:"max_metric_age_seconds"`
	ResourceWeights           map[string]float64 `yaml:"resource_weights"`
	ZeroLocalityNodes         []string           `yaml:"zero_locality_nodes"`
	DescheduleIntervalSeconds float64            `yaml:"deschedule_interval_seconds"`
	SchedulerName             string             `yaml:"scheduler_name"`
	SchedulerIntervalSeconds  float64            `yaml:"scheduler_interval_seconds"`
}

// DefaultResourceWeights defines the 2D (CPU, memory) resource headroom weights.
func DefaultResourceWeights() map[string]float64 {
	return map[string]float64{
		"cpu":    0.5,
		"memory": 0.5,
	}
}

// ValidateResourceWeights checks that cpu and memory weights are finite, non-negative, and sum to 1.
// Legacy 4D configs that also include gpu and vram keys are accepted (gpu/vram are ignored by scoring).
func ValidateResourceWeights(weights map[string]float64) error {
	if _, ok := weights["cpu"]; !ok {
		return fmt.Errorf("resource_weights must contain cpu and memory keys")
	}
	if _, ok := weights["memory"]; !ok {
		return fmt.Errorf("resource_weights must contain cpu and memory keys")
	}
	allowed := map[string]bool{"cpu": true, "memory": true, "gpu": true, "vram": true}
	sum := 0.0
	for k, v := range weights {
		if !allowed[k] {
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
		"manifest": true, "namespace": true, "prometheus_url": true,
		"mode": true, "mutate": true, "eligible_nodes": true, "cpu_utilization_query": true,
		"mobility_query": true, "cpu_saturation": true, "locality_weight": true, "prefer_close": true,
		"max_metric_age_seconds": true, "resource_weights": true, "zero_locality_nodes": true,
		"deschedule_interval_seconds": true, "scheduler_name": true, "scheduler_interval_seconds": true,
		// Legacy keys accepted and ignored for backward compatibility with older ConfigMaps:
		"latency_query": true, "slo_seconds": true, "min_replicas": true, "max_replicas": true,
		"interval_seconds": true, "cooldown_seconds": true, "scale_down_seconds": true,
		"gpu_nodes": true, "gpu_utilization_query": true, "vram_utilization_query": true,
	}
	for k := range rawMap {
		if !allowed[k] {
			return nil, fmt.Errorf("unknown configuration key: %s", k)
		}
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
	if s.Mode == "mobility-aware" {
		if len(s.EligibleNodes) == 0 || strings.TrimSpace(s.CPUUtilizationQuery) == "" || strings.TrimSpace(s.MobilityQuery) == "" {
			return nil, fmt.Errorf("mobility-aware mode requires eligible_nodes, cpu_utilization_query, and mobility_query")
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
