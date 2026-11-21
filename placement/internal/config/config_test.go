package config

import (
	"io"
	"os"
	"path/filepath"
	"testing"

	"gopkg.in/yaml.v3"
)

func TestRepoConfigAndManifestLoad(t *testing.T) {
	cfgPath := filepath.Join("..", "..", "..", "config.yaml")
	s, err := LoadSettings(cfgPath)
	if err != nil {
		t.Fatalf("LoadSettings(%s) failed: %v", cfgPath, err)
	}
	if s.SchedulerName != "mec-placement-scheduler" {
		t.Fatalf("SchedulerName = %q, want mec-placement-scheduler", s.SchedulerName)
	}
	ns, name, err := WorkloadIdentity(s.Manifest, s.Namespace)
	if err != nil {
		t.Fatalf("WorkloadIdentity(%s) failed: %v", s.Manifest, err)
	}
	if ns != "services" || name != "mec-placement-pedestrian-detection" {
		t.Fatalf("got (%q, %q), want (services, mec-placement-pedestrian-detection)", ns, name)
	}
}

func TestDeployConfigMapLoadsCleanly(t *testing.T) {
	deployPath := filepath.Join("..", "..", "deploy.yaml")
	f, err := os.Open(deployPath)
	if err != nil {
		t.Fatalf("open deploy.yaml: %v", err)
	}
	defer f.Close()

	dec := yaml.NewDecoder(f)
	var cfgYAML, manifestYAML string
	foundDeployment := false
	for {
		var doc map[string]any
		err := dec.Decode(&doc)
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("decode deploy.yaml: %v", err)
		}
		if doc == nil {
			continue
		}
		if doc["kind"] == "ConfigMap" {
			data, _ := doc["data"].(map[string]any)
			cfgYAML, _ = data["config.yaml"].(string)
			manifestYAML, _ = data["manifest.yaml"].(string)
		}
		if doc["kind"] == "Deployment" {
			foundDeployment = true
		}
	}
	if !foundDeployment || cfgYAML == "" || manifestYAML == "" {
		t.Fatalf("deploy.yaml must define Deployment and placement-controller-config data")
	}

	dir := t.TempDir()
	// Verify relative path resolution inside ConfigMap (/etc/placement/manifest.yaml).
	manifestPath := filepath.Join(dir, "manifest.yaml")
	if err := os.WriteFile(manifestPath, []byte(manifestYAML), 0o600); err != nil {
		t.Fatal(err)
	}
	var raw map[string]any
	if err := yaml.Unmarshal([]byte(cfgYAML), &raw); err != nil {
		t.Fatal(err)
	}
	raw["manifest"] = manifestPath
	rewritten, err := yaml.Marshal(raw)
	if err != nil {
		t.Fatal(err)
	}
	cfgPath := filepath.Join(dir, "config.yaml")
	if err := os.WriteFile(cfgPath, rewritten, 0o600); err != nil {
		t.Fatal(err)
	}
	s, err := LoadSettings(cfgPath)
	if err != nil {
		t.Fatalf("LoadSettings from deploy ConfigMap failed: %v", err)
	}
	if !s.Mutate || s.Mode != "mobility-aware" || s.SchedulerName != "mec-placement-scheduler" {
		t.Fatalf("unexpected deploy settings: %+v", s)
	}
	ns, name, err := WorkloadIdentity(s.Manifest, s.Namespace)
	if err != nil || ns != "services" || name != "mec-placement-pedestrian-detection" {
		t.Fatalf("unexpected identity (%q, %q) err=%v", ns, name, err)
	}
}

func TestLoadSettingsAcceptsLegacyAndMinimalTwoDimConfigs(t *testing.T) {
	dir := t.TempDir()
	manifestPath := filepath.Join(dir, "manifest.yaml")
	if err := os.WriteFile(manifestPath, []byte("apiVersion: apps/v1\nkind: Deployment\nmetadata:\n  name: app\n  namespace: services\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	legacy := `
manifest: ` + manifestPath + `
prometheus_url: http://prom:9090
mode: mobility-aware
mutate: true
eligible_nodes: [gnb1, gnb-2, gnb-3]
cpu_utilization_query: cpu
mobility_query: mob
latency_query: lat
slo_seconds: 0.15
min_replicas: 1
max_replicas: 12
interval_seconds: 15
cooldown_seconds: 120
scale_down_seconds: 300
gpu_nodes: [gnb-2, gnb-3]
gpu_utilization_query: gpu
vram_utilization_query: vram
resource_weights: {cpu: 0.5, memory: 0.5, gpu: 0.0, vram: 0.0}
`
	legacyPath := filepath.Join(dir, "legacy.yaml")
	if err := os.WriteFile(legacyPath, []byte(legacy), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadSettings(legacyPath); err != nil {
		t.Fatalf("legacy config rejected: %v", err)
	}

	minimal := `
manifest: ` + manifestPath + `
prometheus_url: http://prom:9090
mode: mobility-aware
mutate: true
eligible_nodes: [gnb1, gnb-2, gnb-3]
cpu_utilization_query: cpu
mobility_query: mob
resource_weights: {cpu: 0.5, memory: 0.5}
`
	minimalPath := filepath.Join(dir, "minimal.yaml")
	if err := os.WriteFile(minimalPath, []byte(minimal), 0o600); err != nil {
		t.Fatal(err)
	}
	s, err := LoadSettings(minimalPath)
	if err != nil {
		t.Fatalf("minimal 2D config rejected: %v", err)
	}
	if s.ResourceWeights["cpu"] != 0.5 || s.ResourceWeights["memory"] != 0.5 || len(s.ResourceWeights) != 2 {
		t.Fatalf("unexpected 2D weights: %+v", s.ResourceWeights)
	}
}
