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
	if !s.Mutate || s.Mode != "mobility-aware" || s.SchedulerName != "mec-placement-scheduler" || !s.PodShareScoring {
		t.Fatalf("unexpected deploy settings: %+v", s)
	}
	ns, name, err := WorkloadIdentity(s.Manifest, s.Namespace)
	if err != nil || ns != "services" || name != "mec-placement-pedestrian-detection" {
		t.Fatalf("unexpected identity (%q, %q) err=%v", ns, name, err)
	}
}

func TestPodShareScoringConfig(t *testing.T) {
	base := "manifest: workload.yaml\nprometheus_url: http://prom:9090\nmode: mobility-aware\n"
	for _, tc := range []struct {
		name    string
		options string
		enabled bool
	}{
		{name: "default"},
		{name: "explicit", options: "pod_share_scoring: true\n", enabled: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "config.yaml")
			if err := os.WriteFile(path, []byte(base+tc.options), 0o600); err != nil {
				t.Fatal(err)
			}
			settings, err := LoadSettings(path)
			if err != nil {
				t.Fatal(err)
			}
			if settings.PodShareScoring != tc.enabled {
				t.Fatalf("PodShareScoring = %t, want %t", settings.PodShareScoring, tc.enabled)
			}
		})
	}
}

func TestLoadSettingsMinimalTwoDimConfig(t *testing.T) {
	dir := t.TempDir()
	manifestPath := filepath.Join(dir, "manifest.yaml")
	if err := os.WriteFile(manifestPath, []byte("apiVersion: apps/v1\nkind: Deployment\nmetadata:\n  name: app\n  namespace: services\n"), 0o600); err != nil {
		t.Fatal(err)
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
