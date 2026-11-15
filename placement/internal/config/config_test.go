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
