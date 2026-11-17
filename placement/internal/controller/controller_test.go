package controller

import (
	"bytes"
	"context"
	"fmt"
	"log"
	"strings"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"

	"github.com/yogarn/5g-mec-controller/placement/internal/config"
)

type fakeTelemetry struct {
	cpu     map[string]float64
	traffic map[string]float64
	err     error
}

func (f *fakeTelemetry) PerNode(_ context.Context, expr string, expectedNodes []string) (map[string]float64, error) {
	if f.err != nil {
		return nil, f.err
	}
	src := f.traffic
	if expr == "cpu" {
		src = f.cpu
	}
	out := make(map[string]float64, len(expectedNodes))
	for _, n := range expectedNodes {
		v, ok := src[n]
		if !ok {
			return nil, fmt.Errorf("missing node %s", n)
		}
		out[n] = v
	}
	return out, nil
}

func makeTestNode(name, cpuCap, memCap string) *corev1.Node {
	return &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Status: corev1.NodeStatus{
			Allocatable: corev1.ResourceList{
				corev1.ResourceCPU:    resource.MustParse(cpuCap),
				corev1.ResourceMemory: resource.MustParse(memCap),
			},
			Conditions: []corev1.NodeCondition{
				{Type: corev1.NodeReady, Status: corev1.ConditionTrue},
			},
		},
	}
}

func makeTestPod(name, uid, node, scheduler string, ready bool, cpuReq, memReq string) *corev1.Pod {
	status := corev1.ConditionFalse
	if ready {
		status = corev1.ConditionTrue
	}
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: "services",
			UID:       types.UID(uid),
			Labels:    map[string]string{"app": "pedestrian"},
		},
		Spec: corev1.PodSpec{
			NodeName:      node,
			SchedulerName: scheduler,
			Containers: []corev1.Container{
				{
					Name: "app",
					Resources: corev1.ResourceRequirements{
						Requests: corev1.ResourceList{
							corev1.ResourceCPU:    resource.MustParse(cpuReq),
							corev1.ResourceMemory: resource.MustParse(memReq),
						},
					},
				},
			},
		},
		Status: corev1.PodStatus{
			Phase: corev1.PodRunning,
			Conditions: []corev1.PodCondition{
				{Type: corev1.PodReady, Status: status},
			},
		},
	}
}

func makeTestDeployment(replicas int32) *appsv1.Deployment {
	return &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{Name: "pedestrian", Namespace: "services"},
		Spec: appsv1.DeploymentSpec{
			Replicas: &replicas,
			Selector: &metav1.LabelSelector{MatchLabels: map[string]string{"app": "pedestrian"}},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{"app": "pedestrian"}},
				Spec: corev1.PodSpec{
					SchedulerName: "mec-placement-scheduler",
					Containers: []corev1.Container{
						{
							Name: "app",
							Resources: corev1.ResourceRequirements{
								Requests: corev1.ResourceList{
									corev1.ResourceCPU:    resource.MustParse("1"),
									corev1.ResourceMemory: resource.MustParse("1Gi"),
								},
							},
						},
					},
				},
			},
		},
	}
}

func testSettings() *config.Settings {
	return &config.Settings{
		Mode:                      "mobility-aware",
		Mutate:                    true,
		EligibleNodes:             []string{"a", "b"},
		CPUUtilizationQuery:       "cpu",
		MobilityQuery:             "traffic",
		CPUSaturation:             0.8,
		LocalityWeight:            0.4,
		ResourceWeights:           config.DefaultResourceWeights(),
		DescheduleIntervalSeconds: 120,
		SchedulerName:             "mec-placement-scheduler",
		SchedulerIntervalSeconds:  1,
	}
}

func TestSchedulerBindingInFlightDeduplicationAndCapacity(t *testing.T) {
	pod0 := makeTestPod("app-0", "uid-0", "", "mec-placement-scheduler", false, "2", "2Gi")
	podDefault := makeTestPod("app-default", "uid-def", "", "default-scheduler", false, "1", "1Gi")
	podBound := makeTestPod("app-bound", "uid-bound", "a", "mec-placement-scheduler", true, "1", "1Gi")

	client := fake.NewSimpleClientset(
		makeTestNode("a", "4", "4Gi"),
		makeTestNode("b", "2", "4Gi"),
		makeTestDeployment(3),
		pod0, podDefault, podBound,
	)
	var bindings []*corev1.Binding
	client.PrependReactor("create", "pods", func(action k8stesting.Action) (bool, runtime.Object, error) {
		if action.GetSubresource() == "binding" {
			ca := action.(k8stesting.CreateAction)
			bindings = append(bindings, ca.GetObject().(*corev1.Binding))
			return true, ca.GetObject(), nil
		}
		return false, nil, nil
	})

	prom := &fakeTelemetry{
		cpu:     map[string]float64{"a": 0.2, "b": 0.2},
		traffic: map[string]float64{"a": 1.0, "b": 9.0},
	}
	ctrl := NewController(testSettings(), client, prom, "services", "pedestrian")
	var logs bytes.Buffer
	ctrl.Logger = log.New(&logs, "", 0)

	ctx := context.Background()
	res, err := ctrl.ReconcileScheduler(ctx)
	if err != nil {
		t.Fatalf("ReconcileScheduler failed: %v", err)
	}
	if len(res) != 1 || res[0] != (BoundPodResult{Pod: "app-0", Node: "b"}) {
		t.Fatalf("unexpected bound results: %+v", res)
	}

	for _, operation := range []string{"scheduler_snapshot", "scheduler_score", "scheduler_bind"} {
		if !strings.Contains(logs.String(), "decision timing operation="+operation+" duration_ms=") {
			t.Fatalf("missing monotonic stage timer %s in %s", operation, logs.String())
		}
	}

	// Scale-out: app-1 arrives while app-0's nodeName is not yet reflected on API server.
	// Must NOT re-bind app-0, and must account for app-0's 2 CPU on b so app-1 binds to a.
	pod1 := makeTestPod("app-1", "uid-1", "", "mec-placement-scheduler", false, "2", "2Gi")
	if _, err := client.CoreV1().Pods("services").Create(ctx, pod1, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	res2, err := ctrl.ReconcileScheduler(ctx)
	if err != nil {
		t.Fatalf("second ReconcileScheduler failed: %v", err)
	}
	if len(res2) != 1 || res2[0] != (BoundPodResult{Pod: "app-1", Node: "a"}) {
		t.Fatalf("expected app-1 bound to a, got %+v", res2)
	}
	if len(bindings) != 2 {
		t.Fatalf("expected 2 bindings total (no duplicate bind), got %d", len(bindings))
	}

	// Once API server reflects nodeName on pod0, inFlightBindings entry is pruned.
	pod0.Spec.NodeName = "b"
	if _, err := client.CoreV1().Pods("services").Update(ctx, pod0, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	if _, err := ctrl.ReconcileScheduler(ctx); err != nil {
		t.Fatal(err)
	}
	if _, stillInFlight := ctrl.inFlightBindings["uid-0"]; stillInFlight {
		t.Fatalf("expected uid-0 to be pruned from inFlightBindings once nodeName is set")
	}
}

func TestDeschedulerEvictionAndPostEvictionRescoringAndPDBRejection(t *testing.T) {
	pod0 := makeTestPod("app-0", "uid-0", "a", "mec-placement-scheduler", true, "1", "1Gi")
	client := fake.NewSimpleClientset(
		makeTestNode("a", "4", "4Gi"),
		makeTestNode("b", "4", "4Gi"),
		makeTestDeployment(1),
		pod0,
	)

	pdbBlock := true
	var evicted []string
	var bindings []*corev1.Binding
	client.PrependReactor("create", "pods", func(action k8stesting.Action) (bool, runtime.Object, error) {
		ca := action.(k8stesting.CreateAction)
		if action.GetSubresource() == "eviction" {
			if pdbBlock {
				return true, nil, apierrors.NewTooManyRequests("Cannot evict pod as it would violate PodDisruptionBudget", 1)
			}
			evicted = append(evicted, ca.GetObject().(metav1.Object).GetName())
			return true, ca.GetObject(), nil
		}
		if action.GetSubresource() == "binding" {
			bindings = append(bindings, ca.GetObject().(*corev1.Binding))
			return true, ca.GetObject(), nil
		}
		return false, nil, nil
	})

	prom := &fakeTelemetry{
		cpu:     map[string]float64{"a": 0.2, "b": 0.2},
		traffic: map[string]float64{"a": 0.0, "b": 10.0},
	}
	now := time.Unix(1000, 0)
	ctrl := NewController(testSettings(), client, prom, "services", "pedestrian")
	ctrl.Now = func() time.Time { return now }
	var logs bytes.Buffer
	ctrl.Logger = log.New(&logs, "", 0)

	ctx := context.Background()
	// 1. PDB blocks eviction (HTTP 429): handles cleanly without error.
	cand, err := ctrl.ReconcileDescheduler(ctx)
	if err != nil || cand != nil {
		t.Fatalf("expected clean PDB rejection, cand=%+v err=%v", cand, err)
	}
	if !strings.Contains(logs.String(), "pdb-or-rate-limit") {
		t.Fatalf("expected PDB log, got:\n%s", logs.String())
	}

	// 2. Allow eviction after interval elapses.
	pdbBlock = false
	now = now.Add(121 * time.Second)
	cand, err = ctrl.ReconcileDescheduler(ctx)
	if err != nil || cand == nil || cand.Source != "a" || cand.Destination != "b" {
		t.Fatalf("expected eviction from a (candidate b), got %+v err=%v", cand, err)
	}
	if len(evicted) != 1 || evicted[0] != "app-0" {
		t.Fatalf("expected app-0 evicted, got %v", evicted)
	}

	// Verify Descheduler NEVER patches Deployment.spec.replicas.
	for _, a := range client.Actions() {
		if a.GetResource().Resource == "deployments" && a.GetVerb() != "get" && a.GetVerb() != "list" {
			t.Fatalf("controller must not mutate Deployment, got %s", a.GetVerb())
		}
	}

	// 3. ReplicaSet removes evicted pod and creates replacement pod; traffic flipped to node a.
	if err := client.CoreV1().Pods("services").Delete(ctx, "app-0", metav1.DeleteOptions{}); err != nil {
		t.Fatal(err)
	}
	replacement := makeTestPod("app-1", "uid-1", "", "mec-placement-scheduler", false, "1", "1Gi")
	if _, err := client.CoreV1().Pods("services").Create(ctx, replacement, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	prom.traffic = map[string]float64{"a": 9.0, "b": 1.0}
	bound, err := ctrl.ReconcileScheduler(ctx)
	if err != nil || len(bound) != 1 || bound[0] != (BoundPodResult{Pod: "app-1", Node: "a"}) {
		t.Fatalf("expected replacement re-scored to node a, got %+v err=%v", bound, err)
	}
}

func TestRestartAndBindingConflictRecovery(t *testing.T) {
	boundOnB := makeTestPod("app-0", "uid-0", "b", "mec-placement-scheduler", true, "2", "2Gi")
	pending := makeTestPod("app-1", "uid-1", "", "mec-placement-scheduler", false, "2", "2Gi")
	client := fake.NewSimpleClientset(
		makeTestNode("a", "4", "4Gi"),
		makeTestNode("b", "2", "4Gi"),
		makeTestDeployment(2),
		boundOnB, pending,
	)

	conflict := true
	client.PrependReactor("create", "pods", func(action k8stesting.Action) (bool, runtime.Object, error) {
		if action.GetSubresource() == "binding" {
			if conflict {
				return true, nil, apierrors.NewConflict(schema.GroupResource{Resource: "pods"}, "app-1", fmt.Errorf("already bound"))
			}
			ca := action.(k8stesting.CreateAction)
			return true, ca.GetObject(), nil
		}
		return false, nil, nil
	})

	prom := &fakeTelemetry{
		cpu:     map[string]float64{"a": 0.2, "b": 0.2},
		traffic: map[string]float64{"a": 1.0, "b": 9.0},
	}
	ctrl := NewController(testSettings(), client, prom, "services", "pedestrian")
	ctrl.Logger = log.New(&bytes.Buffer{}, "", 0)

	ctx := context.Background()
	// First pass sees 409 Conflict -> does not poison inFlightBindings.
	res, err := ctrl.ReconcileScheduler(ctx)
	if err != nil || len(res) != 0 {
		t.Fatalf("expected empty result on 409 conflict without crash, got %+v err=%v", res, err)
	}
	if len(ctrl.inFlightBindings) != 0 {
		t.Fatalf("expected no stale inFlightBindings on failed bind")
	}

	// Simulate fresh restart after conflict clears: must account for boundOnB (b full) and pick node a.
	conflict = false
	restarted := NewController(testSettings(), client, prom, "services", "pedestrian")
	restarted.Logger = log.New(&bytes.Buffer{}, "", 0)
	res, err = restarted.ReconcileScheduler(ctx)
	if err != nil || len(res) != 1 || res[0] != (BoundPodResult{Pod: "app-1", Node: "a"}) {
		t.Fatalf("expected restarted controller to bind app-1 to a, got %+v err=%v", res, err)
	}
}

func TestSchedulerAndDeschedulerEnforceNodeConstraints(t *testing.T) {
	nodeA := makeTestNode("a", "4", "4Gi")
	nodeB := makeTestNode("b", "4", "4Gi")
	nodeB.Labels = map[string]string{"pool": "restricted"}

	dep := makeTestDeployment(1)
	dep.Spec.Template.Spec.Affinity = &corev1.Affinity{
		NodeAffinity: &corev1.NodeAffinity{
			RequiredDuringSchedulingIgnoredDuringExecution: &corev1.NodeSelector{
				NodeSelectorTerms: []corev1.NodeSelectorTerm{
					{
						MatchFields: []corev1.NodeSelectorRequirement{
							{Key: "metadata.name", Operator: corev1.NodeSelectorOpIn, Values: []string{"a"}},
						},
					},
				},
			},
		},
	}

	pending := makeTestPod("app-1", "uid-1", "", "mec-placement-scheduler", false, "1", "1Gi")
	pending.Spec.Affinity = dep.Spec.Template.Spec.Affinity.DeepCopy()

	client := fake.NewSimpleClientset(nodeA, nodeB, dep, pending)
	var bindings []*corev1.Binding
	client.PrependReactor("create", "pods", func(action k8stesting.Action) (bool, runtime.Object, error) {
		if action.GetSubresource() == "binding" {
			ca := action.(k8stesting.CreateAction)
			bindings = append(bindings, ca.GetObject().(*corev1.Binding))
			return true, ca.GetObject(), nil
		}
		return false, nil, nil
	})

	// Node b has all traffic, so unconstrained scoring would pick b. Required affinity restricts to a.
	prom := &fakeTelemetry{
		cpu:     map[string]float64{"a": 0.2, "b": 0.2},
		traffic: map[string]float64{"a": 0.0, "b": 10.0},
	}
	ctrl := NewController(testSettings(), client, prom, "services", "pedestrian")
	ctrl.Logger = log.New(&bytes.Buffer{}, "", 0)

	ctx := context.Background()
	res, err := ctrl.ReconcileScheduler(ctx)
	if err != nil || len(res) != 1 || res[0].Node != "a" {
		t.Fatalf("expected pod bound to a under nodeAffinity, got %+v err=%v", res, err)
	}
	if len(bindings) != 1 || bindings[0].Target.Name != "a" {
		t.Fatalf("unexpected bindings: %+v", bindings)
	}

	// Mark app-1 ready on a; descheduler must NOT evict it toward disallowed node b.
	pending.Spec.NodeName = "a"
	pending.Status.Conditions = []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}}
	if _, err := client.CoreV1().Pods("services").Update(ctx, pending, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	cand, err := ctrl.ReconcileDescheduler(ctx)
	if err != nil || cand != nil {
		t.Fatalf("expected descheduler not to evict toward disallowed node b, cand=%+v err=%v", cand, err)
	}
}

func TestSchedulerAndDeschedulerIgnoreMissingAndOtherScheduler(t *testing.T) {
	// 1. Missing Deployment must be ignored without error (e.g. during campaign reset).
	clientMissing := fake.NewSimpleClientset(makeTestNode("a", "4", "4Gi"))
	prom := &fakeTelemetry{
		cpu:     map[string]float64{"a": 0.2},
		traffic: map[string]float64{"a": 1.0},
	}
	ctrl := NewController(testSettings(), clientMissing, prom, "services", "pedestrian")
	ctrl.Logger = log.New(&bytes.Buffer{}, "", 0)
	ctx := context.Background()
	if bound, err := ctrl.ReconcileScheduler(ctx); err != nil || len(bound) != 0 {
		t.Fatalf("expected nil on missing deployment scheduler, got bound=%+v err=%v", bound, err)
	}
	if cand, err := ctrl.ReconcileDescheduler(ctx); err != nil || cand != nil {
		t.Fatalf("expected nil on missing deployment descheduler, got cand=%+v err=%v", cand, err)
	}

	// 2. Baseline Deployment using default-scheduler must never be evicted by in-cluster descheduler.
	dep := makeTestDeployment(1)
	dep.Spec.Template.Spec.SchedulerName = "default-scheduler"
	baselinePod := makeTestPod("base-0", "uid-base", "a", "default-scheduler", true, "1", "1Gi")
	clientBase := fake.NewSimpleClientset(
		makeTestNode("a", "4", "4Gi"),
		makeTestNode("b", "4", "4Gi"),
		dep,
		baselinePod,
	)
	promBase := &fakeTelemetry{
		cpu:     map[string]float64{"a": 0.2, "b": 0.2},
		traffic: map[string]float64{"a": 0.0, "b": 10.0},
	}
	ctrlBase := NewController(testSettings(), clientBase, promBase, "services", "pedestrian")
	ctrlBase.Logger = log.New(&bytes.Buffer{}, "", 0)
	cand, err := ctrlBase.ReconcileDescheduler(ctx)
	if err != nil || cand != nil {
		t.Fatalf("descheduler must ignore default-scheduler baseline deployment, cand=%+v err=%v", cand, err)
	}
}

func TestDeschedulerTickerPathDoesNotDoubleGateJitter(t *testing.T) {
	settings := testSettings()
	settings.DescheduleIntervalSeconds = 30

	dep := makeTestDeployment(1)
	pod := makeTestPod("app-0", "uid-0", "a", settings.SchedulerName, true, "1", "1Gi")
	client := fake.NewSimpleClientset(
		makeTestNode("a", "4", "4Gi"),
		makeTestNode("b", "4", "4Gi"),
		dep,
		pod,
	)
	prom := &fakeTelemetry{
		cpu:     map[string]float64{"a": 0.2, "b": 0.2},
		traffic: map[string]float64{"a": 0.0, "b": 10.0},
	}

	ctrl := NewController(settings, client, prom, "services", "pedestrian")
	ctrl.Logger = log.New(&bytes.Buffer{}, "", 0)
	now := time.Unix(1000, 0)
	ctrl.Now = func() time.Time { return now }

	ctx := context.Background()
	if cand, err := ctrl.reconcileDescheduler(ctx, false); err != nil || cand == nil {
		t.Fatalf("expected initial eviction, got cand=%+v err=%v", cand, err)
	}

	// Simulate ticker jitter arriving 100ms before 30s has elapsed since lastDescheduleTime.
	now = now.Add(29900 * time.Millisecond)
	if cand, err := ctrl.ReconcileDescheduler(ctx); err != nil || cand != nil {
		t.Fatalf("direct ReconcileDescheduler should still enforce interval, got cand=%+v err=%v", cand, err)
	}
	if cand, err := ctrl.reconcileDescheduler(ctx, false); err != nil || cand == nil {
		t.Fatalf("ticker path reconcileDescheduler(false) must not skip jittered 30s tick, got cand=%+v err=%v", cand, err)
	}
}
