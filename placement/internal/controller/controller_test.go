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

func TestPreferCloseSchedulerPlacementIsIndependentOfCreationBatch(t *testing.T) {
	for _, batchSizes := range [][]int{{13}, {2, 11}} {
		t.Run(fmt.Sprint(batchSizes), func(t *testing.T) {
			ctx := context.Background()
			client := fake.NewSimpleClientset(
				makeTestNode("a", "100", "100Gi"),
				makeTestNode("b", "100", "100Gi"),
				makeTestNode("c", "100", "100Gi"),
				makeTestDeployment(13),
			)
			client.PrependReactor("create", "pods", func(action k8stesting.Action) (bool, runtime.Object, error) {
				if action.GetSubresource() == "binding" {
					return true, action.(k8stesting.CreateAction).GetObject(), nil
				}
				return false, nil, nil
			})
			settings := testSettings()
			settings.EligibleNodes = []string{"a", "b", "c"}
			settings.PreferClose = true
			prom := &fakeTelemetry{
				cpu:     map[string]float64{"a": 0.2, "b": 0.2, "c": 0.2},
				traffic: map[string]float64{"a": 1, "b": 1, "c": 1},
			}
			ctrl := NewController(settings, client, prom, "services", "pedestrian")
			podClient := client.CoreV1().Pods("services")
			index := 0
			counts := make(map[string]int)
			for _, size := range batchSizes {
				for range size {
					name := fmt.Sprintf("app-%02d", index)
					pod := makeTestPod(name, name, "", settings.SchedulerName, false, "1", "1Gi")
					if _, err := podClient.Create(ctx, pod, metav1.CreateOptions{}); err != nil {
						t.Fatal(err)
					}
					index++
				}
				bound, err := ctrl.ReconcileScheduler(ctx)
				if err != nil || len(bound) != size {
					t.Fatalf("bound %d pods, want %d: %v", len(bound), size, err)
				}
				for _, result := range bound {
					counts[result.Node]++
				}
				// Reflect one API binding while leaving the others in flight.
				pod, err := podClient.Get(ctx, bound[0].Pod, metav1.GetOptions{})
				if err != nil {
					t.Fatal(err)
				}
				pod.Spec.NodeName = bound[0].Node
				if _, err := podClient.Update(ctx, pod, metav1.UpdateOptions{}); err != nil {
					t.Fatal(err)
				}
			}
			if counts["a"] != 5 || counts["b"] != 4 || counts["c"] != 4 {
				t.Fatalf("expected 5/4/4 managed replicas, got %v", counts)
			}
		})
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

func makeTerminatingPod(name, uid, node, scheduler, cpuReq, memReq string, deletedAt time.Time) *corev1.Pod {
	pod := makeTestPod(name, uid, node, scheduler, true, cpuReq, memReq)
	ts := metav1.NewTime(deletedAt)
	pod.DeletionTimestamp = &ts
	return pod
}

func TestDeschedulerEvictsWhileOldPodIsTerminating(t *testing.T) {
	now := time.Unix(1000, 0)
	settings := testSettings()
	settings.DescheduleIntervalSeconds = 30

	t.Run("five-ready-plus-one-terminating-can-evict", func(t *testing.T) {
		terminatingOld := makeTerminatingPod("app-0", "uid-0", "a", settings.SchedulerName, "1", "1Gi", now)
		client := fake.NewSimpleClientset(
			makeTestNode("a", "10", "10Gi"),
			makeTestNode("b", "10", "10Gi"),
			makeTestDeployment(5),
			terminatingOld,
			makeTestPod("app-1", "uid-1", "a", settings.SchedulerName, true, "1", "1Gi"),
			makeTestPod("app-2", "uid-2", "a", settings.SchedulerName, true, "1", "1Gi"),
			makeTestPod("app-3", "uid-3", "a", settings.SchedulerName, true, "1", "1Gi"),
			makeTestPod("app-4", "uid-4", "a", settings.SchedulerName, true, "1", "1Gi"),
			makeTestPod("app-5", "uid-5", "b", settings.SchedulerName, true, "1", "1Gi"),
		)
		var evicted []string
		client.PrependReactor("create", "pods", func(action k8stesting.Action) (bool, runtime.Object, error) {
			if action.GetSubresource() == "eviction" {
				ca := action.(k8stesting.CreateAction)
				evicted = append(evicted, ca.GetObject().(metav1.Object).GetName())
				return true, ca.GetObject(), nil
			}
			return false, nil, nil
		})

		prom := &fakeTelemetry{
			cpu:     map[string]float64{"a": 0.2, "b": 0.2},
			traffic: map[string]float64{"a": 0.0, "b": 10.0},
		}
		ctrl := NewController(settings, client, prom, "services", "pedestrian")
		ctrl.Now = func() time.Time { return now }
		ctrl.Logger = log.New(&bytes.Buffer{}, "", 0)

		cand, err := ctrl.ReconcileDescheduler(context.Background())
		if err != nil || cand == nil {
			t.Fatalf("expected eviction while old pod drains, got cand=%+v err=%v", cand, err)
		}
		if cand.Source != "a" || cand.Destination != "b" {
			t.Fatalf("expected source=a dest=b, got %+v", cand)
		}
		if len(evicted) != 1 || evicted[0] != "app-1" {
			t.Fatalf("expected single eviction of non-terminating app-1, got %v", evicted)
		}
	})

	t.Run("terminating-pod-resources-still-accounted-for", func(t *testing.T) {
		// Node b has capacity 2 CPU: 1 CPU held by Ready app-5 and 1 CPU still held by terminating app-0.
		// Even though 5 non-terminating pods are Ready, b has no free CPU so descheduler must not evict toward b.
		terminatingOnB := makeTerminatingPod("app-0", "uid-0", "b", settings.SchedulerName, "1", "1Gi", now)
		client := fake.NewSimpleClientset(
			makeTestNode("a", "10", "10Gi"),
			makeTestNode("b", "2", "2Gi"),
			makeTestDeployment(5),
			terminatingOnB,
			makeTestPod("app-1", "uid-1", "a", settings.SchedulerName, true, "1", "1Gi"),
			makeTestPod("app-2", "uid-2", "a", settings.SchedulerName, true, "1", "1Gi"),
			makeTestPod("app-3", "uid-3", "a", settings.SchedulerName, true, "1", "1Gi"),
			makeTestPod("app-4", "uid-4", "a", settings.SchedulerName, true, "1", "1Gi"),
			makeTestPod("app-5", "uid-5", "b", settings.SchedulerName, true, "1", "1Gi"),
		)
		var evicted []string
		client.PrependReactor("create", "pods", func(action k8stesting.Action) (bool, runtime.Object, error) {
			if action.GetSubresource() == "eviction" {
				ca := action.(k8stesting.CreateAction)
				evicted = append(evicted, ca.GetObject().(metav1.Object).GetName())
				return true, ca.GetObject(), nil
			}
			return false, nil, nil
		})

		prom := &fakeTelemetry{
			cpu:     map[string]float64{"a": 0.2, "b": 0.2},
			traffic: map[string]float64{"a": 0.0, "b": 10.0},
		}
		ctrl := NewController(settings, client, prom, "services", "pedestrian")
		ctrl.Now = func() time.Time { return now }
		ctrl.Logger = log.New(&bytes.Buffer{}, "", 0)

		cand, err := ctrl.ReconcileDescheduler(context.Background())
		if err != nil || cand != nil {
			t.Fatalf("expected no eviction when terminating pod exhausts destination capacity, got cand=%+v err=%v", cand, err)
		}
		if len(evicted) != 0 {
			t.Fatalf("expected no eviction, got %v", evicted)
		}
	})
}

func TestDeschedulerDefersWhenReplacementNotSettled(t *testing.T) {
	now := time.Unix(1000, 0)
	settings := testSettings()
	settings.DescheduleIntervalSeconds = 30

	tests := []struct {
		name           string
		replacementPod *corev1.Pod
		bindFirst      bool
	}{
		{
			name:           "missing",
			replacementPod: nil,
		},
		{
			name:           "unbound",
			replacementPod: makeTestPod("app-5", "uid-5", "", settings.SchedulerName, false, "1", "1Gi"),
		},
		{
			name:           "unready",
			replacementPod: makeTestPod("app-5", "uid-5", "b", settings.SchedulerName, false, "1", "1Gi"),
		},
		{
			name:           "still-binding",
			replacementPod: makeTestPod("app-5", "uid-5", "", settings.SchedulerName, false, "1", "1Gi"),
			bindFirst:      true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			objects := []runtime.Object{
				makeTestNode("a", "10", "10Gi"),
				makeTestNode("b", "10", "10Gi"),
				makeTestDeployment(5),
				makeTerminatingPod("app-0", "uid-0", "a", settings.SchedulerName, "1", "1Gi", now),
				makeTestPod("app-1", "uid-1", "a", settings.SchedulerName, true, "1", "1Gi"),
				makeTestPod("app-2", "uid-2", "a", settings.SchedulerName, true, "1", "1Gi"),
				makeTestPod("app-3", "uid-3", "a", settings.SchedulerName, true, "1", "1Gi"),
				makeTestPod("app-4", "uid-4", "a", settings.SchedulerName, true, "1", "1Gi"),
			}
			if tc.replacementPod != nil {
				objects = append(objects, tc.replacementPod)
			}
			client := fake.NewSimpleClientset(objects...)
			var evicted []string
			client.PrependReactor("create", "pods", func(action k8stesting.Action) (bool, runtime.Object, error) {
				ca := action.(k8stesting.CreateAction)
				if action.GetSubresource() == "eviction" {
					evicted = append(evicted, ca.GetObject().(metav1.Object).GetName())
					return true, ca.GetObject(), nil
				}
				if action.GetSubresource() == "binding" {
					return true, ca.GetObject(), nil
				}
				return false, nil, nil
			})

			prom := &fakeTelemetry{
				cpu:     map[string]float64{"a": 0.2, "b": 0.2},
				traffic: map[string]float64{"a": 0.0, "b": 10.0},
			}
			ctrl := NewController(settings, client, prom, "services", "pedestrian")
			ctrl.Now = func() time.Time { return now }
			var logs bytes.Buffer
			ctrl.Logger = log.New(&logs, "", 0)

			ctx := context.Background()
			if tc.bindFirst {
				bound, err := ctrl.ReconcileScheduler(ctx)
				if err != nil || len(bound) != 1 || bound[0].Pod != "app-5" {
					t.Fatalf("expected app-5 bound in-flight, got bound=%+v err=%v", bound, err)
				}
				if len(ctrl.inFlightBindings) != 1 {
					t.Fatalf("expected 1 in-flight binding, got %d", len(ctrl.inFlightBindings))
				}
			}

			cand, err := ctrl.ReconcileDescheduler(ctx)
			if err != nil || cand != nil {
				t.Fatalf("expected descheduler to defer (%s), got cand=%+v err=%v", tc.name, cand, err)
			}
			if len(evicted) != 0 {
				t.Fatalf("expected no eviction (%s), got %v", tc.name, evicted)
			}
			if !strings.Contains(logs.String(), "pods-not-settled") {
				t.Fatalf("expected pods-not-settled log (%s), got:\n%s", tc.name, logs.String())
			}
		})
	}
}

func TestDeschedulerNeverEvictsTerminatingPod(t *testing.T) {
	now := time.Unix(1000, 0)

	t.Run("skips-terminating-pod-with-earlier-name-on-source", func(t *testing.T) {
		settings := testSettings()
		client := fake.NewSimpleClientset(
			makeTestNode("a", "10", "10Gi"),
			makeTestNode("b", "10", "10Gi"),
			makeTestDeployment(5),
			makeTerminatingPod("app-0", "uid-0", "a", settings.SchedulerName, "1", "1Gi", now),
			makeTestPod("app-1", "uid-1", "a", settings.SchedulerName, true, "1", "1Gi"),
			makeTestPod("app-2", "uid-2", "b", settings.SchedulerName, true, "1", "1Gi"),
			makeTestPod("app-3", "uid-3", "b", settings.SchedulerName, true, "1", "1Gi"),
			makeTestPod("app-4", "uid-4", "b", settings.SchedulerName, true, "1", "1Gi"),
			makeTestPod("app-5", "uid-5", "b", settings.SchedulerName, true, "1", "1Gi"),
		)
		var evicted []string
		client.PrependReactor("create", "pods", func(action k8stesting.Action) (bool, runtime.Object, error) {
			if action.GetSubresource() == "eviction" {
				ca := action.(k8stesting.CreateAction)
				evicted = append(evicted, ca.GetObject().(metav1.Object).GetName())
				return true, ca.GetObject(), nil
			}
			return false, nil, nil
		})

		prom := &fakeTelemetry{
			cpu:     map[string]float64{"a": 0.2, "b": 0.2},
			traffic: map[string]float64{"a": 0.0, "b": 10.0},
		}
		ctrl := NewController(settings, client, prom, "services", "pedestrian")
		ctrl.Now = func() time.Time { return now }
		ctrl.Logger = log.New(&bytes.Buffer{}, "", 0)

		cand, err := ctrl.ReconcileDescheduler(context.Background())
		if err != nil || cand == nil {
			t.Fatalf("expected eviction of non-terminating pod, got cand=%+v err=%v", cand, err)
		}
		if len(evicted) != 1 || evicted[0] != "app-1" {
			t.Fatalf("expected app-1 evicted (never terminating app-0), got %v", evicted)
		}
	})

	t.Run("ignores-node-hosting-only-terminating-pod-as-source", func(t *testing.T) {
		settings := testSettings()
		settings.EligibleNodes = []string{"a", "b", "c"}
		client := fake.NewSimpleClientset(
			makeTestNode("a", "10", "10Gi"),
			makeTestNode("b", "10", "10Gi"),
			makeTestNode("c", "10", "10Gi"),
			makeTestDeployment(5),
			// Node a has only a terminating pod; node b has all 5 non-terminating Ready pods.
			makeTerminatingPod("app-0", "uid-0", "a", settings.SchedulerName, "1", "1Gi", now),
			makeTestPod("app-1", "uid-1", "b", settings.SchedulerName, true, "1", "1Gi"),
			makeTestPod("app-2", "uid-2", "b", settings.SchedulerName, true, "1", "1Gi"),
			makeTestPod("app-3", "uid-3", "b", settings.SchedulerName, true, "1", "1Gi"),
			makeTestPod("app-4", "uid-4", "b", settings.SchedulerName, true, "1", "1Gi"),
			makeTestPod("app-5", "uid-5", "b", settings.SchedulerName, true, "1", "1Gi"),
		)
		var evicted []string
		client.PrependReactor("create", "pods", func(action k8stesting.Action) (bool, runtime.Object, error) {
			if action.GetSubresource() == "eviction" {
				ca := action.(k8stesting.CreateAction)
				evicted = append(evicted, ca.GetObject().(metav1.Object).GetName())
				return true, ca.GetObject(), nil
			}
			return false, nil, nil
		})

		prom := &fakeTelemetry{
			cpu:     map[string]float64{"a": 0.2, "b": 0.2, "c": 0.2},
			traffic: map[string]float64{"a": 0.0, "b": 1.0, "c": 10.0},
		}
		ctrl := NewController(settings, client, prom, "services", "pedestrian")
		ctrl.Now = func() time.Time { return now }
		ctrl.Logger = log.New(&bytes.Buffer{}, "", 0)

		cand, err := ctrl.ReconcileDescheduler(context.Background())
		if err != nil || cand == nil {
			t.Fatalf("expected eviction from node b toward c, got cand=%+v err=%v", cand, err)
		}
		if cand.Source != "b" || cand.Destination != "c" {
			t.Fatalf("expected source=b dest=c (not terminating-only node a), got %+v", cand)
		}
		if len(evicted) != 1 || evicted[0] != "app-1" {
			t.Fatalf("expected app-1 evicted (never terminating app-0), got %v", evicted)
		}
	})
}

func TestDeschedulerPreferCloseSmokeRunOfflineReplay(t *testing.T) {
	// Replay the exported demand samples from steep_drain_smoke_01 with
	// representative node capacities and the recorded managed pod layout.
	// - 10:21:17 (+6s into Phase 3): 5 active managed pods (gnb-2:4, gnb-3:1) + 1 draining pod on gnb-3,
	//   the +5s traffic sample still favors gnb-2 (1496 vs 150 gnb-3 and 150 gnb1).
	//   Descheduler must skip evicting gnb-3's pod because PreferClose scheduler would put the replacement back on gnb-3.
	// - 10:21:47 (+36s into Phase 3): the +35s sample favors gnb-3 (503 vs 150 at each other site).
	//   Descheduler must evict a pod from gnb-2, and ReconcileScheduler must bind its replacement to gnb-3.
	now := time.Date(2026, 11, 19, 10, 21, 17, 0, time.UTC)
	settings := testSettings()
	settings.EligibleNodes = []string{"gnb1", "gnb-2", "gnb-3"}
	settings.DescheduleIntervalSeconds = 30
	settings.LocalityWeight = 0.4
	settings.PreferClose = true

	dep := makeTestDeployment(5)
	dep.Spec.Template.Spec.Containers[0].Resources.Requests = corev1.ResourceList{
		corev1.ResourceCPU:    resource.MustParse("500m"),
		corev1.ResourceMemory: resource.MustParse("256Mi"),
	}

	// System/anchor pods reflecting real cluster headroom (gnb1 has much lower headroom than gnb-2/gnb-3).
	sysGnb1 := makeTestPod("sys-gnb1", "uid-sys-1", "gnb1", "default-scheduler", true, "4500m", "9Gi")
	sysGnb1.Namespace = "kube-system"
	sysGnb1.Labels = map[string]string{"app": "system"}
	sysGnb2 := makeTestPod("sys-gnb2", "uid-sys-2", "gnb-2", "default-scheduler", true, "1", "8Gi")
	sysGnb2.Namespace = "kube-system"
	sysGnb2.Labels = map[string]string{"app": "system"}
	sysGnb3 := makeTestPod("sys-gnb3", "uid-sys-3", "gnb-3", "default-scheduler", true, "1", "8Gi")
	sysGnb3.Namespace = "kube-system"
	sysGnb3.Labels = map[string]string{"app": "system"}

	client := fake.NewSimpleClientset(
		makeTestNode("gnb1", "8", "16Gi"),
		makeTestNode("gnb-2", "20", "64Gi"),
		makeTestNode("gnb-3", "20", "64Gi"),
		dep,
		sysGnb1, sysGnb2, sysGnb3,
		// 10:20:48 eviction from gnb-3 still draining its 35s preStop hook at 10:21:17.
		makeTerminatingPod("app-nqqsq", "uid-nqqsq", "gnb-3", settings.SchedulerName, "500m", "256Mi", now.Add(-29*time.Second)),
		// 5 active Ready managed pods: 4 on gnb-2, 1 on gnb-3.
		makeTestPod("app-22jnb", "uid-22jnb", "gnb-2", settings.SchedulerName, true, "500m", "256Mi"),
		makeTestPod("app-p5whl", "uid-p5whl", "gnb-2", settings.SchedulerName, true, "500m", "256Mi"),
		makeTestPod("app-p8954", "uid-p8954", "gnb-2", settings.SchedulerName, true, "500m", "256Mi"),
		makeTestPod("app-p8frr", "uid-p8frr", "gnb-2", settings.SchedulerName, true, "500m", "256Mi"),
		makeTestPod("app-tzljf", "uid-tzljf", "gnb-3", settings.SchedulerName, true, "500m", "256Mi"),
	)

	var evicted []string
	client.PrependReactor("create", "pods", func(action k8stesting.Action) (bool, runtime.Object, error) {
		ca := action.(k8stesting.CreateAction)
		if action.GetSubresource() == "eviction" {
			evicted = append(evicted, ca.GetObject().(metav1.Object).GetName())
			return true, ca.GetObject(), nil
		}
		if action.GetSubresource() == "binding" {
			return true, ca.GetObject(), nil
		}
		return false, nil, nil
	})

	prom := &fakeTelemetry{
		cpu:     map[string]float64{"gnb1": 0.2, "gnb-2": 0.2, "gnb-3": 0.2},
		traffic: map[string]float64{"gnb1": 150, "gnb-2": 1496, "gnb-3": 150},
	}

	ctrl := NewController(settings, client, prom, "services", "pedestrian")
	ctrl.Now = func() time.Time { return now }
	var logs bytes.Buffer
	ctrl.Logger = log.New(&logs, "", 0)

	ctx := context.Background()

	// 1. At 10:21:17, the bad bounce-back eviction of gnb-3's only managed pod must be skipped.
	cand, err := ctrl.ReconcileDescheduler(ctx)
	if err != nil {
		t.Fatalf("ReconcileDescheduler at 10:21:17 failed: %v", err)
	}
	if cand != nil {
		t.Fatalf("expected bad gnb-3 eviction skipped at 10:21:17, got cand=%+v", cand)
	}
	if len(evicted) != 0 {
		t.Fatalf("expected zero evictions at 10:21:17, got %v", evicted)
	}
	if !strings.Contains(logs.String(), "below-min-score-improvement") {
		t.Fatalf("expected below-min-score-improvement log at 10:21:17, got:\n%s", logs.String())
	}

	// 2. At 10:21:47, the old draining pod has exited and the traffic signal favors gnb-3.
	if err := client.CoreV1().Pods("services").Delete(ctx, "app-nqqsq", metav1.DeleteOptions{}); err != nil {
		t.Fatal(err)
	}
	now = now.Add(30 * time.Second)
	prom.traffic = map[string]float64{"gnb1": 150, "gnb-2": 150, "gnb-3": 503}

	cand, err = ctrl.ReconcileDescheduler(ctx)
	if err != nil || cand == nil {
		t.Fatalf("expected eviction from gnb-2 to gnb-3 at 10:21:47, got cand=%+v err=%v", cand, err)
	}
	if cand.Source != "gnb-2" || cand.Destination != "gnb-3" {
		t.Fatalf("expected source=gnb-2 dest=gnb-3 at 10:21:47, got %+v", cand)
	}
	if len(evicted) != 1 || evicted[0] != "app-22jnb" {
		t.Fatalf("expected app-22jnb evicted from gnb-2, got %v", evicted)
	}

	// 3. Simulate Kubernetes eviction (old pod begins draining on gnb-2) and ReplicaSet creating replacement.
	evictedPod, err := client.CoreV1().Pods("services").Get(ctx, "app-22jnb", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	ts := metav1.NewTime(now)
	evictedPod.DeletionTimestamp = &ts
	if _, err := client.CoreV1().Pods("services").Update(ctx, evictedPod, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	replacement := makeTestPod("app-chckk", "uid-chckk", "", settings.SchedulerName, false, "500m", "256Mi")
	if _, err := client.CoreV1().Pods("services").Create(ctx, replacement, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}

	// Scheduler must bind the replacement onto gnb-3 (matching the descheduler's prediction, not bouncing back).
	bound, err := ctrl.ReconcileScheduler(ctx)
	if err != nil || len(bound) != 1 || bound[0] != (BoundPodResult{Pod: "app-chckk", Node: "gnb-3"}) {
		t.Fatalf("expected replacement bound to gnb-3, got bound=%+v err=%v", bound, err)
	}
}

func TestDeschedulerSteepScoreSmoke5sPlacementWindowReplay(t *testing.T) {
	// Replay of steep_score_smoke_01 around Phase 3 start (10:56:33.124 UTC):
	// Active managed pods at +4.726s (10:56:37.850 UTC): gnb-2:3, gnb-3:2, plus 1 draining pod on gnb-3.
	// With the 5s placement window scraped every 1s (last scrape at +4.266s on the original Prometheus phase):
	//   counts = {gnb1: 50, gnb-2: 121, gnb-3: 86} -> rates = {gnb1: 10.0, gnb-2: 24.2, gnb-3: 17.2} req/s.
	// The controller must NOT evict gnb-3 -> gnb-2; and once the next 1s scrape (+5.266s) sees gnb-3 ahead
	// ({gnb1: 50, gnb-2: 50, gnb-3: 112} -> rates 10.0 / 10.0 / 22.4), an eligible check moves gnb-2 -> gnb-3.
	now := time.Date(2026, 11, 19, 10, 56, 37, 850000000, time.UTC)
	settings := testSettings()
	settings.EligibleNodes = []string{"gnb1", "gnb-2", "gnb-3"}
	settings.DescheduleIntervalSeconds = 30
	settings.LocalityWeight = 0.4
	settings.PreferClose = true

	dep := makeTestDeployment(5)
	dep.Spec.Template.Spec.Containers[0].Resources.Requests = corev1.ResourceList{
		corev1.ResourceCPU:    resource.MustParse("500m"),
		corev1.ResourceMemory: resource.MustParse("256Mi"),
	}

	sysGnb1 := makeTestPod("sys-gnb1", "uid-sys-1", "gnb1", "default-scheduler", true, "4500m", "9Gi")
	sysGnb1.Namespace = "kube-system"
	sysGnb1.Labels = map[string]string{"app": "system"}
	sysGnb2 := makeTestPod("sys-gnb2", "uid-sys-2", "gnb-2", "default-scheduler", true, "1", "8Gi")
	sysGnb2.Namespace = "kube-system"
	sysGnb2.Labels = map[string]string{"app": "system"}
	sysGnb3 := makeTestPod("sys-gnb3", "uid-sys-3", "gnb-3", "default-scheduler", true, "1", "8Gi")
	sysGnb3.Namespace = "kube-system"
	sysGnb3.Labels = map[string]string{"app": "system"}

	client := fake.NewSimpleClientset(
		makeTestNode("gnb1", "8", "16Gi"),
		makeTestNode("gnb-2", "20", "64Gi"),
		makeTestNode("gnb-3", "20", "64Gi"),
		dep,
		sysGnb1, sysGnb2, sysGnb3,
		// 10:56:07 eviction from gnb-3 still draining at 10:56:37.850.
		makeTerminatingPod("app-dx2cz", "uid-dx2cz", "gnb-3", settings.SchedulerName, "500m", "256Mi", now.Add(-30*time.Second)),
		// 5 active Ready managed pods: gnb-2:3, gnb-3:2.
		makeTestPod("app-2d45k", "uid-2d45k", "gnb-2", settings.SchedulerName, true, "500m", "256Mi"),
		makeTestPod("app-g5llp", "uid-g5llp", "gnb-2", settings.SchedulerName, true, "500m", "256Mi"),
		makeTestPod("app-l99gg", "uid-l99gg", "gnb-2", settings.SchedulerName, true, "500m", "256Mi"),
		makeTestPod("app-qwd5d", "uid-qwd5d", "gnb-3", settings.SchedulerName, true, "500m", "256Mi"),
		makeTestPod("app-z4pw9", "uid-z4pw9", "gnb-3", settings.SchedulerName, true, "500m", "256Mi"),
	)

	var evicted []string
	client.PrependReactor("create", "pods", func(action k8stesting.Action) (bool, runtime.Object, error) {
		if action.GetSubresource() == "eviction" {
			ca := action.(k8stesting.CreateAction)
			evicted = append(evicted, ca.GetObject().(metav1.Object).GetName())
			return true, ca.GetObject(), nil
		}
		return false, nil, nil
	})

	prom := &fakeTelemetry{
		cpu:     map[string]float64{"gnb1": 0.2, "gnb-2": 0.2, "gnb-3": 0.2},
		traffic: map[string]float64{"gnb1": 50.0 / 5.0, "gnb-2": 121.0 / 5.0, "gnb-3": 86.0 / 5.0},
	}
	ctrl := NewController(settings, client, prom, "services", "pedestrian")
	ctrl.Now = func() time.Time { return now }
	ctrl.Logger = log.New(&bytes.Buffer{}, "", 0)

	ctx := context.Background()
	cand, err := ctrl.ReconcileDescheduler(ctx)
	if err != nil {
		t.Fatalf("ReconcileDescheduler at +4.726s failed: %v", err)
	}
	if cand != nil || len(evicted) != 0 {
		t.Fatalf("expected no gnb-3 -> gnb-2 eviction at +4.726s under 5s/1s metric, got cand=%+v evicted=%v", cand, evicted)
	}

	// Next eligible check once +5.266s scrape sees gnb-3 ahead (50 / 50 / 112 in 5s window).
	now = now.Add(30 * time.Second)
	prom.traffic = map[string]float64{"gnb1": 50.0 / 5.0, "gnb-2": 50.0 / 5.0, "gnb-3": 112.0 / 5.0}
	cand, err = ctrl.ReconcileDescheduler(ctx)
	if err != nil || cand == nil {
		t.Fatalf("expected eviction toward gnb-3 once 5s metric sees gnb-3 ahead, got cand=%+v err=%v", cand, err)
	}
	if cand.Source != "gnb-2" || cand.Destination != "gnb-3" {
		t.Fatalf("expected source=gnb-2 dest=gnb-3, got %+v", cand)
	}
}
