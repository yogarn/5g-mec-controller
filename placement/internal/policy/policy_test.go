package policy

import (
	"math"
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/yogarn/5g-mec-controller/placement/internal/config"
)

func TestRankAndScoreCandidateNodeParity(t *testing.T) {
	weights := map[string]float64{"cpu": 0.5, "memory": 0.5}
	nodes := []Node{
		{
			Name:           "small",
			Capacity:       map[string]float64{"cpu": 4, "memory": 8},
			Allocated:      map[string]float64{"cpu": 1, "memory": 3},
			CPUUtilization: 0.2,
			ArrivalRate:    3,
			Ready:          true,
		},
		{
			Name:           "large",
			Capacity:       map[string]float64{"cpu": 8, "memory": 16},
			Allocated:      map[string]float64{"cpu": 3, "memory": 7},
			CPUUtilization: 0.2,
			ArrivalRate:    1,
			Ready:          true,
		},
		{
			Name:           "saturated",
			Capacity:       map[string]float64{"cpu": 4, "memory": 8},
			Allocated:      map[string]float64{"cpu": 0, "memory": 0},
			CPUUtilization: 0.9,
			ArrivalRate:    10,
			Ready:          true,
		},
	}
	req := map[string]float64{"cpu": 1, "memory": 1}
	scores, err := RankNodes(nodes, req, 0.8, 0.4, weights)
	if err != nil {
		t.Fatalf("RankNodes failed: %v", err)
	}
	if _, ok := scores["saturated"]; ok {
		t.Fatalf("expected saturated node to be filtered out")
	}
	wantSmall := 0.6*(0.5*0.75+0.5*0.625) + 0.4*(3.0/14.0)
	if math.Abs(scores["small"]-wantSmall) > 1e-9 {
		t.Fatalf("small score = %.9f, want %.9f", scores["small"], wantSmall)
	}

	// Verify 2D weights produce identical scores to legacy 4D weights with gpu=vram=0.
	legacyWeights := map[string]float64{"cpu": 0.5, "memory": 0.5, "gpu": 0.0, "vram": 0.0}
	legacyScores, err := RankNodes(nodes, req, 0.8, 0.4, legacyWeights)
	if err != nil {
		t.Fatalf("RankNodes with legacy 4D weights failed: %v", err)
	}
	for name, score := range scores {
		if legacyScores[name] != score {
			t.Fatalf("score mismatch for %s: 2D=%.12f legacy4D=%.12f", name, score, legacyScores[name])
		}
	}

	best, candidates, err := ScoreCandidateNode(nodes, req, 0.8, 0.4, weights, "", nil)
	if err != nil {
		t.Fatalf("ScoreCandidateNode failed: %v", err)
	}
	if best != "small" {
		t.Fatalf("best = %q, want small", best)
	}
	if _, ok := candidates["saturated"]; ok {
		t.Fatalf("saturated node must not be a candidate")
	}
}

func TestSelectDescheduleCandidatePicksHighestPositiveDelta(t *testing.T) {
	weights := config.DefaultResourceWeights()
	nodes := []Node{
		{
			Name:           "a",
			Capacity:       map[string]float64{"cpu": 10, "memory": 10},
			Allocated:      map[string]float64{"cpu": 5, "memory": 5},
			CPUUtilization: 0.1,
			ArrivalRate:    3,
			Ready:          true,
		},
		{
			Name:           "b",
			Capacity:       map[string]float64{"cpu": 10, "memory": 10},
			Allocated:      map[string]float64{"cpu": 8, "memory": 8},
			CPUUtilization: 0.1,
			ArrivalRate:    1,
			Ready:          true,
		},
		{
			Name:           "c",
			Capacity:       map[string]float64{"cpu": 10, "memory": 10},
			Allocated:      map[string]float64{},
			CPUUtilization: 0.1,
			ArrivalRate:    6,
			Ready:          true,
		},
	}
	counts := map[string]int{"a": 1, "b": 2, "c": 0}
	req := map[string]float64{"cpu": 1, "memory": 1}
	cand, err := SelectDescheduleCandidate(nodes, req, counts, 0.8, 0.4, weights, nil)
	if err != nil {
		t.Fatalf("SelectDescheduleCandidate failed: %v", err)
	}
	if cand == nil || cand.Source != "b" || cand.Destination != "c" || cand.Delta <= 0 {
		t.Fatalf("unexpected deschedule candidate: %+v", cand)
	}
}

func TestPodRequestsAndSelectorMatches(t *testing.T) {
	spec := corev1.PodSpec{
		Containers: []corev1.Container{
			{Resources: corev1.ResourceRequirements{Requests: corev1.ResourceList{
				corev1.ResourceCPU:    resource.MustParse("500m"),
				corev1.ResourceMemory: resource.MustParse("1Gi"),
			}}},
		},
		InitContainers: []corev1.Container{
			{Resources: corev1.ResourceRequirements{Requests: corev1.ResourceList{
				corev1.ResourceCPU: resource.MustParse("2"),
			}}},
		},
		Overhead: corev1.ResourceList{
			corev1.ResourceCPU: resource.MustParse("100m"),
		},
	}
	reqs, err := PodRequests(spec)
	if err != nil {
		t.Fatalf("PodRequests failed: %v", err)
	}
	if math.Abs(reqs["cpu"]-2.1) > 1e-6 {
		t.Fatalf("cpu = %v, want 2.1", reqs["cpu"])
	}

	sel := &metav1.LabelSelector{
		MatchLabels: map[string]string{"app": "pedestrian"},
		MatchExpressions: []metav1.LabelSelectorRequirement{
			{Key: "tier", Operator: metav1.LabelSelectorOpIn, Values: []string{"edge", "core"}},
		},
	}
	ok, err := SelectorMatches(sel, map[string]string{"app": "pedestrian", "tier": "edge"})
	if err != nil || !ok {
		t.Fatalf("expected selector match, ok=%v err=%v", ok, err)
	}
	ok, err = SelectorMatches(sel, map[string]string{"app": "other", "tier": "edge"})
	if err != nil || ok {
		t.Fatalf("expected selector mismatch, ok=%v err=%v", ok, err)
	}
}

func TestSelectDescheduleCandidateRejectsBounceBackToSource(t *testing.T) {
	weights := config.DefaultResourceWeights()
	// Source a would still be chosen on the post-removal snapshot because freeing a's pod keeps a ahead of b;
	// SelectDescheduleCandidate must evaluate postRemoval with exclude="" and return nil rather than evicting.
	nodes := []Node{
		{
			Name:           "a",
			Capacity:       map[string]float64{"cpu": 10, "memory": 10},
			Allocated:      map[string]float64{"cpu": 3, "memory": 3},
			CPUUtilization: 0.1,
			ArrivalRate:    8.0,
			Ready:          true,
		},
		{
			Name:           "b",
			Capacity:       map[string]float64{"cpu": 10, "memory": 10},
			Allocated:      map[string]float64{"cpu": 5, "memory": 5},
			CPUUtilization: 0.1,
			ArrivalRate:    2.0,
			Ready:          true,
		},
	}
	req := map[string]float64{"cpu": 1, "memory": 1}
	post := SubtractNodeAllocation(nodes, "a", req)
	if post[0].Allocated["cpu"] != 2 {
		t.Fatalf("expected post-removal cpu allocation 2 on a, got %v", post[0].Allocated["cpu"])
	}
	dest, _, err := ScoreCandidateNode(post, req, 0.8, 0.4, weights, "", nil)
	if err != nil || dest != "a" {
		t.Fatalf("expected post-removal scheduling to pick a, got dest=%q err=%v", dest, err)
	}

	counts := map[string]int{"a": 1, "b": 0}
	cand, err := SelectDescheduleCandidate(nodes, req, counts, 0.8, 0.4, weights, nil)
	if err != nil {
		t.Fatalf("SelectDescheduleCandidate failed: %v", err)
	}
	if cand != nil {
		t.Fatalf("expected nil candidate when post-removal scheduling picks source a, got %+v", cand)
	}
}

func TestPodFitsNodeConstraints(t *testing.T) {
	nodeA := &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name:   "edge-1",
			Labels: map[string]string{"zone": "mec-a", "kubernetes.io/hostname": "edge-1"},
		},
		Spec: corev1.NodeSpec{
			Taints: []corev1.Taint{
				{Key: "dedicated", Value: "gpu", Effect: corev1.TaintEffectNoSchedule},
			},
		},
	}
	nodeB := &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name:   "core-1",
			Labels: map[string]string{"zone": "core", "kubernetes.io/hostname": "core-1"},
		},
	}

	spec := corev1.PodSpec{
		NodeSelector: map[string]string{"zone": "mec-a"},
		Affinity: &corev1.Affinity{
			NodeAffinity: &corev1.NodeAffinity{
				RequiredDuringSchedulingIgnoredDuringExecution: &corev1.NodeSelector{
					NodeSelectorTerms: []corev1.NodeSelectorTerm{
						{
							MatchFields: []corev1.NodeSelectorRequirement{
								{Key: "metadata.name", Operator: corev1.NodeSelectorOpIn, Values: []string{"edge-1", "core-1"}},
							},
						},
					},
				},
			},
		},
		Tolerations: []corev1.Toleration{
			{Key: "dedicated", Operator: corev1.TolerationOpEqual, Value: "gpu", Effect: corev1.TaintEffectNoSchedule},
		},
	}

	if ok, err := PodFitsNodeConstraints(spec, nodeA); err != nil || !ok {
		t.Fatalf("expected nodeA to fit, ok=%v err=%v", ok, err)
	}
	if ok, err := PodFitsNodeConstraints(spec, nodeB); err != nil || ok {
		t.Fatalf("expected nodeB to be rejected by nodeSelector, ok=%v err=%v", ok, err)
	}

	// Missing toleration must reject nodeA.
	noTolSpec := spec
	noTolSpec.Tolerations = nil
	if ok, err := PodFitsNodeConstraints(noTolSpec, nodeA); err != nil || ok {
		t.Fatalf("expected untolerated taint to reject nodeA, ok=%v err=%v", ok, err)
	}

	// Unsupported podAffinity must fail closed with error.
	badSpec := corev1.PodSpec{
		Affinity: &corev1.Affinity{
			PodAntiAffinity: &corev1.PodAntiAffinity{
				RequiredDuringSchedulingIgnoredDuringExecution: []corev1.PodAffinityTerm{{TopologyKey: "kubernetes.io/hostname"}},
			},
		},
	}
	if _, err := PodFitsNodeConstraints(badSpec, nodeA); err == nil {
		t.Fatalf("expected error for unsupported podAntiAffinity")
	}

	// Empty nodeSelectorTerm matches no nodes (matching Kubernetes semantics).
	emptyTermSpec := corev1.PodSpec{
		Affinity: &corev1.Affinity{
			NodeAffinity: &corev1.NodeAffinity{
				RequiredDuringSchedulingIgnoredDuringExecution: &corev1.NodeSelector{
					NodeSelectorTerms: []corev1.NodeSelectorTerm{{}},
				},
			},
		},
	}
	if ok, err := PodFitsNodeConstraints(emptyTermSpec, nodeA); err != nil || ok {
		t.Fatalf("expected empty nodeSelectorTerm to match no nodes, ok=%v err=%v", ok, err)
	}

	// Empty term alongside a matching term must skip the empty term and honor the valid term.
	mixedTermSpec := corev1.PodSpec{
		Tolerations: spec.Tolerations,
		Affinity: &corev1.Affinity{
			NodeAffinity: &corev1.NodeAffinity{
				RequiredDuringSchedulingIgnoredDuringExecution: &corev1.NodeSelector{
					NodeSelectorTerms: []corev1.NodeSelectorTerm{
						{},
						{
							MatchFields: []corev1.NodeSelectorRequirement{
								{Key: "metadata.name", Operator: corev1.NodeSelectorOpIn, Values: []string{"edge-1"}},
							},
						},
					},
				},
			},
		},
	}
	if ok, err := PodFitsNodeConstraints(mixedTermSpec, nodeA); err != nil || !ok {
		t.Fatalf("expected mixed nodeSelectorTerms to match edge-1, ok=%v err=%v", ok, err)
	}
	if ok, err := PodFitsNodeConstraints(mixedTermSpec, nodeB); err != nil || ok {
		t.Fatalf("expected mixed nodeSelectorTerms to reject core-1, ok=%v err=%v", ok, err)
	}
}

func TestPreferCloseDemandMinusPodShare(t *testing.T) {
	nodes := []Node{
		{Name: "a", Capacity: map[string]float64{"cpu": 10, "memory": 10}, Allocated: map[string]float64{}, CPUUtilization: .1, ArrivalRate: 9, Ready: true},
		{Name: "b", Capacity: map[string]float64{"cpu": 10, "memory": 10}, Allocated: map[string]float64{}, CPUUtilization: .1, ArrivalRate: 1, Ready: true},
	}
	weights := map[string]float64{"cpu": 1, "memory": 0, "gpu": 0, "vram": 0}
	requests := map[string]float64{"cpu": 1, "memory": 1}
	raw, err := RankNodes(nodes, requests, .8, .4, weights)
	if err != nil {
		t.Fatal(err)
	}
	adjusted, err := RankNodes(nodes, requests, .8, .4, weights, map[string]float64{"a": 1})
	if err != nil {
		t.Fatal(err)
	}
	if math.Abs((raw["a"]-adjusted["a"])-.4) > 1e-9 || raw["b"] != adjusted["b"] {
		t.Fatalf("pod share adjustment incorrect: raw=%v adjusted=%v", raw, adjusted)
	}
}

func TestSelectDescheduleCandidateRequiresMinimumImprovement(t *testing.T) {
	capacity := map[string]float64{"cpu": 10, "memory": 10}
	nodes := []Node{
		{Name: "source", Capacity: capacity, Allocated: map[string]float64{"cpu": 2.3, "memory": 2.3}, Ready: true},
		{Name: "destination", Capacity: capacity, Allocated: map[string]float64{"cpu": 2, "memory": 2}, Ready: true},
	}
	requests := map[string]float64{"cpu": 0.1, "memory": 0.1}
	for _, tc := range []struct {
		name       string
		count      int
		allocation float64
		wantMove   bool
	}{
		{name: "weak gain with one pod", count: 1, allocation: 2.3},
		{name: "weak gain with two pods", count: 2, allocation: 2.3},
		{name: "strong gain with one pod", count: 1, allocation: 3, wantMove: true},
		{name: "strong gain with two pods", count: 2, allocation: 3, wantMove: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			nodes[0].Allocated["cpu"] = tc.allocation
			nodes[0].Allocated["memory"] = tc.allocation
			candidate, err := SelectDescheduleCandidate(nodes, requests, map[string]int{"source": tc.count},
				0.8, 0, config.DefaultResourceWeights(), nil, true)
			if err != nil {
				t.Fatal(err)
			}
			if (candidate != nil) != tc.wantMove {
				t.Fatalf("want move %v, got %+v", tc.wantMove, candidate)
			}
		})
	}
}

func TestSelectDescheduleCandidatePreferCloseSkipsBounceBack(t *testing.T) {
	weights := config.DefaultResourceWeights()
	// Mirrors the smoke-run state at 10:21:17: gnb-2 has 4 pods and 70% stale traffic,
	// gnb-3 has 1 pod and 22% rising traffic. Without PreferClose pod shares, gnb-2
	// looks better than gnb-3, but after removing gnb-3's pod the scheduler sees
	// podShares={gnb-2: 0.8, gnb-3: 0.0} and puts the replacement right back on gnb-3.
	nodes := []Node{
		{Name: "gnb1", Capacity: map[string]float64{"cpu": 8, "memory": 16}, Allocated: map[string]float64{"cpu": 4, "memory": 8}, CPUUtilization: 0.2, ArrivalRate: 8, Ready: true},
		{Name: "gnb-2", Capacity: map[string]float64{"cpu": 20, "memory": 64}, Allocated: map[string]float64{"cpu": 3, "memory": 8}, CPUUtilization: 0.2, ArrivalRate: 70, Ready: true},
		{Name: "gnb-3", Capacity: map[string]float64{"cpu": 20, "memory": 64}, Allocated: map[string]float64{"cpu": 2, "memory": 8}, CPUUtilization: 0.2, ArrivalRate: 22, Ready: true},
	}
	counts := map[string]int{"gnb1": 0, "gnb-2": 4, "gnb-3": 1}
	req := map[string]float64{"cpu": 0.5, "memory": 0.25}

	cand, err := SelectDescheduleCandidate(nodes, req, counts, 0.8, 0.4, weights, nil, true)
	if err != nil {
		t.Fatalf("SelectDescheduleCandidate failed: %v", err)
	}
	if cand != nil {
		t.Fatalf("expected nil candidate when PreferClose scheduler would re-place on source, got %+v", cand)
	}

	// Once traffic catches up to gnb-3 (83% vs 8%), PreferClose descheduler moves a pod from gnb-2 to gnb-3.
	nodes[0].ArrivalRate = 2
	nodes[1].ArrivalRate = 2
	nodes[2].ArrivalRate = 20
	cand, err = SelectDescheduleCandidate(nodes, req, counts, 0.8, 0.4, weights, nil, true)
	if err != nil {
		t.Fatalf("SelectDescheduleCandidate failed after shift: %v", err)
	}
	if cand == nil || cand.Source != "gnb-2" || cand.Destination != "gnb-3" || cand.Delta <= 0 {
		t.Fatalf("expected gnb-2 -> gnb-3 move once traffic catches up, got %+v", cand)
	}
}
