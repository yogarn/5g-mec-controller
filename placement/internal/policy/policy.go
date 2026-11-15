package policy

import (
	"fmt"
	"math"
	"sort"
	"strconv"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/yogarn/5g-mec-controller/placement/internal/config"
)

// Node holds snapshot capacity, allocated resources, and telemetry for placement scoring.
type Node struct {
	Name           string
	Capacity       map[string]float64
	Allocated      map[string]float64
	CPUUtilization float64
	ArrivalRate    float64
	Ready          bool
	GPUFree        float64
	VRAMFree       float64
}

func cloneNodeWithDelta(node Node, requests map[string]float64) Node {
	alloc := make(map[string]float64, len(node.Allocated)+len(requests))
	for k, v := range node.Allocated {
		alloc[k] = v
	}
	for k, req := range requests {
		v := alloc[k] + req
		if v < 0 {
			v = 0
		}
		alloc[k] = v
	}
	node.Allocated = alloc
	return node
}

// AddNodeAllocation returns a new slice with requests added to nodeName's allocated resources.
func AddNodeAllocation(nodes []Node, nodeName string, requests map[string]float64) []Node {
	out := make([]Node, len(nodes))
	for i, n := range nodes {
		if n.Name == nodeName {
			out[i] = cloneNodeWithDelta(n, requests)
		} else {
			out[i] = n
		}
	}
	return out
}

// SubtractNodeAllocation returns a new slice with requests subtracted from nodeName's allocated resources.
func SubtractNodeAllocation(nodes []Node, nodeName string, requests map[string]float64) []Node {
	neg := make(map[string]float64, len(requests))
	for k, v := range requests {
		neg[k] = -v
	}
	return AddNodeAllocation(nodes, nodeName, neg)
}

func finiteNonNeg(v float64) bool {
	return !math.IsNaN(v) && !math.IsInf(v, 0) && v >= 0
}

// RankNodes implements the exact same resource headroom + traffic locality score as controller/policy.py.
func RankNodes(nodes []Node, requests map[string]float64, saturation, localityWeight float64, weights map[string]float64) (map[string]float64, error) {
	if err := config.ValidateResourceWeights(weights); err != nil {
		return nil, err
	}
	if len(nodes) == 0 || len(requests) == 0 {
		return nil, fmt.Errorf("missing node or request data")
	}
	for k, v := range requests {
		if !finiteNonNeg(v) {
			return nil, fmt.Errorf("invalid request for %s", k)
		}
	}
	if math.IsNaN(saturation) || math.IsInf(saturation, 0) || saturation <= 0 || saturation > 1 {
		return nil, fmt.Errorf("invalid cpu saturation")
	}
	if math.IsNaN(localityWeight) || math.IsInf(localityWeight, 0) || localityWeight < 0 || localityWeight > 1 {
		return nil, fmt.Errorf("invalid locality weight")
	}

	total := 0.0
	for _, n := range nodes {
		if math.IsNaN(n.CPUUtilization) || math.IsInf(n.CPUUtilization, 0) || n.CPUUtilization < 0 || n.CPUUtilization > 1 {
			return nil, fmt.Errorf("invalid CPU utilization for node %s", n.Name)
		}
		if !finiteNonNeg(n.ArrivalRate) {
			return nil, fmt.Errorf("invalid arrival rate for node %s", n.Name)
		}
		if math.IsNaN(n.GPUFree) || n.GPUFree < 0 || n.GPUFree > 1 || math.IsNaN(n.VRAMFree) || n.VRAMFree < 0 || n.VRAMFree > 1 {
			return nil, fmt.Errorf("invalid GPU/VRAM telemetry for node %s", n.Name)
		}
		for _, m := range []map[string]float64{n.Capacity, n.Allocated} {
			for k, v := range m {
				if !finiteNonNeg(v) {
					return nil, fmt.Errorf("invalid resource quantity %s on node %s", k, n.Name)
				}
			}
		}
		total += n.ArrivalRate
	}
	if math.IsNaN(total) || math.IsInf(total, 0) {
		return nil, fmt.Errorf("invalid total arrival rate")
	}

	scores := make(map[string]float64, len(nodes))
	for _, n := range nodes {
		if !n.Ready || n.CPUUtilization >= saturation {
			continue
		}
		headroom := map[string]float64{"cpu": 0, "memory": 0}
		for _, key := range []string{"cpu", "memory"} {
			capVal := n.Capacity[key]
			if capVal > 0 {
				h := (capVal - n.Allocated[key]) / capVal
				if h < 0 {
					h = 0
				}
				headroom[key] = h
			}
		}
		resourceScore := weights["cpu"]*headroom["cpu"] +
			weights["memory"]*headroom["memory"] +
			weights["gpu"]*n.GPUFree +
			weights["vram"]*n.VRAMFree
		locality := 0.0
		if total > 0 {
			locality = n.ArrivalRate / total
		}
		scores[n.Name] = (1-localityWeight)*resourceScore + localityWeight*locality
	}
	return scores, nil
}

// ScoreCandidateNode filters feasible nodes (optionally restricted to allowed) and returns the highest-scoring placement node.
func ScoreCandidateNode(nodes []Node, requests map[string]float64, saturation, localityWeight float64, weights map[string]float64, exclude string, allowed map[string]bool) (string, map[string]float64, error) {
	if _, err := RankNodes(nodes, requests, saturation, localityWeight, weights); err != nil {
		return "", nil, err
	}
	candidates := make(map[string]float64)
	for i, n := range nodes {
		if n.Name == exclude || (allowed != nil && !allowed[n.Name]) || !n.Ready || n.CPUUtilization >= saturation {
			continue
		}
		fits := true
		for key, req := range requests {
			if n.Capacity[key]-n.Allocated[key] < req {
				fits = false
				break
			}
		}
		if !fits {
			continue
		}
		projected := make([]Node, len(nodes))
		copy(projected, nodes)
		projected[i] = cloneNodeWithDelta(n, requests)
		ranked, err := RankNodes(projected, requests, saturation, localityWeight, weights)
		if err != nil {
			return "", nil, err
		}
		candidates[n.Name] = ranked[n.Name]
	}
	if len(candidates) == 0 {
		return "", candidates, nil
	}
	best := ""
	bestScore := math.Inf(-1)
	for name, score := range candidates {
		if best == "" || score > bestScore || (score == bestScore && name < best) {
			best = name
			bestScore = score
		}
	}
	return best, candidates, nil
}

// DescheduleCandidate identifies the bound source node with maximum positive improvement delta.
type DescheduleCandidate struct {
	Source           string
	Destination      string
	Delta            float64
	SourceScore      float64
	DestinationScore float64
}

// SelectDescheduleCandidate evaluates post-removal scheduling before selecting an eviction candidate,
// guaranteeing that the freed source node would not immediately win re-scheduling.
func SelectDescheduleCandidate(nodes []Node, requests map[string]float64, counts map[string]int, saturation, localityWeight float64, weights map[string]float64, allowed map[string]bool) (*DescheduleCandidate, error) {
	ranked, err := RankNodes(nodes, requests, saturation, localityWeight, weights)
	if err != nil {
		return nil, err
	}
	var occupied []string
	for name, count := range counts {
		if count > 0 {
			occupied = append(occupied, name)
		}
	}
	sort.Strings(occupied)

	var best *DescheduleCandidate
	for _, source := range occupied {
		sourceScore, ok := ranked[source]
		if !ok {
			sourceScore = math.Inf(-1)
		}
		postRemoval := SubtractNodeAllocation(nodes, source, requests)
		dest, postScores, err := ScoreCandidateNode(postRemoval, requests, saturation, localityWeight, weights, "", allowed)
		if err != nil {
			return nil, err
		}
		if dest == "" || dest == source {
			continue
		}
		delta := postScores[dest] - sourceScore
		if delta <= 0 {
			continue
		}
		if best == nil || delta > best.Delta ||
			(delta == best.Delta && (source < best.Source || (source == best.Source && dest < best.Destination))) {
			best = &DescheduleCandidate{
				Source:           source,
				Destination:      dest,
				Delta:            delta,
				SourceScore:      sourceScore,
				DestinationScore: postScores[dest],
			}
		}
	}
	return best, nil
}

func matchNodeSelectorRequirement(req corev1.NodeSelectorRequirement, val string, exists bool) (bool, error) {
	inValues := false
	for _, v := range req.Values {
		if v == val {
			inValues = true
			break
		}
	}
	switch req.Operator {
	case corev1.NodeSelectorOpIn:
		return exists && inValues, nil
	case corev1.NodeSelectorOpNotIn:
		return !exists || !inValues, nil
	case corev1.NodeSelectorOpExists:
		return exists, nil
	case corev1.NodeSelectorOpDoesNotExist:
		return !exists, nil
	case corev1.NodeSelectorOpGt, corev1.NodeSelectorOpLt:
		if !exists || len(req.Values) != 1 {
			return false, nil
		}
		lhs, err1 := strconv.ParseInt(val, 10, 64)
		rhs, err2 := strconv.ParseInt(req.Values[0], 10, 64)
		if err1 != nil || err2 != nil {
			return false, nil
		}
		if req.Operator == corev1.NodeSelectorOpGt {
			return lhs > rhs, nil
		}
		return lhs < rhs, nil
	default:
		return false, fmt.Errorf("unknown nodeSelector operator %q", req.Operator)
	}
}

func toleratesTaint(tolerations []corev1.Toleration, taint corev1.Taint) bool {
	for _, tol := range tolerations {
		if tol.Effect != "" && tol.Effect != taint.Effect {
			continue
		}
		if tol.Key == "" && tol.Operator == corev1.TolerationOpExists {
			return true
		}
		if tol.Key != taint.Key {
			continue
		}
		if tol.Operator == corev1.TolerationOpExists {
			return true
		}
		if (tol.Operator == "" || tol.Operator == corev1.TolerationOpEqual) && tol.Value == taint.Value {
			return true
		}
	}
	return false
}

// PodFitsNodeConstraints verifies nodeSelector, required nodeAffinity, and taints/tolerations,
// failing closed on unsupported hard constraints such as podAffinity, podAntiAffinity, or topologySpreadConstraints.
func PodFitsNodeConstraints(spec corev1.PodSpec, node *corev1.Node) (bool, error) {
	if node == nil {
		return false, nil
	}
	for _, c := range spec.TopologySpreadConstraints {
		if c.WhenUnsatisfiable == corev1.DoNotSchedule {
			return false, fmt.Errorf("hard topologySpreadConstraints are not supported by mec-placement-scheduler")
		}
	}
	for k, v := range spec.NodeSelector {
		if node.Labels[k] != v {
			return false, nil
		}
	}
	if spec.Affinity != nil {
		if spec.Affinity.PodAffinity != nil || spec.Affinity.PodAntiAffinity != nil {
			return false, fmt.Errorf("podAffinity and podAntiAffinity are not supported by mec-placement-scheduler")
		}
		if na := spec.Affinity.NodeAffinity; na != nil && na.RequiredDuringSchedulingIgnoredDuringExecution != nil {
			terms := na.RequiredDuringSchedulingIgnoredDuringExecution.NodeSelectorTerms
			if len(terms) == 0 {
				return false, nil
			}
			termMatched := false
			for _, term := range terms {
				// Kubernetes defines an empty NodeSelectorTerm as matching no nodes.
				if len(term.MatchExpressions) == 0 && len(term.MatchFields) == 0 {
					continue
				}
				allOK := true
				for _, expr := range term.MatchExpressions {
					val, exists := node.Labels[expr.Key]
					ok, err := matchNodeSelectorRequirement(expr, val, exists)
					if err != nil {
						return false, err
					}
					if !ok {
						allOK = false
						break
					}
				}
				if !allOK {
					continue
				}
				for _, field := range term.MatchFields {
					if field.Key != "metadata.name" {
						return false, fmt.Errorf("unsupported nodeAffinity matchFields key %q", field.Key)
					}
					ok, err := matchNodeSelectorRequirement(field, node.Name, true)
					if err != nil {
						return false, err
					}
					if !ok {
						allOK = false
						break
					}
				}
				if allOK {
					termMatched = true
					break
				}
			}
			if !termMatched {
				return false, nil
			}
		}
	}
	for _, taint := range node.Spec.Taints {
		if taint.Effect == corev1.TaintEffectNoSchedule || taint.Effect == corev1.TaintEffectNoExecute {
			if !toleratesTaint(spec.Tolerations, taint) {
				return false, nil
			}
		}
	}
	return true, nil
}

// PodRequests computes Kubernetes scheduler request accounting (container sum, max with init containers, plus overhead).
func PodRequests(spec corev1.PodSpec) (map[string]float64, error) {
	if spec.Resources != nil {
		return nil, fmt.Errorf("pod-level resources are not supported for request accounting")
	}
	total := make(map[string]float64)
	for _, c := range spec.Containers {
		for k, q := range c.Resources.Requests {
			total[string(k)] += q.AsApproximateFloat64()
		}
	}
	for _, ic := range spec.InitContainers {
		if ic.RestartPolicy != nil && *ic.RestartPolicy == corev1.ContainerRestartPolicyAlways {
			return nil, fmt.Errorf("restartable init containers are not supported for request accounting")
		}
		for k, q := range ic.Resources.Requests {
			key := string(k)
			val := q.AsApproximateFloat64()
			if val > total[key] {
				total[key] = val
			}
		}
	}
	for k, q := range spec.Overhead {
		total[string(k)] += q.AsApproximateFloat64()
	}
	return total, nil
}

// SelectorMatches evaluates a Deployment LabelSelector against Pod labels.
func SelectorMatches(selector *metav1.LabelSelector, labels map[string]string) (bool, error) {
	if selector == nil {
		return false, nil
	}
	for k, v := range selector.MatchLabels {
		if labels[k] != v {
			return false, nil
		}
	}
	for _, term := range selector.MatchExpressions {
		val, exists := labels[term.Key]
		inValues := false
		for _, candidate := range term.Values {
			if candidate == val {
				inValues = true
				break
			}
		}
		var matched bool
		switch term.Operator {
		case metav1.LabelSelectorOpIn:
			matched = exists && inValues
		case metav1.LabelSelectorOpNotIn:
			matched = !exists || !inValues
		case metav1.LabelSelectorOpExists:
			matched = exists
		case metav1.LabelSelectorOpDoesNotExist:
			matched = !exists
		default:
			return false, fmt.Errorf("unknown label selector operator %q", term.Operator)
		}
		if !matched {
			return false, nil
		}
	}
	return true, nil
}
