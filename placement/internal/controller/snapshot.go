package controller

import (
	"context"
	"fmt"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/yogarn/5g-mec-controller/placement/internal/policy"
)

func isPodReady(pod *corev1.Pod) bool {
	if pod.DeletionTimestamp != nil {
		return false
	}
	for _, cond := range pod.Status.Conditions {
		if cond.Type == corev1.PodReady && cond.Status == corev1.ConditionTrue {
			return true
		}
	}
	return false
}

func isNodeReady(node *corev1.Node) bool {
	if node.Spec.Unschedulable {
		return false
	}
	for _, cond := range node.Status.Conditions {
		if cond.Type == corev1.NodeReady && cond.Status == corev1.ConditionTrue {
			return true
		}
	}
	return false
}

func (c *Controller) workloadPods(ctx context.Context, dep *appsv1.Deployment) ([]*corev1.Pod, error) {
	list, err := c.Client.CoreV1().Pods(c.Namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, err
	}
	var pods []*corev1.Pod
	activeUIDs, boundUIDs := make(map[string]bool, len(list.Items)), make(map[string]bool, len(list.Items))
	for i := range list.Items {
		pod := &list.Items[i]
		if pod.Status.Phase == corev1.PodSucceeded || pod.Status.Phase == corev1.PodFailed {
			continue
		}
		matched, err := policy.SelectorMatches(dep.Spec.Selector, pod.Labels)
		if err != nil {
			return nil, err
		}
		if !matched {
			continue
		}
		pods = append(pods, pod)
		if uid := string(pod.UID); uid != "" {
			activeUIDs[uid] = true
			if pod.Spec.NodeName != "" {
				boundUIDs[uid] = true
			}
		}
	}
	c.mu.Lock()
	for uid := range c.inFlightBindings {
		if !activeUIDs[uid] || boundUIDs[uid] {
			delete(c.inFlightBindings, uid)
		}
	}
	c.mu.Unlock()
	return pods, nil
}

func (c *Controller) logReadinessIfChanged(dep *appsv1.Deployment, pods []*corev1.Pod) {
	replicas := int32(1)
	if dep.Spec.Replicas != nil {
		replicas = *dep.Spec.Replicas
	}
	ready, unscheduled := 0, 0
	placement := make(map[string]int)
	for _, pod := range pods {
		if isPodReady(pod) {
			ready++
		}
		if pod.Spec.NodeName == "" && pod.DeletionTimestamp == nil {
			unscheduled++
		}
		if pod.Spec.NodeName != "" {
			placement[pod.Spec.NodeName]++
		}
	}
	key := fmt.Sprintf("%d:%d:%d:%d:%v", replicas, len(pods), ready, unscheduled, placement)
	c.mu.Lock()
	changed := key != c.lastReadinessKey
	if changed {
		c.lastReadinessKey = key
	}
	c.mu.Unlock()
	if changed {
		c.Logger.Printf("workload readiness replicas=%d active=%d ready=%d unscheduled=%d placement=%v",
			replicas, len(pods), ready, unscheduled, placement)
	}
}

func (c *Controller) queryNodeTelemetry(ctx context.Context) (map[string]float64, map[string]float64, error) {
	s := c.Settings
	cpu, err := c.Prometheus.PerNode(ctx, s.CPUUtilizationQuery, s.EligibleNodes)
	if err != nil {
		return nil, nil, fmt.Errorf("CPU utilization query requires one series per eligible node: %w", err)
	}
	zeroSet := make(map[string]bool, len(s.ZeroLocalityNodes))
	for _, n := range s.ZeroLocalityNodes {
		zeroSet[n] = true
	}
	var localityNodes []string
	for _, n := range s.EligibleNodes {
		if !zeroSet[n] {
			localityNodes = append(localityNodes, n)
		}
	}
	mobility, err := c.Prometheus.PerNode(ctx, s.MobilityQuery, localityNodes)
	if err != nil {
		return nil, nil, err
	}
	for _, n := range s.ZeroLocalityNodes {
		mobility[n] = 0.0
	}
	return cpu, mobility, nil
}

func (c *Controller) collectAllocatedResources(ctx context.Context, allocated map[string]map[string]float64, extra map[string]inFlightBinding) error {
	allPods, err := c.Client.CoreV1().Pods(corev1.NamespaceAll).List(ctx, metav1.ListOptions{})
	if err != nil {
		return err
	}
	seenBoundUIDs := make(map[string]bool)
	for i := range allPods.Items {
		pod := &allPods.Items[i]
		node := pod.Spec.NodeName
		if _, ok := allocated[node]; !ok || pod.Status.Phase == corev1.PodSucceeded || pod.Status.Phase == corev1.PodFailed {
			continue
		}
		if pod.UID != "" {
			seenBoundUIDs[string(pod.UID)] = true
		}
		reqs, err := policy.PodRequests(pod.Spec)
		if err != nil {
			return err
		}
		for k, v := range reqs {
			allocated[node][k] += v
		}
	}
	for uid, binding := range extra {
		if nodeAlloc, ok := allocated[binding.node]; ok && !seenBoundUIDs[uid] {
			for k, v := range binding.requests {
				nodeAlloc[k] += v
			}
		}
	}
	return nil
}

func (c *Controller) buildPolicyNodes(eligibleKNodes map[string]*corev1.Node, allocated map[string]map[string]float64, cpu, mobility map[string]float64) ([]policy.Node, error) {
	nodes := make([]policy.Node, 0, len(c.Settings.EligibleNodes))
	for _, name := range c.Settings.EligibleNodes {
		kNode := eligibleKNodes[name]
		capMap := make(map[string]float64, len(kNode.Status.Allocatable))
		for k, q := range kNode.Status.Allocatable {
			capMap[string(k)] = q.AsApproximateFloat64()
		}
		if capMap["cpu"] <= 0 || capMap["memory"] <= 0 {
			return nil, fmt.Errorf("node %s lacks allocatable CPU or memory", name)
		}
		nodes = append(nodes, policy.Node{
			Name:           name,
			Capacity:       capMap,
			Allocated:      allocated[name],
			CPUUtilization: cpu[name],
			ArrivalRate:    mobility[name],
			Ready:          isNodeReady(kNode),
		})
	}
	return nodes, nil
}

func (c *Controller) nodesSnapshot(ctx context.Context, extra map[string]inFlightBinding) ([]policy.Node, map[string]*corev1.Node, error) {
	cpu, mobility, err := c.queryNodeTelemetry(ctx)
	if err != nil {
		return nil, nil, err
	}
	nodeList, err := c.Client.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, nil, err
	}
	allNodes := make(map[string]*corev1.Node, len(nodeList.Items))
	for i := range nodeList.Items {
		allNodes[nodeList.Items[i].Name] = &nodeList.Items[i]
	}
	allocated := make(map[string]map[string]float64, len(c.Settings.EligibleNodes))
	eligibleKNodes := make(map[string]*corev1.Node, len(c.Settings.EligibleNodes))
	for _, name := range c.Settings.EligibleNodes {
		kNode, ok := allNodes[name]
		if !ok {
			return nil, nil, fmt.Errorf("eligible node %s missing from Kubernetes", name)
		}
		allocated[name] = make(map[string]float64)
		eligibleKNodes[name] = kNode
	}
	if err := c.collectAllocatedResources(ctx, allocated, extra); err != nil {
		return nil, nil, err
	}
	nodes, err := c.buildPolicyNodes(eligibleKNodes, allocated, cpu, mobility)
	if err != nil {
		return nil, nil, err
	}
	return nodes, eligibleKNodes, nil
}

func allowedNodesForSpec(spec corev1.PodSpec, kNodes map[string]*corev1.Node) (map[string]bool, error) {
	allowed := make(map[string]bool, len(kNodes))
	for name, kNode := range kNodes {
		ok, err := policy.PodFitsNodeConstraints(spec, kNode)
		if err != nil {
			return nil, err
		}
		allowed[name] = ok
	}
	return allowed, nil
}
