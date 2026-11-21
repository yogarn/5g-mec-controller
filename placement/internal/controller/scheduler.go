package controller

import (
	"context"
	"sort"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/yogarn/5g-mec-controller/placement/internal/policy"
)

func (c *Controller) pendingPods(pods []*corev1.Pod) ([]*corev1.Pod, map[string]inFlightBinding) {
	c.mu.Lock()
	defer c.mu.Unlock()
	var unscheduled []*corev1.Pod
	for _, pod := range pods {
		if pod.Spec.NodeName != "" || pod.Spec.SchedulerName != c.Settings.SchedulerName || pod.DeletionTimestamp != nil {
			continue
		}
		if uid := string(pod.UID); uid != "" {
			if _, inFlight := c.inFlightBindings[uid]; inFlight {
				continue
			}
		}
		unscheduled = append(unscheduled, pod)
	}
	extra := make(map[string]inFlightBinding, len(c.inFlightBindings))
	for uid, b := range c.inFlightBindings {
		extra[uid] = b
	}
	sort.Slice(unscheduled, func(i, j int) bool {
		ti, tj := unscheduled[i].CreationTimestamp.Time, unscheduled[j].CreationTimestamp.Time
		if !ti.Equal(tj) {
			return ti.Before(tj)
		}
		if unscheduled[i].Name != unscheduled[j].Name {
			return unscheduled[i].Name < unscheduled[j].Name
		}
		return unscheduled[i].UID < unscheduled[j].UID
	})
	return unscheduled, extra
}

func (c *Controller) computePodShares(dep *appsv1.Deployment, pods []*corev1.Pod, extra map[string]inFlightBinding) (map[string]float64, float64) {
	if !c.Settings.PodShareScoring {
		return nil, 0
	}
	podShares := make(map[string]float64)
	active := 0.0
	for _, existing := range pods {
		if existing.DeletionTimestamp != nil {
			continue
		}
		active++
		node := existing.Spec.NodeName
		if node == "" {
			node = extra[string(existing.UID)].node
		}
		if node != "" {
			podShares[node]++
		}
	}
	if dep.Spec.Replicas != nil {
		active = max(active, float64(*dep.Spec.Replicas))
	}
	sharePerBinding := 1 / active
	for name := range podShares {
		podShares[name] *= sharePerBinding
	}
	return podShares, sharePerBinding
}

func (c *Controller) bindPod(ctx context.Context, pod *corev1.Pod, targetNode string, reqs map[string]float64) error {
	binding := &corev1.Binding{
		ObjectMeta: metav1.ObjectMeta{Name: pod.Name, Namespace: c.Namespace},
		Target:     corev1.ObjectReference{APIVersion: "v1", Kind: "Node", Name: targetNode},
	}
	bindStarted := time.Now()
	err := c.Client.CoreV1().Pods(c.Namespace).Bind(ctx, binding, metav1.CreateOptions{})
	c.logDecisionTiming("scheduler_bind", bindStarted, pod.Name)
	if err != nil {
		c.Logger.Printf("scheduler binding failed pod=%s uid=%s node=%s err=%v", pod.Name, pod.UID, targetNode, err)
		return err
	}
	if pod.UID != "" {
		c.mu.Lock()
		c.inFlightBindings[string(pod.UID)] = inFlightBinding{node: targetNode, requests: reqs}
		c.mu.Unlock()
	}
	return nil
}

func (c *Controller) scheduleOnePod(ctx context.Context, pod *corev1.Pod, nodes []policy.Node, kNodes map[string]*corev1.Node, podShares map[string]float64, sharePerBinding float64) ([]policy.Node, *BoundPodResult, error) {
	reqs, err := policy.PodRequests(pod.Spec)
	if err != nil {
		c.Logger.Printf("scheduler skipped pod=%s uid=%s reason=invalid-requests err=%v", pod.Name, pod.UID, err)
		return nodes, nil, nil
	}
	allowed, err := allowedNodesForSpec(pod.Spec, kNodes)
	if err != nil {
		c.Logger.Printf("scheduler skipped pod=%s uid=%s reason=unsupported-constraints err=%v", pod.Name, pod.UID, err)
		return nodes, nil, nil
	}
	policyStarted := time.Now()
	targetNode, scores, err := policy.ScoreCandidateNode(
		nodes, reqs, c.Settings.CPUSaturation, c.Settings.LocalityWeight, c.Settings.ResourceWeights, "", allowed, podShares,
	)
	c.logDecisionTiming("scheduler_score", policyStarted, pod.Name)
	if err != nil {
		return nodes, nil, err
	}
	if targetNode == "" {
		c.Logger.Printf("scheduler deferred reason=no-feasible-node pod=%s uid=%s", pod.Name, pod.UID)
		return nodes, nil, nil
	}
	if err := c.bindPod(ctx, pod, targetNode, reqs); err != nil {
		return nodes, nil, nil
	}
	nodes = policy.AddNodeAllocation(nodes, targetNode, reqs)
	if podShares != nil {
		podShares[targetNode] += sharePerBinding
	}
	c.Logger.Printf("scheduler bound pod=%s uid=%s node=%s score=%.6f scores=%v",
		pod.Name, pod.UID, targetNode, scores[targetNode], scores)
	return nodes, &BoundPodResult{Pod: pod.Name, Node: targetNode}, nil
}

// ReconcileScheduler schedules unscheduled experiment Pods matching SchedulerName via Pod Binding API.
func (c *Controller) ReconcileScheduler(ctx context.Context) ([]BoundPodResult, error) {
	dep, err := c.Client.AppsV1().Deployments(c.Namespace).Get(ctx, c.Name, metav1.GetOptions{})
	if err != nil {
		if apierrors.IsNotFound(err) {
			return nil, nil
		}
		return nil, err
	}
	pods, err := c.workloadPods(ctx, dep)
	if err != nil {
		return nil, err
	}
	c.logReadinessIfChanged(dep, pods)

	unscheduled, extra := c.pendingPods(pods)
	if len(unscheduled) == 0 {
		return nil, nil
	}
	snapshotStarted := time.Now()
	nodes, kNodes, err := c.nodesSnapshot(ctx, extra)
	c.logDecisionTiming("scheduler_snapshot", snapshotStarted, "")
	if err != nil {
		return nil, err
	}

	podShares, sharePerBinding := c.computePodShares(dep, pods, extra)
	var bound []BoundPodResult
	for _, pod := range unscheduled {
		var result *BoundPodResult
		nodes, result, err = c.scheduleOnePod(ctx, pod, nodes, kNodes, podShares, sharePerBinding)
		if err != nil {
			return bound, err
		}
		if result != nil {
			bound = append(bound, *result)
		}
	}
	return bound, nil
}
