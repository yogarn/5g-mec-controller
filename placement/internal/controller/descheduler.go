package controller

import (
	"context"
	"fmt"
	"sort"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/yogarn/5g-mec-controller/placement/internal/policy"
)

// ReconcileDescheduler evicts at most one bound Pod with positive delta via policy/v1 Eviction API.
func (c *Controller) ReconcileDescheduler(ctx context.Context) (*policy.DescheduleCandidate, error) {
	return c.reconcileDescheduler(ctx, true)
}

func (c *Controller) descheduleIntervalDue(now time.Time) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return !c.hasDescheduled || now.Sub(c.lastDescheduleTime).Seconds() >= c.Settings.DescheduleIntervalSeconds
}

func (c *Controller) settledActivePods(dep *appsv1.Deployment, pods []*corev1.Pod) ([]*corev1.Pod, bool) {
	replicas := int32(1)
	if dep.Spec.Replicas != nil {
		replicas = *dep.Spec.Replicas
	}
	activePods := make([]*corev1.Pod, 0, len(pods))
	terminating, ready, anyUnbound := 0, 0, false
	for _, pod := range pods {
		if pod.DeletionTimestamp != nil {
			terminating++
			continue
		}
		if pod.Spec.SchedulerName != c.Settings.SchedulerName {
			return nil, false
		}
		activePods = append(activePods, pod)
		if pod.Spec.NodeName == "" {
			anyUnbound = true
		}
		if isPodReady(pod) {
			ready++
		}
	}
	c.mu.Lock()
	inFlight := len(c.inFlightBindings)
	c.mu.Unlock()
	if replicas < 1 || int32(len(activePods)) != replicas || inFlight > 0 || anyUnbound || int32(ready) != replicas {
		c.Logger.Printf("descheduler deferred reason=pods-not-settled replicas=%d active=%d terminating=%d ready=%d in_flight=%d",
			replicas, len(activePods), terminating, ready, inFlight)
		return nil, false
	}
	return activePods, true
}

func (c *Controller) groupPodsByNode(activePods []*corev1.Pod) (map[string]int, map[string][]*corev1.Pod) {
	counts := make(map[string]int, len(c.Settings.EligibleNodes))
	podsByNode := make(map[string][]*corev1.Pod, len(c.Settings.EligibleNodes))
	for _, n := range c.Settings.EligibleNodes {
		counts[n] = 0
	}
	for _, pod := range activePods {
		if _, ok := counts[pod.Spec.NodeName]; ok {
			counts[pod.Spec.NodeName]++
			podsByNode[pod.Spec.NodeName] = append(podsByNode[pod.Spec.NodeName], pod)
		}
	}
	return counts, podsByNode
}

func deploymentRequests(dep *appsv1.Deployment) (map[string]float64, error) {
	rawReq, err := policy.PodRequests(dep.Spec.Template.Spec)
	if err != nil {
		return nil, err
	}
	if rawReq["cpu"] <= 0 || rawReq["memory"] <= 0 {
		return nil, fmt.Errorf("Deployment containers must request CPU and memory")
	}
	reqs := make(map[string]float64, len(rawReq))
	for k, v := range rawReq {
		if v > 0 {
			reqs[k] = v
		}
	}
	return reqs, nil
}

func (c *Controller) evictVictim(ctx context.Context, victim *corev1.Pod, candidate *policy.DescheduleCandidate) (*policy.DescheduleCandidate, error) {
	eviction := &policyv1.Eviction{
		ObjectMeta: metav1.ObjectMeta{Name: victim.Name, Namespace: c.Namespace},
	}
	if err := c.Client.CoreV1().Pods(c.Namespace).EvictV1(ctx, eviction); err != nil {
		if apierrors.IsTooManyRequests(err) {
			c.Logger.Printf("descheduler eviction rejected reason=pdb-or-rate-limit pod=%s source=%s candidate_dest=%s delta=%.6f",
				victim.Name, candidate.Source, candidate.Destination, candidate.Delta)
			return nil, nil
		}
		if apierrors.IsNotFound(err) || apierrors.IsConflict(err) {
			c.Logger.Printf("descheduler eviction skipped reason=conflict-or-not-found pod=%s source=%s err=%v",
				victim.Name, candidate.Source, err)
			return nil, nil
		}
		c.Logger.Printf("descheduler eviction failed pod=%s source=%s err=%v", victim.Name, candidate.Source, err)
		return nil, err
	}
	c.Logger.Printf("descheduler evicted pod=%s source=%s candidate_dest=%s delta=%.6f source_score=%.6f destination_score=%.6f",
		victim.Name, candidate.Source, candidate.Destination, candidate.Delta, candidate.SourceScore, candidate.DestinationScore)
	return candidate, nil
}

func (c *Controller) reconcileDescheduler(ctx context.Context, enforceInterval bool) (*policy.DescheduleCandidate, error) {
	now := c.Now()
	if enforceInterval && !c.descheduleIntervalDue(now) {
		return nil, nil
	}
	dep, err := c.Client.AppsV1().Deployments(c.Namespace).Get(ctx, c.Name, metav1.GetOptions{})
	if err != nil {
		if apierrors.IsNotFound(err) {
			return nil, nil
		}
		return nil, err
	}
	if dep.Spec.Template.Spec.SchedulerName != c.Settings.SchedulerName {
		return nil, nil
	}
	pods, err := c.workloadPods(ctx, dep)
	if err != nil {
		return nil, err
	}
	activePods, settled := c.settledActivePods(dep, pods)
	if !settled {
		return nil, nil
	}
	c.mu.Lock()
	c.lastDescheduleTime, c.hasDescheduled = now, true
	c.mu.Unlock()

	reqs, err := deploymentRequests(dep)
	if err != nil {
		return nil, err
	}
	nodes, kNodes, err := c.nodesSnapshot(ctx, nil)
	if err != nil {
		return nil, err
	}
	allowed, err := allowedNodesForSpec(dep.Spec.Template.Spec, kNodes)
	if err != nil {
		c.Logger.Printf("descheduler skipped reason=unsupported-constraints err=%v", err)
		return nil, err
	}
	counts, podsByNode := c.groupPodsByNode(activePods)
	policyStarted := time.Now()
	candidate, err := policy.SelectDescheduleCandidate(
		nodes, reqs, counts, c.Settings.CPUSaturation, c.Settings.LocalityWeight, c.Settings.ResourceWeights, allowed, c.Settings.PodShareScoring,
	)
	c.logDecisionTiming("descheduler_score", policyStarted, "")
	if err != nil || candidate == nil {
		if err == nil {
			c.Logger.Printf("descheduler noop reason=below-min-score-improvement counts=%v", counts)
		}
		return nil, err
	}
	victims := podsByNode[candidate.Source]
	sort.Slice(victims, func(i, j int) bool { return victims[i].Name < victims[j].Name })
	return c.evictVictim(ctx, victims[0], candidate)
}
