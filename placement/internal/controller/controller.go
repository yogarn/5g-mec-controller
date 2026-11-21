package controller

import (
	"context"
	"fmt"
	"log"
	"sort"
	"sync"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/cache"

	"github.com/yogarn/5g-mec-controller/placement/internal/config"
	"github.com/yogarn/5g-mec-controller/placement/internal/policy"
)

// TelemetrySource abstracts Prometheus per-node instant vector queries.
type TelemetrySource interface {
	PerNode(ctx context.Context, expr string, expectedNodes []string) (map[string]float64, error)
}

type inFlightBinding struct {
	node     string
	requests map[string]float64
}

// Controller implements the proposed Kubernetes-native Scheduler (Pod Binding API)
// and Descheduler (policy/v1 Eviction API) for a single Deployment workload.
// Replica count scaling belongs strictly to the Kubernetes HorizontalPodAutoscaler.
type Controller struct {
	Settings   *config.Settings
	Client     kubernetes.Interface
	Prometheus TelemetrySource
	Namespace  string
	Name       string
	Now        func() time.Time
	Logger     *log.Logger

	mu                 sync.Mutex
	inFlightBindings   map[string]inFlightBinding
	lastDescheduleTime time.Time
	hasDescheduled     bool
	lastReadinessKey   string
}

func NewController(settings *config.Settings, client kubernetes.Interface, prom TelemetrySource, namespace, name string) *Controller {
	return &Controller{
		Settings:         settings,
		Client:           client,
		Prometheus:       prom,
		Namespace:        namespace,
		Name:             name,
		Now:              time.Now,
		Logger:           log.Default(),
		inFlightBindings: make(map[string]inFlightBinding),
	}
}

func isPodReady(pod *corev1.Pod) bool {
	if pod.DeletionTimestamp != nil {
		return false
	}
	for _, c := range pod.Status.Conditions {
		if c.Type == corev1.PodReady && c.Status == corev1.ConditionTrue {
			return true
		}
	}
	return false
}

func isNodeReady(node *corev1.Node) bool {
	if node.Spec.Unschedulable {
		return false
	}
	for _, c := range node.Status.Conditions {
		if c.Type == corev1.NodeReady && c.Status == corev1.ConditionTrue {
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
	activeUIDs := make(map[string]bool, len(list.Items))
	boundUIDs := make(map[string]bool, len(list.Items))
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
		uid := string(pod.UID)
		if uid != "" {
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
	ready := 0
	unscheduled := 0
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

func (c *Controller) nodesSnapshot(ctx context.Context, extra map[string]inFlightBinding) ([]policy.Node, map[string]*corev1.Node, error) {
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

	nodeList, err := c.Client.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, nil, err
	}
	allNodes := make(map[string]*corev1.Node, len(nodeList.Items))
	for i := range nodeList.Items {
		n := &nodeList.Items[i]
		allNodes[n.Name] = n
	}
	allocated := make(map[string]map[string]float64, len(s.EligibleNodes))
	eligibleKNodes := make(map[string]*corev1.Node, len(s.EligibleNodes))
	for _, name := range s.EligibleNodes {
		kNode, ok := allNodes[name]
		if !ok {
			return nil, nil, fmt.Errorf("eligible node %s missing from Kubernetes", name)
		}
		allocated[name] = make(map[string]float64)
		eligibleKNodes[name] = kNode
	}

	allPods, err := c.Client.CoreV1().Pods(corev1.NamespaceAll).List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, nil, err
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
			return nil, nil, err
		}
		for k, v := range reqs {
			allocated[node][k] += v
		}
	}
	for uid, binding := range extra {
		if seenBoundUIDs[uid] {
			continue
		}
		nodeAlloc, ok := allocated[binding.node]
		if !ok {
			continue
		}
		for k, v := range binding.requests {
			nodeAlloc[k] += v
		}
	}

	nodes := make([]policy.Node, 0, len(s.EligibleNodes))
	for _, name := range s.EligibleNodes {
		kNode := allNodes[name]
		capMap := make(map[string]float64, len(kNode.Status.Allocatable))
		for k, q := range kNode.Status.Allocatable {
			capMap[string(k)] = q.AsApproximateFloat64()
		}
		if capMap["cpu"] <= 0 || capMap["memory"] <= 0 {
			return nil, nil, fmt.Errorf("node %s lacks allocatable CPU or memory", name)
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

// BoundPodResult records a Pod -> Node binding executed by ReconcileScheduler.
type BoundPodResult struct {
	Pod  string
	Node string
}

// ReconcileScheduler schedules unscheduled experiment Pods matching SchedulerName via Pod Binding API.
func (c *Controller) ReconcileScheduler(ctx context.Context) ([]BoundPodResult, error) {
	if c.Settings.Mode != "mobility-aware" {
		return nil, nil
	}
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

	c.mu.Lock()
	var unscheduled []*corev1.Pod
	for _, pod := range pods {
		if pod.Spec.NodeName != "" ||
			pod.Spec.SchedulerName != c.Settings.SchedulerName ||
			pod.DeletionTimestamp != nil {
			continue
		}
		uid := string(pod.UID)
		if uid != "" {
			if _, inFlight := c.inFlightBindings[uid]; inFlight {
				continue
			}
		}
		unscheduled = append(unscheduled, pod)
	}
	extraSnapshot := make(map[string]inFlightBinding, len(c.inFlightBindings))
	for uid, b := range c.inFlightBindings {
		extraSnapshot[uid] = b
	}
	c.mu.Unlock()

	if len(unscheduled) == 0 {
		return nil, nil
	}

	sort.Slice(unscheduled, func(i, j int) bool {
		ti := unscheduled[i].CreationTimestamp.Time
		tj := unscheduled[j].CreationTimestamp.Time
		if !ti.Equal(tj) {
			return ti.Before(tj)
		}
		if unscheduled[i].Name != unscheduled[j].Name {
			return unscheduled[i].Name < unscheduled[j].Name
		}
		return unscheduled[i].UID < unscheduled[j].UID
	})

	snapshotStarted := time.Now()
	nodes, kNodes, err := c.nodesSnapshot(ctx, extraSnapshot)
	c.logDecisionTiming("scheduler_snapshot", snapshotStarted, "")
	if err != nil {
		return nil, err
	}

	var bound []BoundPodResult
	var podShares map[string]float64
	var podSharePerBinding float64
	if c.Settings.PodShareScoring {
		podShares = make(map[string]float64)
		active := 0.0
		for _, existing := range pods {
			if existing.DeletionTimestamp != nil {
				continue
			}
			active++
			node := existing.Spec.NodeName
			if node == "" {
				node = extraSnapshot[string(existing.UID)].node
			}
			if node != "" {
				podShares[node]++
			}
		}
		// Include desired replicas before the ReplicaSet creates the whole
		// batch, so the first bound pods do not represent the entire fleet.
		if dep.Spec.Replicas != nil {
			active = max(active, float64(*dep.Spec.Replicas))
		}
		podSharePerBinding = 1 / active
		for name := range podShares {
			podShares[name] *= podSharePerBinding
		}
	}
	for _, pod := range unscheduled {
		reqs, err := policy.PodRequests(pod.Spec)
		if err != nil {
			c.Logger.Printf("scheduler skipped pod=%s uid=%s reason=invalid-requests err=%v", pod.Name, pod.UID, err)
			continue
		}
		allowed, err := allowedNodesForSpec(pod.Spec, kNodes)
		if err != nil {
			c.Logger.Printf("scheduler skipped pod=%s uid=%s reason=unsupported-constraints err=%v", pod.Name, pod.UID, err)
			continue
		}
		policyStarted := time.Now()
		targetNode, scores, err := policy.ScoreCandidateNode(
			nodes, reqs, c.Settings.CPUSaturation, c.Settings.LocalityWeight, c.Settings.ResourceWeights, "", allowed, podShares,
		)
		c.logDecisionTiming("scheduler_score", policyStarted, pod.Name)
		if err != nil {
			return bound, err
		}
		if targetNode == "" {
			c.Logger.Printf("scheduler deferred reason=no-feasible-node pod=%s uid=%s", pod.Name, pod.UID)
			continue
		}
		if !c.Settings.Mutate {
			c.Logger.Printf("scheduler dry-run pod=%s uid=%s node=%s score=%.6f scores=%v",
				pod.Name, pod.UID, targetNode, scores[targetNode], scores)
			nodes = policy.AddNodeAllocation(nodes, targetNode, reqs)
			if podShares != nil {
				podShares[targetNode] += podSharePerBinding
			}
			bound = append(bound, BoundPodResult{Pod: pod.Name, Node: targetNode})
			continue
		}

		binding := &corev1.Binding{
			ObjectMeta: metav1.ObjectMeta{
				Name:      pod.Name,
				Namespace: c.Namespace,
			},
			Target: corev1.ObjectReference{
				APIVersion: "v1",
				Kind:       "Node",
				Name:       targetNode,
			},
		}
		bindStarted := time.Now()
		bindErr := c.Client.CoreV1().Pods(c.Namespace).Bind(ctx, binding, metav1.CreateOptions{})
		c.logDecisionTiming("scheduler_bind", bindStarted, pod.Name)
		if err := bindErr; err != nil {
			c.Logger.Printf("scheduler binding failed pod=%s uid=%s node=%s err=%v", pod.Name, pod.UID, targetNode, err)
			continue
		}
		if pod.UID != "" {
			c.mu.Lock()
			c.inFlightBindings[string(pod.UID)] = inFlightBinding{node: targetNode, requests: reqs}
			c.mu.Unlock()
		}
		nodes = policy.AddNodeAllocation(nodes, targetNode, reqs)
		if podShares != nil {
			podShares[targetNode] += podSharePerBinding
		}
		bound = append(bound, BoundPodResult{Pod: pod.Name, Node: targetNode})
		c.Logger.Printf("scheduler bound pod=%s uid=%s node=%s score=%.6f scores=%v",
			pod.Name, pod.UID, targetNode, scores[targetNode], scores)
	}
	return bound, nil
}

// ReconcileDescheduler evicts at most one bound Pod with positive delta via policy/v1 Eviction API.
func (c *Controller) ReconcileDescheduler(ctx context.Context) (*policy.DescheduleCandidate, error) {
	return c.reconcileDescheduler(ctx, true)
}

func (c *Controller) reconcileDescheduler(ctx context.Context, enforceInterval bool) (*policy.DescheduleCandidate, error) {
	if c.Settings.Mode != "mobility-aware" {
		return nil, nil
	}
	now := c.Now()
	if enforceInterval {
		c.mu.Lock()
		tooSoon := c.hasDescheduled && now.Sub(c.lastDescheduleTime).Seconds() < c.Settings.DescheduleIntervalSeconds
		c.mu.Unlock()
		if tooSoon {
			return nil, nil
		}
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
	replicas := int32(1)
	if dep.Spec.Replicas != nil {
		replicas = *dep.Spec.Replicas
	}
	activePods := make([]*corev1.Pod, 0, len(pods))
	terminatingCount := 0
	readyCount := 0
	anyUnbound := false
	for _, pod := range pods {
		if pod.DeletionTimestamp != nil {
			terminatingCount++
			continue
		}
		if pod.Spec.SchedulerName != c.Settings.SchedulerName {
			return nil, nil
		}
		activePods = append(activePods, pod)
		if pod.Spec.NodeName == "" {
			anyUnbound = true
		}
		if isPodReady(pod) {
			readyCount++
		}
	}
	c.mu.Lock()
	inFlightCount := len(c.inFlightBindings)
	c.mu.Unlock()

	if replicas < 1 || int32(len(activePods)) != replicas || inFlightCount > 0 || anyUnbound || int32(readyCount) != replicas {
		c.Logger.Printf("descheduler deferred reason=pods-not-settled replicas=%d active=%d terminating=%d ready=%d in_flight=%d",
			replicas, len(activePods), terminatingCount, readyCount, inFlightCount)
		return nil, nil
	}

	c.mu.Lock()
	c.lastDescheduleTime = now
	c.hasDescheduled = true
	c.mu.Unlock()

	counts := make(map[string]int, len(c.Settings.EligibleNodes))
	podsByNode := make(map[string][]*corev1.Pod, len(c.Settings.EligibleNodes))
	for _, n := range c.Settings.EligibleNodes {
		counts[n] = 0
	}
	for _, pod := range activePods {
		node := pod.Spec.NodeName
		if _, ok := counts[node]; ok {
			counts[node]++
			podsByNode[node] = append(podsByNode[node], pod)
		}
	}

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

	nodes, kNodes, err := c.nodesSnapshot(ctx, nil)
	if err != nil {
		return nil, err
	}
	allowed, err := allowedNodesForSpec(dep.Spec.Template.Spec, kNodes)
	if err != nil {
		c.Logger.Printf("descheduler skipped reason=unsupported-constraints err=%v", err)
		return nil, err
	}
	policyStarted := time.Now()
	candidate, err := policy.SelectDescheduleCandidate(
		nodes, reqs, counts, c.Settings.CPUSaturation, c.Settings.LocalityWeight, c.Settings.ResourceWeights, allowed, c.Settings.PodShareScoring,
	)
	c.logDecisionTiming("descheduler_score", policyStarted, "")
	if err != nil {
		return nil, err
	}
	if candidate == nil {
		c.Logger.Printf("descheduler noop reason=below-min-score-improvement counts=%v", counts)
		return nil, nil
	}

	victims := podsByNode[candidate.Source]
	sort.Slice(victims, func(i, j int) bool { return victims[i].Name < victims[j].Name })
	victim := victims[0]

	if !c.Settings.Mutate {
		c.Logger.Printf("descheduler dry-run pod=%s source=%s candidate_dest=%s delta=%.6f source_score=%.6f destination_score=%.6f",
			victim.Name, candidate.Source, candidate.Destination, candidate.Delta, candidate.SourceScore, candidate.DestinationScore)
		return candidate, nil
	}

	eviction := &policyv1.Eviction{
		ObjectMeta: metav1.ObjectMeta{
			Name:      victim.Name,
			Namespace: c.Namespace,
		},
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

// Run starts event-driven Informers for Pod scheduling and periodic Descheduler checks until ctx is cancelled.
func (c *Controller) Run(ctx context.Context) error {
	factory := informers.NewSharedInformerFactoryWithOptions(
		c.Client,
		time.Duration(c.Settings.SchedulerIntervalSeconds*float64(time.Second)),
		informers.WithNamespace(c.Namespace),
	)
	trigger := make(chan struct{}, 1)
	enqueue := func() {
		select {
		case trigger <- struct{}{}:
		default:
		}
	}

	podInformer := factory.Core().V1().Pods().Informer()
	_, err := podInformer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj any) {
			if pod, ok := obj.(*corev1.Pod); ok && pod.Spec.SchedulerName == c.Settings.SchedulerName {
				enqueue()
			}
		},
		UpdateFunc: func(_, newObj any) {
			if pod, ok := newObj.(*corev1.Pod); ok && pod.Spec.SchedulerName == c.Settings.SchedulerName {
				if pod.Spec.NodeName != "" && pod.UID != "" {
					c.mu.Lock()
					delete(c.inFlightBindings, string(pod.UID))
					c.mu.Unlock()
				}
				enqueue()
			}
		},
		DeleteFunc: func(obj any) {
			if pod, ok := obj.(*corev1.Pod); ok && pod.UID != "" {
				c.mu.Lock()
				delete(c.inFlightBindings, string(pod.UID))
				c.mu.Unlock()
			}
			enqueue()
		},
	})
	if err != nil {
		return err
	}

	factory.Start(ctx.Done())
	if !cache.WaitForCacheSync(ctx.Done(), podInformer.HasSynced) {
		return fmt.Errorf("failed to sync Pod informer cache")
	}

	enqueue()
	schedInterval := time.Duration(c.Settings.SchedulerIntervalSeconds * float64(time.Second))
	if schedInterval < 100*time.Millisecond {
		schedInterval = 100 * time.Millisecond
	}
	schedTicker := time.NewTicker(schedInterval)
	defer schedTicker.Stop()

	deschedInterval := time.Duration(c.Settings.DescheduleIntervalSeconds * float64(time.Second))
	if deschedInterval < time.Second {
		deschedInterval = time.Second
	}
	deschedTicker := time.NewTicker(deschedInterval)
	defer deschedTicker.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-trigger:
			if _, err := c.ReconcileScheduler(ctx); err != nil {
				c.Logger.Printf("scheduler error: %v", err)
			}
		case <-schedTicker.C:
			if _, err := c.ReconcileScheduler(ctx); err != nil {
				c.Logger.Printf("scheduler error: %v", err)
			}
		case <-deschedTicker.C:
			if _, err := c.reconcileDescheduler(ctx, false); err != nil {
				c.Logger.Printf("descheduler error: %v", err)
			}
		}
	}
}

// logDecisionTiming uses monotonic wall time, separate from queueing/lifecycle delay.
// Snapshot includes API/telemetry reads; score is only the policy call; bind is API time.
func (c *Controller) logDecisionTiming(operation string, started time.Time, pod string) {
	c.Logger.Printf("decision timing operation=%s duration_ms=%.6f pod=%s", operation, float64(time.Since(started).Nanoseconds())/1e6, pod)
}
