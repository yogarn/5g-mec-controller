package controller

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/cache"

	"github.com/yogarn/5g-mec-controller/placement/internal/config"
)

// TelemetrySource abstracts Prometheus per-node instant vector queries.
type TelemetrySource interface {
	PerNode(ctx context.Context, expr string, expectedNodes []string) (map[string]float64, error)
}

type inFlightBinding struct {
	node     string
	requests map[string]float64
}

// BoundPodResult records a Pod -> Node binding executed by ReconcileScheduler.
type BoundPodResult struct {
	Pod  string
	Node string
}

// Controller implements the Kubernetes-native Scheduler (Pod Binding API)
// and Descheduler (policy/v1 Eviction API) for a single Deployment workload.
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

func (c *Controller) clearInFlight(uid string) {
	if uid == "" {
		return
	}
	c.mu.Lock()
	delete(c.inFlightBindings, uid)
	c.mu.Unlock()
}

func (c *Controller) startPodInformer(ctx context.Context, enqueue func()) error {
	resync := time.Duration(c.Settings.SchedulerIntervalSeconds * float64(time.Second))
	factory := informers.NewSharedInformerFactoryWithOptions(c.Client, resync, informers.WithNamespace(c.Namespace))
	podInformer := factory.Core().V1().Pods().Informer()
	_, err := podInformer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj any) {
			if pod, ok := obj.(*corev1.Pod); ok && pod.Spec.SchedulerName == c.Settings.SchedulerName {
				enqueue()
			}
		},
		UpdateFunc: func(_, newObj any) {
			if pod, ok := newObj.(*corev1.Pod); ok && pod.Spec.SchedulerName == c.Settings.SchedulerName {
				if pod.Spec.NodeName != "" {
					c.clearInFlight(string(pod.UID))
				}
				enqueue()
			}
		},
		DeleteFunc: func(obj any) {
			if pod, ok := obj.(*corev1.Pod); ok {
				c.clearInFlight(string(pod.UID))
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
	return nil
}

// Run starts the Pod Informer and periodic scheduler/descheduler loops until ctx is cancelled.
func (c *Controller) Run(ctx context.Context) error {
	trigger := make(chan struct{}, 1)
	enqueue := func() {
		select {
		case trigger <- struct{}{}:
		default:
		}
	}
	if err := c.startPodInformer(ctx, enqueue); err != nil {
		return err
	}
	enqueue()

	schedInterval := max(100*time.Millisecond, time.Duration(c.Settings.SchedulerIntervalSeconds*float64(time.Second)))
	schedTicker := time.NewTicker(schedInterval)
	defer schedTicker.Stop()

	deschedInterval := max(time.Second, time.Duration(c.Settings.DescheduleIntervalSeconds*float64(time.Second)))
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

func (c *Controller) logDecisionTiming(operation string, started time.Time, pod string) {
	c.Logger.Printf("decision timing operation=%s duration_ms=%.6f pod=%s",
		operation, float64(time.Since(started).Nanoseconds())/1e6, pod)
}
