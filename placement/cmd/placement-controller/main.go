package main

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"

	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"

	"github.com/yogarn/5g-mec-controller/placement/internal/config"
	"github.com/yogarn/5g-mec-controller/placement/internal/controller"
	"github.com/yogarn/5g-mec-controller/placement/internal/telemetry"
)

func buildKubeConfig(kubeconfig string) (*rest.Config, error) {
	if kubeconfig != "" {
		return clientcmd.BuildConfigFromFlags("", kubeconfig)
	}
	if cfg, err := rest.InClusterConfig(); err == nil {
		return cfg, nil
	}
	loadingRules := clientcmd.NewDefaultClientConfigLoadingRules()
	return clientcmd.NewNonInteractiveDeferredLoadingClientConfig(loadingRules, &clientcmd.ConfigOverrides{}).ClientConfig()
}

func main() {
	configPath := flag.String("config", "config.yaml", "path to controller config.yaml")
	kubeconfig := flag.String("kubeconfig", os.Getenv("KUBECONFIG"), "optional kubeconfig path (defaults to in-cluster or default kubeconfig)")
	once := flag.Bool("once", false, "run one scheduler and descheduler pass and exit")
	flag.Parse()

	log.SetFlags(log.LstdFlags | log.Lmicroseconds)

	settings, err := config.LoadSettings(*configPath)
	if err != nil {
		log.Fatalf("configuration error: %v", err)
	}
	ns, name, err := config.WorkloadIdentity(settings.Manifest, settings.Namespace)
	if err != nil {
		log.Fatalf("workload identity error: %v", err)
	}
	restCfg, err := buildKubeConfig(*kubeconfig)
	if err != nil {
		log.Fatalf("kubernetes config error: %v", err)
	}
	clientset, err := kubernetes.NewForConfig(restCfg)
	if err != nil {
		log.Fatalf("kubernetes client error: %v", err)
	}

	prom := telemetry.NewPrometheus(settings.PrometheusURL, settings.MaxMetricAgeSeconds)
	ctrl := controller.NewController(settings, clientset, prom, ns, name)

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	if *once {
		if _, err := ctrl.ReconcileScheduler(ctx); err != nil {
			log.Fatalf("scheduler pass failed: %v", err)
		}
		if _, err := ctrl.ReconcileDescheduler(ctx); err != nil {
			log.Fatalf("descheduler pass failed: %v", err)
		}
		return
	}

	log.Printf("starting placement-controller namespace=%s deployment=%s schedulerName=%s mutate=%v",
		ns, name, settings.SchedulerName, settings.Mutate)
	if err := ctrl.Run(ctx); err != nil {
		log.Fatalf("controller stopped with error: %v", err)
	}
}
