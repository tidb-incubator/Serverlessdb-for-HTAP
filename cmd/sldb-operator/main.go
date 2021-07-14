// Copyright 2018 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"context"
	"flag"
	"k8s.io/client-go/rest"
	"net/http"
	_ "net/http/pprof"
	"os"
	"reflect"

	versioned2 "github.com/pingcap/tidb-operator/pkg/client/clientset/versioned"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/leaderelection"
	"k8s.io/client-go/tools/leaderelection/resourcelock"
	"k8s.io/client-go/tools/record"
	"k8s.io/component-base/logs"
	"k8s.io/klog"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/tidb-incubator/Serverlessdb-for-HTAP/pkg/sldb-operator/client/clientset/versioned"
	"github.com/tidb-incubator/Serverlessdb-for-HTAP/pkg/sldb-operator/controller"
	"github.com/tidb-incubator/Serverlessdb-for-HTAP/pkg/sldb-operator/controller/serverlessdb"
	"github.com/tidb-incubator/Serverlessdb-for-HTAP/pkg/sldb-operator/scheme"
	"github.com/tidb-incubator/Serverlessdb-for-HTAP/pkg/sldb-operator/version"
)

func main() {
	cliCfg := controller.DefaultCLIConfig()
	cliCfg.AddFlag(flag.CommandLine)
	//features.DefaultFeatureGate.AddFlag(flag.CommandLine)
	//flag.Parse()

	if cliCfg.PrintVersion {
		version.PrintVersionInfo()
		os.Exit(0)
	}

	logs.InitLogs()
	defer logs.FlushLogs()

	version.LogVersionInfo()
	flag.VisitAll(func(flag *flag.Flag) {
		klog.V(1).Infof("FLAG: --%s=%q", flag.Name, flag.Value)
	})

	hostName, err := os.Hostname()
	if err != nil {
		klog.Fatalf("failed to get hostname: %v", err)
	}

	ns := os.Getenv("WATCH_NAMESPACE")
	// for debug
	//ns = "default"
	if ns == "" {
		// default ns for operator
		ns = "tidb-admin"
		klog.Fatal("NAMESPACE environment variable not set")
	}

	cfg, err := rest.InClusterConfig()
	// for debug
	if err != nil {
		klog.Fatalf("failed to get config: %v", err)
	}

	cli, err := versioned.NewForConfig(cfg)
	if err != nil {
		klog.Fatalf("failed to create Clientset: %v", err)
	}
	var kubeCli kubernetes.Interface
	kubeCli, err = kubernetes.NewForConfig(cfg)
	if err != nil {
		klog.Fatalf("failed to get kubernetes Clientset: %v", err)
	}

	pingcapCli, err := versioned2.NewForConfig(cfg)
	if err != nil {
		klog.Fatalf("failed to get kubernetes Clientset: %v", err)
	}
	// TODO: optimize the read of genericCli with the shared cache
	genericCli, err := client.New(cfg, client.Options{Scheme: scheme.Scheme})
	if err != nil {
		klog.Fatalf("failed to get the generic kube-apiserver client: %v", err)
	}

	deps := controller.NewDependencies(ns, cliCfg, cfg, cli, pingcapCli, kubeCli, genericCli)
	controllerCtx, cancel := context.WithCancel(context.Background())
	defer cancel()

	onStarted := func(ctx context.Context) {
		// Define some nested types to simplify the codebase
		type Controller interface {
			Run(int, <-chan struct{})
		}
		type InformerFactory interface {
			Start(stopCh <-chan struct{})
			WaitForCacheSync(stopCh <-chan struct{}) map[reflect.Type]bool
		}

		// Initialize all controllers
		controllers := []Controller{
			// add serverlessdb controller
			serverlessdb.NewController(deps),
		}

		// Start informer factories after all controllers are initialized.
		informerFactories := []InformerFactory{
			deps.InformerFactory,
			deps.PingcapInformerFactory,
			deps.KubeInformerFactory,
			deps.LabelFilterKubeInformerFactory,
		}
		for _, f := range informerFactories {
			f.Start(ctx.Done())
			for v, synced := range f.WaitForCacheSync(wait.NeverStop) {
				if !synced {
					klog.Fatalf("error syncing informer for %v", v)
				}
			}
		}
		klog.Info("cache of informer factories sync successfully")

		// Start syncLoop for all controllers
		for _, controller := range controllers {
			c := controller
			go wait.Forever(func() { c.Run(cliCfg.Workers, ctx.Done()) }, cliCfg.WaitDuration)
		}
	}
	onStopped := func() {
		klog.Fatalf("leader election lost")
	}

	endPointsName := "sldb-controller-manager"
	// leader election for multiple tidb-controller-manager instances
	go wait.Forever(func() {
		leaderelection.RunOrDie(controllerCtx, leaderelection.LeaderElectionConfig{
			Lock: &resourcelock.EndpointsLock{
				EndpointsMeta: metav1.ObjectMeta{
					Namespace: ns,
					Name:      endPointsName,
				},
				Client: kubeCli.CoreV1(),
				LockConfig: resourcelock.ResourceLockConfig{
					Identity:      hostName,
					EventRecorder: &record.FakeRecorder{},
				},
			},
			LeaseDuration: cliCfg.LeaseDuration,
			RenewDeadline: cliCfg.RenewDuration,
			RetryPeriod:   cliCfg.RetryPeriod,
			Callbacks: leaderelection.LeaderCallbacks{
				OnStartedLeading: onStarted,
				OnStoppedLeading: onStopped,
			},
		})
	}, cliCfg.WaitDuration)

	klog.Fatal(http.ListenAndServe(":6060", nil))
}
