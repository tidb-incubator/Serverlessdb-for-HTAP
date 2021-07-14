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
	"github.com/pingcap/tidb-operator/pkg/controller"
	"github.com/pingcap/tidb-operator/pkg/version"
	"github.com/tidb-incubator/Serverlessdb-for-HTAP/pkg/scale-operator/scalepb"
	"github.com/tidb-incubator/Serverlessdb-for-HTAP/pkg/scale-operator/scaleservice"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/rest"
	"net"

	"github.com/tidb-incubator/Serverlessdb-for-HTAP/pkg/scale-operator/sldbcluster"
	"github.com/tidb-incubator/Serverlessdb-for-HTAP/pkg/scale-operator/timetask"
	//"k8s.io/client-go/tools/clientcmd"
	"k8s.io/component-base/logs"
	"k8s.io/klog"
	_ "net/http/pprof"
	"os"
	"reflect"
)

func main() {
	cliCfg := controller.DefaultCLIConfig()
	cliCfg.AddFlag(flag.CommandLine)
	//features.DefaultFeatureGate.AddFlag(flag.CommandLine)
	flag.Parse()

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

	go timetask.ScalerTimeTask()

	cfg, err := rest.InClusterConfig()
	if err != nil {
		klog.Fatalf("failed to get config: %v", err)
	}

	sldbcluster.Sldb_Cluster_Init(cfg)
	controllerCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	type InformerFactory interface {
		Start(stopCh <-chan struct{})
		WaitForCacheSync(stopCh <-chan struct{}) map[reflect.Type]bool
	}
	// Start informer factories after all controllers are initialized.
	informerFactories := []InformerFactory{
		sldbcluster.SldbClient.SldbSharedInformerFactory,
		sldbcluster.SldbClient.PingCapSharedInformerFactory,
		sldbcluster.SldbClient.KubeInformerFactory,
	}
	for _, f := range informerFactories {
		f.Start(controllerCtx.Done())
		for v, synced := range f.WaitForCacheSync(wait.NeverStop) {
			if !synced {
				klog.Fatalf("error syncing informer for %v", v)
			}
		}
	}
	k8sInformer := sldbcluster.SldbClient.KubeInformerFactory.Core().V1().Pods().Informer()
	pingCapInformer := sldbcluster.SldbClient.PingCapSharedInformerFactory.Pingcap().V1alpha1().TidbClusters().Informer()
	sldbInformer := sldbcluster.SldbClient.SldbSharedInformerFactory.Bcrds().V1alpha1().ServerlessDBs().Informer()
	go wait.Forever(func() { pingCapInformer.Run(controllerCtx.Done()) }, cliCfg.WaitDuration)
	go wait.Forever(func() { sldbInformer.Run(controllerCtx.Done()) }, cliCfg.WaitDuration)
	go wait.Forever(func() { k8sInformer.Run(controllerCtx.Done()) }, cliCfg.WaitDuration)
	klog.Info("cache of informer factories sync successfully")
	//scalerroute.ScalerRoute()
	lis, err := net.Listen("tcp", ":8028") //监听所有网卡8028端口的TCP连接
	if err != nil {
		klog.Fatalf("监听失败: %v", err)
	}
	s := grpc.NewServer() //创建gRPC服务

	scalepb.RegisterScaleServer(s, &scaleservice.Service{})

	// 在gRPC服务器上注册反射服务
	reflection.Register(s)
	// 将监听交给gRPC服务处理
	err = s.Serve(lis)
	if err != nil {
		klog.Fatalf("failed to serve: %v", err)
	}

}
