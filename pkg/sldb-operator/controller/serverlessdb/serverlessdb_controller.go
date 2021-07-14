// Copyright 2021 CMSS, Inc.
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

package serverlessdb

import (
	"fmt"
	"time"

	perrors "github.com/pingcap/errors"
	"k8s.io/apimachinery/pkg/api/errors"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/util/workqueue"
	"k8s.io/klog"

	"github.com/tidb-incubator/Serverlessdb-for-HTAP/pkg/sldb-operator/apis/bcrds/v1alpha1"
	"github.com/tidb-incubator/Serverlessdb-for-HTAP/pkg/sldb-operator/controller"
	"github.com/tidb-incubator/Serverlessdb-for-HTAP/pkg/sldb-operator/manager/member"
	"github.com/tidb-incubator/Serverlessdb-for-HTAP/pkg/sldb-operator/util"
)

// Controller controls ServerlessDB.
type Controller struct {
	deps *controller.Dependencies
	// control returns an interface capable of syncing a ServerlessDB.
	// Abstracted out for testing.
	control ControlInterface
	// ServerlessDB that need to be synced.
	queue workqueue.RateLimitingInterface

	diskUsageUpdater Updater
}

// NewController creates a ServerlessDB controller.
func NewController(deps *controller.Dependencies) *Controller {
	c := &Controller{
		deps: deps,
		control: NewDefaultServerlessDBControl(
			deps.ServerlessControl,
			member.NewTCMemberManager(deps),
			member.NewProxyMemberManager(deps),
			member.NewSldbPhaseMemberManager(deps),
			member.NewSldbFinalizerMemberManager(deps),
			member.NewSldbFreezeMemberManager(deps),
			deps.Recorder),
		queue:            workqueue.NewNamedRateLimitingQueue(workqueue.DefaultControllerRateLimiter(), "serverlessdb"),
		diskUsageUpdater: &DiskUsageUpdater{deps},
	}

	serverlessdbInformer := deps.InformerFactory.Bcrds().V1alpha1().ServerlessDBs()
	//statefulsetInformer := deps.KubeInformerFactory.Apps().V1().StatefulSets()
	serverlessdbInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: c.enqueueServerlessDB,
		UpdateFunc: func(old, cur interface{}) {
			c.enqueueServerlessDB(cur)
		},
		DeleteFunc: c.enqueueServerlessDB,
	})

	return c
}

// Run runs the serverlessdb controller.
func (c *Controller) Run(workers int, stopCh <-chan struct{}) {
	defer utilruntime.HandleCrash()
	defer c.queue.ShutDown()

	klog.Info("Starting ServerlessDB controller")
	defer klog.Info("Shutting down ServerlessDB controller")

	for i := 0; i < workers; i++ {
		go wait.Until(c.worker, time.Second, stopCh)
	}

	// start diskUsageUpdater periodically.
	go wait.Until(c.diskUsageUpdater.Run, c.deps.CLIConfig.InstanceFetchInterval, stopCh)

	<-stopCh
}

// worker runs a worker goroutine that invokes processNextWorkItem until the the controller's queue is closed
func (c *Controller) worker() {
	for c.processNextWorkItem() {
	}
}

// processNextWorkItem dequeues items, processes them, and marks them done. It enforces that the syncHandler is never
// invoked concurrently with the same key.
func (c *Controller) processNextWorkItem() bool {
	key, quit := c.queue.Get()
	if quit {
		return false
	}
	defer c.queue.Done(key)
	if err := c.sync(key.(string)); err != nil {
		if perrors.Find(err, util.IsRequeueError) != nil {
			klog.Infof("ServerlessDB: %v, still need sync: %v, requeuing", key.(string), err)
		} else {
			utilruntime.HandleError(fmt.Errorf("ServerlessDB: %v, sync failed %v, requeuing", key.(string), err))
		}
		c.queue.AddRateLimited(key)
	} else {
		c.queue.Forget(key)
	}
	return true
}

// sync syncs the given serverlessdb.
func (c *Controller) sync(key string) error {
	startTime := time.Now()
	defer func() {
		klog.V(4).Infof("Finished syncing ServerlessDB %q (%v)", key, time.Since(startTime))
	}()

	ns, name, err := cache.SplitMetaNamespaceKey(key)
	if err != nil {
		return err
	}
	db, err := c.deps.ServerlessDBLister.ServerlessDBs(ns).Get(name)
	if errors.IsNotFound(err) {
		klog.Infof("ServerlessDB has been deleted %v", key)
		return nil
	}
	if err != nil {
		return err
	}

	return c.syncServerlessDB(db.DeepCopy())
}

func (c *Controller) syncServerlessDB(db *v1alpha1.ServerlessDB) error {
	return c.control.UpdateServerlessDB(db)
}

// enqueueServerlessDB enqueues the given ServerlessDB in the work queue.
func (c *Controller) enqueueServerlessDB(obj interface{}) {
	key, err := cache.DeletionHandlingMetaNamespaceKeyFunc(obj)
	if err != nil {
		utilruntime.HandleError(fmt.Errorf("Cound't get key for object %+v: %v", obj, err))
		return
	}
	c.queue.Add(key)
}
