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

package controller

import (
	"fmt"
	"k8s.io/client-go/tools/cache"
	"strings"

	corev1 "k8s.io/api/core/v1"
	apiequality "k8s.io/apimachinery/pkg/api/equality"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/client-go/tools/record"
	"k8s.io/client-go/util/retry"
	"k8s.io/klog"

	"github.com/tidb-incubator/Serverlessdb-for-HTAP/pkg/sldb-operator/apis/bcrds/v1alpha1"
	"github.com/tidb-incubator/Serverlessdb-for-HTAP/pkg/sldb-operator/client/clientset/versioned"
	informers "github.com/tidb-incubator/Serverlessdb-for-HTAP/pkg/sldb-operator/client/informers/externalversions/bcrds/v1alpha1"
	listers "github.com/tidb-incubator/Serverlessdb-for-HTAP/pkg/sldb-operator/client/listers/bcrds/v1alpha1"
)

// ServerlessDBControlInterface manages ServerlessDB
type ServerlessDBControlInterface interface {
	Update(db *v1alpha1.ServerlessDB) (*v1alpha1.ServerlessDB, error)
	UpdateServerlessDB(*v1alpha1.ServerlessDB, *v1alpha1.ServerlessDBStatus, *v1alpha1.ServerlessDBStatus) (*v1alpha1.ServerlessDB, error)
	Create(*v1alpha1.ServerlessDB) error
	Patch(tc *v1alpha1.ServerlessDB, data []byte, subresources ...string) (result *v1alpha1.ServerlessDB, err error)
	RecordServerlessDBEvent(verb, message string, object runtime.Object, err error)
}

type realServerlessDBControl struct {
	cli      versioned.Interface
	dbLister listers.ServerlessDBLister
	recorder record.EventRecorder
}

// NewRealServerlessDBControl creates a new ServerlessDBControlInterface
func NewRealServerlessDBControl(cli versioned.Interface,
	dbLister listers.ServerlessDBLister,
	recorder record.EventRecorder) ServerlessDBControlInterface {
	return &realServerlessDBControl{
		cli,
		dbLister,
		recorder,
	}
}

func (c *realServerlessDBControl) Update(db *v1alpha1.ServerlessDB) (*v1alpha1.ServerlessDB, error) {
	return c.UpdateServerlessDB(db, nil, nil)
}

func (c *realServerlessDBControl) UpdateServerlessDB(db *v1alpha1.ServerlessDB, newStatus *v1alpha1.ServerlessDBStatus, oldStatus *v1alpha1.ServerlessDBStatus) (*v1alpha1.ServerlessDB, error) {
	ns := db.GetNamespace()
	tcName := db.GetName()

	status := db.Status.DeepCopy()
	var serverlessDB *v1alpha1.ServerlessDB

	// don't wait due to limited number of clients, but backoff after the default number of steps
	err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		var updateErr error
		serverlessDB, updateErr = c.cli.BcrdsV1alpha1().ServerlessDBs(ns).Update(db)
		if updateErr == nil {
			klog.Infof("ServerlessDB: [%s/%s] updated spec successfully", ns, tcName)
			return nil
		}
		klog.V(4).Infof("failed to update ServerlessDB: [%s/%s], error: %v", ns, tcName, updateErr)

		if updated, err := c.dbLister.ServerlessDBs(ns).Get(tcName); err == nil {
			// make a copy so we don't mutate the shared cache
			db = updated.DeepCopy()
			db.Status = *status
		} else {
			utilruntime.HandleError(fmt.Errorf("error getting updated ServerlessDB %s/%s from lister: %v", ns, tcName, err))
		}

		return updateErr
	})

	updateStatusErr := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		var updateErr error
		if !apiequality.Semantic.DeepEqual(newStatus, oldStatus) {
			dbUpdating := db.DeepCopy()
			dbUpdating.Status = *newStatus
			serverlessDB, updateErr = c.cli.BcrdsV1alpha1().ServerlessDBs(ns).UpdateStatus(dbUpdating)
		} else {
			return nil
		}
		if updateErr == nil {
			klog.Infof("ServerlessDB: [%s/%s] updated status successfully", ns, tcName)
			return nil
		}
		klog.V(4).Infof("failed to update ServerlessDB: [%s/%s], error: %v", ns, tcName, updateErr)

		if updated, err := c.dbLister.ServerlessDBs(ns).Get(tcName); err == nil {
			// make a copy so we don't mutate the shared cache
			db = updated.DeepCopy()
			db.Status = *status
		} else {
			utilruntime.HandleError(fmt.Errorf("error getting updated ServerlessDB %s/%s from lister: %v", ns, tcName, err))
		}

		return updateErr
	})

	if err != nil || updateStatusErr != nil {
		klog.Errorf("failed to update ServerlessDB: [%s/%s], error: %v", ns, tcName, err)
	}
	return serverlessDB, err
}

func (c *realServerlessDBControl) Create(*v1alpha1.ServerlessDB) error {
	return nil
}

func (c *realServerlessDBControl) Patch(tc *v1alpha1.ServerlessDB, data []byte, subresources ...string) (result *v1alpha1.ServerlessDB, err error) {
	err = retry.RetryOnConflict(retry.DefaultRetry, func() error {
		var patchErr error
		_, patchErr = c.cli.BcrdsV1alpha1().ServerlessDBs(tc.Namespace).Patch(tc.Name, types.MergePatchType, data)
		return patchErr
	})
	if err != nil {
		klog.Errorf("failed to patch ServerlessDB: [%s/%s], error: %v", tc.Namespace, tc.Name, err)
	}
	return tc, err
}

func (c *realServerlessDBControl) RecordServerlessDBEvent(verb, message string, object runtime.Object, err error) {
	if err == nil {
		reason := fmt.Sprintf("Successful%s", strings.Title(verb))
		c.recorder.Event(object, corev1.EventTypeNormal, reason, message)
	} else {
		reason := fmt.Sprintf("Failed%s", strings.Title(verb))
		message := fmt.Sprintf("%s: %s", message, err)
		c.recorder.Event(object, corev1.EventTypeWarning, reason, message)
	}
}

// RequestTracker is used by unit test for mocking request error
type RequestTracker struct {
	requests int
	err      error
	after    int
}

// FakeTidbClusterControl is a fake TidbClusterControlInterface
type FakeServerlessDBControl struct {
	SlLister                 listers.ServerlessDBLister
	SlIndexer                cache.Indexer
	updateTidbClusterTracker RequestTracker
}

// NewFakeTidbClusterControl returns a FakeTidbClusterControl
func NewFakeServerlessDBControl(slInformer informers.ServerlessDBInformer) *FakeServerlessDBControl {
	return &FakeServerlessDBControl{
		slInformer.Lister(),
		slInformer.Informer().GetIndexer(),
		RequestTracker{},
	}
}

func (c *FakeServerlessDBControl) Update(db *v1alpha1.ServerlessDB) (*v1alpha1.ServerlessDB, error) {
	return nil, nil
}

func (c *FakeServerlessDBControl) UpdateServerlessDB(db *v1alpha1.ServerlessDB, newStatus *v1alpha1.ServerlessDBStatus, oldStatus *v1alpha1.ServerlessDBStatus) (*v1alpha1.ServerlessDB, error) {
	return nil, nil
}

func (c *FakeServerlessDBControl) Create(*v1alpha1.ServerlessDB) error {
	return nil
}

func (c *FakeServerlessDBControl) Patch(tc *v1alpha1.ServerlessDB, data []byte, subresources ...string) (result *v1alpha1.ServerlessDB, err error) {
	return tc, err
}

func (c *FakeServerlessDBControl) RecordServerlessDBEvent(verb, message string, object runtime.Object, err error) {
}
