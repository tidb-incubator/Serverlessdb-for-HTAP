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

package serverlessdb

import (
	"github.com/tidb-incubator/Serverlessdb-for-HTAP/pkg/sldb-operator/apis/bcrds/v1alpha1"
	"github.com/tidb-incubator/Serverlessdb-for-HTAP/pkg/sldb-operator/controller"
	"github.com/tidb-incubator/Serverlessdb-for-HTAP/pkg/sldb-operator/manager"
	corev1 "k8s.io/api/core/v1"
	apiequality "k8s.io/apimachinery/pkg/api/equality"
	"k8s.io/apimachinery/pkg/api/resource"
	errorutils "k8s.io/apimachinery/pkg/util/errors"
	"k8s.io/client-go/tools/record"
	"k8s.io/klog"
)

// ControlInterface implements the control logic for updating TidbClusters and their children StatefulSets.
// It is implemented as an interface to allow for extensions that provide different semantics.
// Currently, there is only one implementation.
type ControlInterface interface {
	// UpdateTidbCluster implements the control logic for StatefulSet creation, update, and deletion
	UpdateServerlessDB(db *v1alpha1.ServerlessDB) error
}

// NewDefaultServerlessControl returns a new instance of the default implementation TidbClusterControlInterface that
// implements the documented semantics for TidbClusters.
func NewDefaultServerlessDBControl(
	dbControl controller.ServerlessDBControlInterface,
	tidbClusterMemberManager manager.Manager,
	proxyMemberManager manager.Manager,
	sldbPhaseMemberManager manager.Manager,
	sldbFinalizerMemberManager manager.Manager,
	sldbFreezeMemberManager manager.Manager,
	recorder record.EventRecorder,
) ControlInterface {
	return &defaultServerlessDBControl{
		dbControl:                  dbControl,
		tidbClusterMemberManager:   tidbClusterMemberManager,
		proxyMemberManager:         proxyMemberManager,
		sldbPhaseMemberManager:     sldbPhaseMemberManager,
		sldbFinalizerMemberManager: sldbFinalizerMemberManager,
		sldbFreezeMemberManager:    sldbFreezeMemberManager,
		recorder:                   recorder,
	}
}

type defaultServerlessDBControl struct {
	dbControl                  controller.ServerlessDBControlInterface
	tidbClusterMemberManager   manager.Manager
	proxyMemberManager         manager.Manager
	sldbPhaseMemberManager     manager.Manager
	sldbFinalizerMemberManager manager.Manager
	sldbBackupMemberManager    manager.Manager
	sldbRestoreMemberManager   manager.Manager
	sldbFreezeMemberManager    manager.Manager
	recorder                   record.EventRecorder
}

// UpdateServerlessDB executes the core logic loop for a ServerlessDB.
func (c *defaultServerlessDBControl) UpdateServerlessDB(db *v1alpha1.ServerlessDB) error {
	klog.Infof("ServerlessDB %s/%s is being updating.", db.Namespace, db.Name)

	c.setServerlessDBDefault(db)

	var errs []error
	oldStatus := db.Status.DeepCopy()
	oldFinalizers := make([]string, len(db.Finalizers))
	copy(oldFinalizers, db.Finalizers)

	// todo: tidb sync
	if err := c.tidbClusterMemberManager.Sync(db); err != nil {
		errs = append(errs, err)
	}

	if err := c.proxyMemberManager.Sync(db); err != nil {
		errs = append(errs, err)
	}

	if db.Spec.Paused {
		klog.Infof("ServerlessDB %s/%s is being paused.", db.Namespace, db.Name)
		return nil
	}

	if err := c.sldbFinalizerMemberManager.Sync(db); err != nil {
		errs = append(errs, err)
	}

	if err := c.sldbBackupMemberManager.Sync(db); err != nil {
		errs = append(errs, err)
	}

	if err := c.sldbRestoreMemberManager.Sync(db); err != nil {
		errs = append(errs, err)
	}

	if err := c.sldbFreezeMemberManager.Sync(db); err != nil {
		errs = append(errs, err)
	}

	// update status finally
	if err := c.sldbPhaseMemberManager.Sync(db); err != nil {
		errs = append(errs, err)
	}

	// update serverlessdb status, finalizer
	statusEqual := apiequality.Semantic.DeepEqual(&db.Status, oldStatus)
	if !statusEqual {
		klog.Infof("ServerlessDB %s/%s status has changed.", db.Namespace, db.Name)
	}
	finalizerEqual := apiequality.Semantic.DeepEqual(db.Finalizers, oldFinalizers)
	if !finalizerEqual {
		klog.Infof("ServerlessDB %s/%s finalizer has changed.", db.Namespace, db.Name)
	}
	if statusEqual && finalizerEqual {
		return errorutils.NewAggregate(errs)
	}
	if _, err := c.dbControl.UpdateServerlessDB(db.DeepCopy(), &db.Status, oldStatus); err != nil {
		errs = append(errs, err)
	}
	return errorutils.NewAggregate(errs)
}

// Set default value for serverlessdb cr
func (c *defaultServerlessDBControl) setServerlessDBDefault(db *v1alpha1.ServerlessDB) {
	if string(db.Spec.ImagePullPolicy) == "" {
		db.Spec.ImagePullPolicy = corev1.PullIfNotPresent
	}
	// tikv storage
	if _, err := resource.ParseQuantity(db.Spec.MaxValue.Component.StorageSize); err != nil {
		db.Spec.MaxValue.Component.StorageSize = "10Gi"
	}
}

var _ ControlInterface = &defaultServerlessDBControl{}

type FakeServerlessDBControlInterface struct {
	err error
}

func NewFakeServerlessDBControlInterface() *FakeServerlessDBControlInterface {
	return &FakeServerlessDBControlInterface{}
}

func (c *FakeServerlessDBControlInterface) UpdateServerlessDB(_ *v1alpha1.ServerlessDB) error {
	if c.err != nil {
		return c.err
	}
	return nil
}

var _ ControlInterface = &FakeServerlessDBControlInterface{}
