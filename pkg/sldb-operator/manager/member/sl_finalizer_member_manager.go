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

package member

import (
	"github.com/pingcap/tidb-operator/pkg/label"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/klog"
	"k8s.io/kubernetes/pkg/util/slice"

	"github.com/tidb-incubator/Serverlessdb-for-HTAP/pkg/sldb-operator/apis/bcrds/v1alpha1"
	"github.com/tidb-incubator/Serverlessdb-for-HTAP/pkg/sldb-operator/controller"
	"github.com/tidb-incubator/Serverlessdb-for-HTAP/pkg/sldb-operator/manager"
)

const (
	finalizerPVC    = "sldb-pvc"
	finalizerSecret = "sldb-secret"
)

type sldbFinalizerMemberManager struct {
	deps *controller.Dependencies
}

// NewTCMemberManager returns a *tcMemberManager
func NewSldbFinalizerMemberManager(dependencies *controller.Dependencies) manager.Manager {
	return &sldbFinalizerMemberManager{
		deps: dependencies,
	}
}

func (m *sldbFinalizerMemberManager) Sync(db *v1alpha1.ServerlessDB) error {
	return m.handleFinalizer(db)
}

func (m *sldbFinalizerMemberManager) handleFinalizer(db *v1alpha1.ServerlessDB) error {
	finalizers := db.Finalizers

	if db.DeletionTimestamp.IsZero() {
		if !slice.ContainsString(finalizers, finalizerPVC, nil) {
			// Add finalizer for newly created resource object.
			db.Finalizers = append(finalizers, finalizerPVC)
		}
		if !slice.ContainsString(finalizers, finalizerSecret, nil) {
			// Add finalizer for newly created resource object.
			db.Finalizers = append(finalizers, finalizerSecret)
		}

	} else {
		if slice.ContainsString(finalizers, finalizerPVC, nil) {
			klog.Infof("ServerlessDB [%s/%s]: finalizer [%s] is being deleted", db.Namespace, db.Name, finalizerPVC)
			selector, _ := label.Label{}.Instance(db.Name).Selector()
			err := m.deps.KubeClientset.CoreV1().PersistentVolumeClaims(db.Namespace).DeleteCollection(&metav1.DeleteOptions{}, metav1.ListOptions{LabelSelector: selector.String()})
			if err != nil && !errors.IsNotFound(err) {
				return err
			}
			pvcs, err := m.deps.KubeClientset.CoreV1().PersistentVolumeClaims(db.Namespace).List(metav1.ListOptions{LabelSelector: selector.String()})
			if err != nil && !errors.IsNotFound(err) {
				return err
			}
			// Remove finalizer.
			if pvcs == nil || len(pvcs.Items) <= 0 {
				klog.Infof("ServerlessDB [%s/%s]: PVC of has been deleted", db.Namespace, db.Name)
				db.Finalizers = slice.RemoveString(finalizers, finalizerPVC, nil)
			} else {
				pvcsNums := len(pvcs.Items)
				for _, pvc := range pvcs.Items {
					if !pvc.DeletionTimestamp.IsZero() {
						klog.Infof("ServerlessDB [%s/%s]: DeletionTimestamp of PVC %s is not empty.", db.Namespace, db.Name, pvc.Name)
						pvcsNums--
					}
				}
				if pvcsNums <= 0 {
					db.Finalizers = slice.RemoveString(finalizers, finalizerPVC, nil)
				}
			}
		}
		if slice.ContainsString(finalizers, finalizerSecret, nil) {
			klog.Infof("ServerlessDB [%s/%s]: finalizer [%s] is being deleted ", db.Namespace, db.Name, finalizerSecret)
			selector, _ := label.Label{}.Instance(db.Name).Selector()
			err := m.deps.KubeClientset.CoreV1().Secrets(db.Namespace).DeleteCollection(&metav1.DeleteOptions{}, metav1.ListOptions{LabelSelector: selector.String()})
			if err != nil && !errors.IsNotFound(err) {
				return err
			}
			// Remove finalizer.
			db.Finalizers = slice.RemoveString(finalizers, finalizerSecret, nil)
		}

	}
	return nil
}
