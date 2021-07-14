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
	"fmt"
	pcv1alpha1 "github.com/pingcap/tidb-operator/pkg/apis/pingcap/v1alpha1"
	"k8s.io/klog"
	"time"

	"github.com/tidb-incubator/Serverlessdb-for-HTAP/pkg/sldb-operator/apis/bcrds/v1alpha1"
	"github.com/tidb-incubator/Serverlessdb-for-HTAP/pkg/sldb-operator/controller"
	"github.com/tidb-incubator/Serverlessdb-for-HTAP/pkg/sldb-operator/manager"
	"github.com/tidb-incubator/Serverlessdb-for-HTAP/pkg/sldb-operator/util"
	corev1 "k8s.io/api/core/v1"
)

const (
	healthTiDBReplica = 1
	healthTiPDReplica = 3/2 + 1
	healthTiKVReplica = 1
)

type sldbPhaseMemberManager struct {
	deps *controller.Dependencies
}

// NewTCMemberManager returns a *tcMemberManager
func NewSldbPhaseMemberManager(dependencies *controller.Dependencies) manager.Manager {
	return &sldbPhaseMemberManager{
		deps: dependencies,
	}
}

func (m *sldbPhaseMemberManager) Sync(db *v1alpha1.ServerlessDB) error {
	tc, err := m.deps.TiDBClusterLister.TidbClusters(db.Namespace).Get(db.Name)
	if err != nil {
		return fmt.Errorf("fail to get tc %s for cluster %s/%s, error: %s", db.Name, db.Namespace, db.Name, err)
	}

	if db.Status.Phase == v1alpha1.PhaseFailed {
		klog.Warningf("ServerlessDB %s/%s phase is %s", db.Namespace, db.Name, v1alpha1.PhaseFailed)
		return nil
	}
	if db.Status.Phase == "" {
		db.Status.Phase = v1alpha1.PhaseCreating
		tiCondition := util.NewServerlessDBCondition(v1alpha1.TiDBInitial, corev1.ConditionTrue, util.TiDBInitialNotDone, util.TiDBInitialNotDone)
		util.SetServerlessDBCondition(&db.Status, *tiCondition)
	}
	if db.Status.Phase == v1alpha1.PhaseCreating || db.Status.Phase == v1alpha1.PhaseRecovering {
		createDuration, err := time.ParseDuration(m.deps.CLIConfig.Config.Duration.CreateTimeout)
		if err != nil {
			createDuration, _ = time.ParseDuration("1h")
		}
		// default 1 h
		if (db.CreationTimestamp.Time.In(time.UTC).Add(createDuration)).Before(time.Now().In(time.UTC)) {
			db.Status.Phase = v1alpha1.PhaseFailed
			// todo: reset replica to 0 for tidb cluster
			message := fmt.Sprintf("Create ServerlessDB %s time out", db.Name)
			m.deps.Controls.ServerlessControl.RecordServerlessDBEvent("Create", message, db, err)
			//return fmt.Errorf(message)
			return nil
		}
		// check creating completed
		if completed, err := m.IsServerlessDBCreatComplete(db, tc); err != nil {
			return err
		} else {
			if completed {
				db.Status.Phase = v1alpha1.PhaseRecovering
				return nil
			} else {
				db.Status.Phase = v1alpha1.PhaseCreating
				return nil
			}
		}
	}

	if restartTime, ok := db.Annotations[util.RestartAtAnnotationKey]; ok {
		//restartTimeParsed, err := time.Parse(time.RFC3339, restartTime)
		restartTimeParsed, err := time.ParseInLocation("2006-01-02T15:04", restartTime, time.Local)
		if err != nil {
			klog.Errorf("The format of restart annotation value %s for sldb %s/%s is error", restartTime, db.Namespace, db.Name)
		} else {
			internal, err := time.ParseDuration(m.deps.CLIConfig.Config.Duration.RestartCheck)
			if err != nil {
				internal, _ = time.ParseDuration("2m")
			}
			// It was shown as restarting two minutes ago
			timeNow := time.Now().Local()
			// 两者时间差小于2m
			if restartTimeParsed.Sub(timeNow).Seconds() < internal.Seconds() && restartTimeParsed.After(timeNow) {
				klog.Infof("add restart condition type for sldb %s/%s", db.Namespace, db.Name)
				restartCondition := util.NewServerlessDBCondition(v1alpha1.TiDBRestart, corev1.ConditionTrue, util.TiDBRestartNotDone, util.TiDBRestartNotDone)
				util.SetServerlessDBCondition(&db.Status, *restartCondition)
			}
		}
	}

	if db.Status.Phase == v1alpha1.PhaseRecovering {
		return nil
	}

	if available, _ := m.IsServerlessDBAvailable(db); available {
		db.Status.Phase = v1alpha1.PhaseAvailable
	} else {
		db.Status.Phase = v1alpha1.PhaseUnavailable
	}
	return nil
}

// All components of tidb cluster are healthy:
// TidbCluster Ready is True, and proxy is running
func (m *sldbPhaseMemberManager) IsServerlessDBCreatComplete(db *v1alpha1.ServerlessDB, tc *pcv1alpha1.TidbCluster) (bool, error) {
	if db == nil {
		return false, nil
	}
	// TiDB healthy
	if !IsTiDBAllComponentsHealthy(tc) {
		return false, nil
	}

	// TiDB Initial
	if util.GetServerlessDBCondition(db.Status, v1alpha1.TiDBInitial) != nil {
		return false, nil
	}

	// he3proxy healthy
	deploy, err := m.deps.DeploymentLister.Deployments(db.Namespace).Get(util.GetProxyResourceName(db.Name))
	if err != nil {
		return false, fmt.Errorf("fail to get Deployments %s for cluster %s/%s health check, error: %s", db.Name, db.Namespace, db.Name, err)
	}
	return deploy.Status.ReadyReplicas >= deploy.Status.Replicas, nil
}

// isTiDBServerAllAvailable, Determine whether sldb is available
func (m *sldbPhaseMemberManager) IsServerlessDBAvailable(db *v1alpha1.ServerlessDB) (bool, error) {
	if db == nil {
		return false, nil
	}
	selector, _ := util.Label{}.BcRdsInstance(db.Name).Selector()
	list, err := m.deps.TiDBClusterLister.TidbClusters(db.Namespace).List(selector)
	if err != nil {
		klog.Errorf("Error list TidbClusters for instance %s/%s when check ServerlessDB Available: %v.", db.Namespace, db.Name, err)
		return false, err
	}

	pdTiKV := func(tc *pcv1alpha1.TidbCluster) bool {
		// PD
		if tc.Spec.PD.Replicas > 0 {
			if !tc.Status.PD.Leader.Health {
				return false
			}
			healthReplica := 0
			for _, pdMem := range tc.Status.PD.Members {
				if pdMem.Health {
					healthReplica++
				}
			}
			if healthReplica < healthTiPDReplica {
				return false
			}
			if tc.Status.PD.StatefulSet == nil || tc.Status.PD.StatefulSet.ReadyReplicas < healthTiPDReplica {
				return false
			}
		}

		// TiKV
		if tc.Spec.TiKV.Replicas > 0 {
			healthReplica := 0
			for _, kvMem := range tc.Status.TiKV.Stores {
				if kvMem.State == pcv1alpha1.TiKVStateUp {
					healthReplica++
				}
			}
			if healthReplica < healthTiKVReplica {
				return false
			}
			if tc.Status.TiKV.StatefulSet == nil || tc.Status.TiKV.StatefulSet.ReadyReplicas < healthTiKVReplica {
				return false
			}
		}
		return true
	}

	tidb := func(tc *pcv1alpha1.TidbCluster) bool {
		// TiDB
		if tc.Spec.TiDB.Replicas > 0 {
			healthReplica := 0
			for _, kvMem := range tc.Status.TiDB.Members {
				if kvMem.Health {
					healthReplica++
				}
			}
			if healthReplica < healthTiDBReplica {
				return false
			}
		}
		return true
	}

	tidbAvailable := 0
	for _, tc := range list {
		if db.Name == tc.Name {
			if available := pdTiKV(tc); !available {
				return false, nil
			}
		}
		if available := tidb(tc); available {
			tidbAvailable++
		}
	}
	if util.IsServerlessDBSilence(db) {
		return true, nil
	} else {
		if tidbAvailable > 0 {
			return true, nil
		}
	}
	return false, nil
}

// This judgment method is more strict and is used to judge whether the creation and restart are completed
func IsTiDBAllComponentsHealthy(tc *pcv1alpha1.TidbCluster) bool {
	if tc == nil {
		return false
	}
	// tidbcluster condition is empty when creating at the beginning
	if tc.Status.Conditions == nil || len(tc.Status.Conditions) <= 0 {
		return false
	}
	for _, conditions := range tc.Status.Conditions {
		if conditions.Status != corev1.ConditionTrue {
			return false
		}
	}
	return true
}

// The minimum number of replicas of each component of tidb that meet the health requirements
func IsTiDBAvailable(tc *pcv1alpha1.TidbCluster) bool {
	// PD
	if tc.Spec.PD.Replicas > 0 {
		if !tc.Status.PD.Leader.Health {
			return false
		}
		healthReplica := 0
		for _, pdMem := range tc.Status.PD.Members {
			if pdMem.Health {
				healthReplica++
			}
		}
		if healthReplica < healthTiPDReplica {
			return false
		}
		if tc.Status.PD.StatefulSet == nil || tc.Status.PD.StatefulSet.ReadyReplicas < healthTiPDReplica {
			return false
		}
	}

	// TiKV
	if tc.Spec.TiKV.Replicas > 0 {
		healthReplica := 0
		for _, kvMem := range tc.Status.TiKV.Stores {
			if kvMem.State == pcv1alpha1.TiKVStateUp {
				healthReplica++
			}
		}
		if healthReplica < healthTiKVReplica {
			return false
		}
		if tc.Status.TiKV.StatefulSet == nil || tc.Status.TiKV.StatefulSet.ReadyReplicas < healthTiKVReplica {
			return false
		}
	}

	// TiDB
	if tc.Spec.TiDB.Replicas > 0 {
		healthReplica := 0
		for _, kvMem := range tc.Status.TiDB.Members {
			if kvMem.Health {
				healthReplica++
			}
		}
		if healthReplica < healthTiDBReplica {
			return false
		}
	}
	return true
}
