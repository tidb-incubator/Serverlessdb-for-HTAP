// Copyright 2020 PingCAP, Inc.
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

package util

import (
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/tidb-incubator/Serverlessdb-for-HTAP/pkg/sldb-operator/apis/bcrds/v1alpha1"
)

const (
	// Reasons for ServerlessDB conditions.

	// Ready
	Ready = "Ready"
	// TiDBInitialNotDone is added when ti is not done.
	TiDBInitialNotDone = "TiDBInitialNotDone"
	// TiDBRestartNotDone is added when ti is not done.
	TiDBRestartNotDone = "TiDBRestartNotDone"
)

// NewServerlessDBCondition creates a new ServerlessDB condition.
func NewServerlessDBCondition(condType v1alpha1.ConditionType, status v1.ConditionStatus, reason, message string) *v1alpha1.ServerlessDBCondition {
	return &v1alpha1.ServerlessDBCondition{
		Type:               condType,
		Status:             status,
		LastUpdateTime:     metav1.Now(),
		LastTransitionTime: metav1.Now(),
		Reason:             reason,
		Message:            message,
	}
}

// GetServerlessDBCondition returns the condition with the provided type.
func GetServerlessDBCondition(status v1alpha1.ServerlessDBStatus, condType v1alpha1.ConditionType) *v1alpha1.ServerlessDBCondition {
	for i := range status.Conditions {
		c := status.Conditions[i]
		if c.Type == condType {
			return &c
		}
	}
	return nil
}

// SetServerlessDBCondition updates the ServerlessDB to include the provided condition. If the condition that
// we are about to add already exists and has the same status and reason then we are not going to update.
func SetServerlessDBCondition(status *v1alpha1.ServerlessDBStatus, condition v1alpha1.ServerlessDBCondition) {
	if status == nil {
		return
	}
	currentCond := GetServerlessDBCondition(*status, condition.Type)
	// Do not update lastTransitionTime if the status of the condition doesn't change.
	if currentCond != nil && currentCond.Status == condition.Status {
		condition.LastTransitionTime = currentCond.LastTransitionTime
	}
	newConditions := FilterOutCondition(status.Conditions, condition.Type)
	status.Conditions = append(newConditions, condition)
}

// FilterOutCondition returns a new slice of ServerlessDB conditions without conditions with the provided type.
func FilterOutCondition(conditions []v1alpha1.ServerlessDBCondition, condType v1alpha1.ConditionType) []v1alpha1.ServerlessDBCondition {
	var newConditions []v1alpha1.ServerlessDBCondition
	for _, c := range conditions {
		if c.Type == condType {
			continue
		}
		newConditions = append(newConditions, c)
	}
	return newConditions
}

// IsServerlessDBSilence, true means tidb server pods replica is zero.
func IsServerlessDBSilence(db *v1alpha1.ServerlessDB) bool {
	if db == nil {
		return false
	}
	if restartCondition := GetServerlessDBCondition(db.Status, v1alpha1.TiDBSilence); restartCondition != nil {
		return true
	}
	return false
}

// IsServerlessDBRestart, true means tidb is restarting.
func IsServerlessDBRestart(db *v1alpha1.ServerlessDB) bool {
	if db == nil {
		return false
	}
	if restartCondition := GetServerlessDBCondition(db.Status, v1alpha1.TiDBRestart); restartCondition != nil {
		return true
	}
	return false
}
