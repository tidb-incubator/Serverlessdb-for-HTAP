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
	"github.com/tidb-incubator/Serverlessdb-for-HTAP/pkg/sldb-operator/apis/bcrds/v1alpha1"
	"github.com/tidb-incubator/Serverlessdb-for-HTAP/pkg/sldb-operator/controller"
	"github.com/tidb-incubator/Serverlessdb-for-HTAP/pkg/sldb-operator/manager"
	"github.com/tidb-incubator/Serverlessdb-for-HTAP/pkg/sldb-operator/util"
	"k8s.io/apimachinery/pkg/labels"
)

// only freeze tidbClusters, unfreezing is implemented by others.
type sldbFreezeMemberManager struct {
	deps *controller.Dependencies
}

func NewSldbFreezeMemberManager(dependencies *controller.Dependencies) manager.Manager {
	return &sldbFreezeMemberManager{
		deps: dependencies,
	}
}

func (m *sldbFreezeMemberManager) Sync(db *v1alpha1.ServerlessDB) error {
	if !db.Spec.Freeze {
		return nil
	}

	// freeze all tidbClusters associated with current sldb.
	selector := labels.NewSelector().Add(*util.LabelEq(util.BcRdsInstanceLabelKey, db.Name))

	list, err := m.deps.TiDBClusterLister.TidbClusters(db.Namespace).List(selector)
	if err != nil {
		return err
	}

	for _, tc := range list {
		if tc.Spec.TiDB.Replicas <= 0 {
			continue
		}
		tc.Spec.TiDB.Replicas = 0
		if _, err := m.deps.PingcapClientset.PingcapV1alpha1().TidbClusters(db.Namespace).Update(tc); err != nil {
			m.recordErr(db, fmt.Errorf("tc [%s.%s] freezing error: %v", tc.Name, tc.Namespace, err))
			return err
		} else {
			m.recordStr(db, fmt.Sprintf("tc [%s.%s] has been frozen", tc.Name, tc.Namespace))
		}
	}

	return nil
}

func (m *sldbFreezeMemberManager) recordStr(db *v1alpha1.ServerlessDB, str string) {
	m.deps.Controls.ServerlessControl.RecordServerlessDBEvent("Freeze", str, db, nil)
}

func (m *sldbFreezeMemberManager) recordErr(db *v1alpha1.ServerlessDB, err error) {
	m.deps.Controls.ServerlessControl.RecordServerlessDBEvent("Freeze", err.Error(), db, err)
}
