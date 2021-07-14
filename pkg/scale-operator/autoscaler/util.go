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

package autoscaler

import (
	"fmt"
	appsv1 "github.com/pingcap/advanced-statefulset/client/apis/apps/v1"
	"github.com/pingcap/tidb-operator/pkg/apis/pingcap/v1alpha1"
	"github.com/tidb-incubator/Serverlessdb-for-HTAP/pkg/scale-operator/utils"
	sldbv1 "github.com/tidb-incubator/Serverlessdb-for-HTAP/pkg/sldb-operator/apis/bcrds/v1alpha1"
	"os"
)

var prometheusAdress = os.Getenv("PROMETHEUS_ADRESS")

// checkStsAutoScalingPrerequisites would check the sts status to ensure wouldn't happen during
// upgrading, scaling
func checkStsAutoScalingPrerequisites(set *appsv1.StatefulSet) bool {
	return (set.Status.CurrentRevision == set.Status.UpdateRevision) && (set.Status.Replicas == *set.Spec.Replicas)
}

// checkAutoScalingPrerequisites would check the tidbcluster status to ensure the autoscaling would'n happen during
// upgrading, scaling and syncing
func checkAutoScalingPrerequisites(tc *v1alpha1.TidbCluster, sts *appsv1.StatefulSet, memberType v1alpha1.MemberType) bool {
	if !checkStsAutoScalingPrerequisites(sts) {
		return false
	}
	switch memberType {
	case v1alpha1.TiDBMemberType:
		return tc.Status.TiDB.Phase == v1alpha1.NormalPhase
	case v1alpha1.TiKVMemberType:
		return tc.Status.TiKV.Synced && tc.Status.TiKV.Phase == v1alpha1.NormalPhase
	default:
		// Unknown MemberType
		return false
	}
}

// limitTargetHashrate would limit the calculated target hashrate to ensure the min/max hashrate
func limitTargetHashrate(targetHashrate float64, sldb *sldbv1.ServerlessDB, memberType v1alpha1.MemberType) float64 {
	var min, max float64
	//max limit call func get
	hashrate := utils.CalcMaxPerfResource(sldb.Spec.MaxValue.Metric)
	maxHashrate := float64(hashrate)
	//maxHashrate,_ := strconv.ParseFloat(utils.MaxHashrate,64)
	switch memberType {
	case v1alpha1.TiDBMemberType:
		min, max = 0.5, maxHashrate
	default:
		return targetHashrate
	}
	if utils.FGreater(targetHashrate, max) == true {
		targetHashrate = maxHashrate
	}
	if utils.FSmaller(targetHashrate, min) == true {
		targetHashrate = min
	}
	return targetHashrate
}

// If the minReplicas not set, the default value would be 1
// If the Metrics not set, the default metric will be set to 80% average CPU utilization.
// defaultTAC would default the omitted value
func defaultTAC(sldb *sldbv1.ServerlessDB) {
	if sldb.Annotations == nil {
		sldb.Annotations = map[string]string{}
	}
}

func genMetricsEndpoint(sldb *sldbv1.ServerlessDB) (string, error) {
	if prometheusAdress != "" {
		return prometheusAdress, nil
	}
	return fmt.Sprintf("http://%s-prometheus.%s.svc:9090", sldb.Name, sldb.Namespace), nil
}
