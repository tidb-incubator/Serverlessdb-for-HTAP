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
	"github.com/tidb-incubator/Serverlessdb-for-HTAP/pkg/scale-operator/utils"
	"math"
	"time"

	"github.com/pingcap/tidb-operator/pkg/apis/pingcap/v1alpha1"
	"github.com/tidb-incubator/Serverlessdb-for-HTAP/pkg/scale-operator/autoscaler/calculate"

	appsv1 "github.com/pingcap/advanced-statefulset/client/apis/apps/v1"
	operatorUtils "github.com/pingcap/tidb-operator/pkg/util"
	promClient "github.com/prometheus/client_golang/api"
	sldbv1 "github.com/tidb-incubator/Serverlessdb-for-HTAP/pkg/sldb-operator/apis/bcrds/v1alpha1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/klog"
)

func getTidbStatefulSets(am *AutoScalerManager, tc *utils.SigleTc) (*appsv1.StatefulSet, error) {
	return am.Sldbclient.AsCli.AppsV1().StatefulSets(tc.Tc.Namespace).Get(operatorUtils.GetStatefulSetName(&tc.Tc, v1alpha1.TiDBMemberType), metav1.GetOptions{})
}

func (am *AutoScalerManager) SyncTiDB(tcArr *utils.TClus, sldb *sldbv1.ServerlessDB) error {
	var totalHashRate float64
	var instances []string
	var minNorms bool
	for _, tc := range tcArr.NewTc {
		if tc.Tc.Spec.TiDB.Replicas == 0 {
			continue
		}
		if tc.Tc.Spec.TiDB.Limits.Cpu().String() == "500m" {
			minNorms = true
		}
		sts, err := getTidbStatefulSets(am, &tc)
		if err != nil {
			return err
		}
		if !checkAutoScalingPrerequisites(&tc.Tc, sts, v1alpha1.TiDBMemberType) {
			klog.Infof("[%s/%s] tidb replicas change no completed", tc.Tc.Namespace, tc.Tc.Name)
			return nil
		}
		//cluster1  hashrate
		ins := filterTidbInstances(&tc.Tc)
		instances = append(instances, ins...)
	}
	oldHashRate := tcArr.OldHashRate
	var err error
	totalHashRate, err = calculateTidbMetrics(sldb, instances)
	if err != nil {
		sldb.Name = tcArr.NewTc[0].Tc.Name
		if err.Error() == "promethus wrong" {
			if totalHashRate < oldHashRate {
				totalHashRate = oldHashRate
			}
		} else {
			return err
		}
	}
	if minNorms == true {
		fcur, err := calculate.GetUtilizationRatio()
		if err != nil {
			return err
		}
		//0.5 hashrate handler
		if totalHashRate > fcur {
			if totalHashRate < 0.7 {
				totalHashRate = 0.7
			}
		}
	}
	totalHashRate = limitTargetHashrate(totalHashRate, sldb, v1alpha1.TiDBMemberType)
	tcArr.NewHashRate = totalHashRate
	if utils.FEqual(totalHashRate, oldHashRate) == true {
		return nil
	}
	return syncTiDBAfterCalculated(tcArr, sldb, totalHashRate, oldHashRate)
}

// syncTiDBAfterCalculated would check the Consecutive count to avoid jitter, and it would also check the interval
// duration between each auto-scaling. If either of them is not meet, the auto-scaling would be rejected.
// If the auto-scaling is permitted, the timestamp would be recorded and the Consecutive count would be zeroed.
func syncTiDBAfterCalculated(tcArr *utils.TClus, sldb *sldbv1.ServerlessDB, totalHashRate, oldHashRate float64) error {
	//duration, err := time.ParseDuration(sldb.Spec.ScaleIn)
	_, err := time.ParseDuration(sldb.Spec.ScaleIn)
	if err != nil {
		return err
	}
	//intervalSeconds := duration.Seconds()
	intervalSeconds := float64(1.0)
	if utils.FGreater(totalHashRate, oldHashRate) == true {
		duration, err := time.ParseDuration(sldb.Spec.ScaleOut)
		if err != nil {
			return err
		}
		intervalSeconds = duration.Seconds()
	}
	ableToScale, err := utils.CheckStsAutoScalingInterval(sldb, int32(math.Floor(intervalSeconds)), v1alpha1.TiDBMemberType)
	if err != nil {
		return err
	}
	if !ableToScale {
		return nil
	}
	klog.Infof("[%s/%s] expand current hashrate %v target hashrate %v", sldb.Namespace, sldb.Name, tcArr.OldHashRate, tcArr.NewHashRate)
	return updateTcTiDBIfScale(sldb)
}

// Currently we didnt' record the auto-scaling out slot for tidb, because it is pointless for now.
func updateTcTiDBIfScale(sldb *sldbv1.ServerlessDB) error {
	sldb.Annotations[utils.AnnTiDBLastAutoScalingTimestamp] = fmt.Sprintf("%d", time.Now().Unix())
	return nil
}

var strDuration = "30s"

func calculateTidbMetrics(sldb *sldbv1.ServerlessDB, instances []string) (float64, error) {
	ep, err := genMetricsEndpoint(sldb)
	if err != nil {
		return -1, err
	}
	client, err := promClient.NewClient(promClient.Config{Address: ep})
	if err != nil {
		return -1, err
	}
	duration, err := time.ParseDuration(strDuration)
	if err != nil {
		return -1, err
	}
	//promethus problem
	var errFlag int
	//cpu
	sq := &calculate.SingleQuery{
		Endpoint:  ep,
		Timestamp: time.Now().Unix(),
		Instances: instances,
		Query:     fmt.Sprintf(calculate.TidbSumCpuMetricsPattern, sldb.Name, sldb.Namespace, strDuration),
	}
	rc1, err := calculate.CalculateRecomendedReplicasByCpuCosts(sldb, sq, client, v1alpha1.TiDBMemberType, duration, calculate.CpuType)
	if err != nil {
		errFlag = errFlag - 1
		klog.Errorf("[%s/%s] 's CalculateRecomendedReplicasByCpuCosts got error: %v", sldb.Namespace, sldb.Name, err)
	}

	if errFlag == 0 {
		return rc1, nil
	} else {
		return rc1, fmt.Errorf("promethus wrong")
	}
}

func filterTidbInstances(tc *v1alpha1.TidbCluster) []string {
	var instances []string
	for _, v := range tc.Status.TiDB.Members {
		if _, existed := tc.Status.TiDB.FailureMembers[v.Name]; !existed {
			if v.Health == true {
				instances = append(instances, v.Name)
			}
		}
	}
	return instances
}

func GetStorageSize(sldb *sldbv1.ServerlessDB, instances []string) (uint64, error) {
	ep, err := genMetricsEndpoint(sldb)
	if err != nil {
		return 0, err
	}
	client, err := promClient.NewClient(promClient.Config{Address: ep})
	if err != nil {
		return 0, err
	}
	sq := &calculate.SingleQuery{
		Endpoint:  ep,
		Timestamp: time.Now().Unix(),
		Instances: instances,
		Query:     fmt.Sprintf(calculate.TikvSumStorageMetricsPattern, sldb.Name, sldb.Namespace),
	}
	return calculate.CalculateStorageSize(sldb, sq, client)
}
