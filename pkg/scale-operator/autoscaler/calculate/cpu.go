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

package calculate

import (
	"fmt"
	"strconv"
	"time"

	"github.com/pingcap/tidb-operator/pkg/apis/pingcap/v1alpha1"
	promClient "github.com/prometheus/client_golang/api"

	"github.com/tidb-incubator/Serverlessdb-for-HTAP/pkg/scale-operator/utils"
	sldbv1 "github.com/tidb-incubator/Serverlessdb-for-HTAP/pkg/sldb-operator/apis/bcrds/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/klog"
	"k8s.io/utils/pointer"
	"os"
)

const (
	CpuSumMetricsErrorMsg  = "[%s/%s] cpu sum metrics error, can't calculate the past %s cpu metrics, may caused by prometheus restart while data persistence not enabled"
	TpmcSumMetricsErrorMsg = "[%s/%s] tps sum metrics error, can't calculate the past %s tps metrics, may caused by prometheus restart while data persistence not enabled"
)

var Tpc = os.Getenv("TPMC_PER_CPU")
var AverageUtilization = os.Getenv("CPU_AVERAGE_UTILIZATION")

const (
	TpmcType = 0
	CpuType  = 1
)

func GetUtilizationRatio() (float64, error) {
	aver, err := strconv.Atoi(AverageUtilization)
	if err != nil {
		return 0.0, fmt.Errorf("AverageUtilization is not int")
	}
	utilizationRatio := float64(aver) / 100.0
	return utilizationRatio, nil
}

//TODO: create issue to explain how auto-scaling algorithm based on cpu metrics work
func CalculateRecomendedReplicasByCpuCosts(sldb *sldbv1.ServerlessDB, sq *SingleQuery,
	client promClient.Client, memberType v1alpha1.MemberType, duration time.Duration, isCpuOrTpmc int) (float64, error) {
	instances := sq.Instances
	var err error
	r := &Response{}
	err = queryMetricsFromPrometheus(sldb, client, sq, r)
	if err != nil {
		return -1, err
	}
	sum, err := sumForEachInstance(instances, r)
	if err != nil {
		return -1, err
	}
	var hrate float64
	if sum < 0 {
		return -1, fmt.Errorf(CpuSumMetricsErrorMsg, sldb.Namespace, sldb.Name, duration.String())
	}
	cpuSecsTotal := sum
	durationSeconds := duration.Seconds()
	utilizationRatio, err := GetUtilizationRatio()
	if err != nil {
		return -1, err
	}
	expectedCpuSecsTotal := durationSeconds * utilizationRatio
	rc, err := calculate(cpuSecsTotal, expectedCpuSecsTotal)
	if err != nil {
		return -1, err
	}
	metrics := v1alpha1.MetricsStatus{
		Name:           string(corev1.ResourceCPU),
		CurrentValue:   pointer.StringPtr(fmt.Sprintf("%v", cpuSecsTotal)),
		ThresholdValue: pointer.StringPtr(fmt.Sprintf("%v", expectedCpuSecsTotal)),
	}
	hrate = rc * float64(utils.HashratePerTidb)
	klog.Infof("[%s/%s]  type %d,metrics CurrentValue %v ThresholdValue %v,expectHashrate %v",
		sldb.Namespace, sldb.Name, isCpuOrTpmc, *metrics.CurrentValue, *metrics.ThresholdValue, hrate)

	return hrate, nil
}

func extractCpuRequestsRatio(c *corev1.Container) (float64, error) {
	if c.Resources.Requests.Cpu() == nil || c.Resources.Requests.Cpu().MilliValue() < 1 {
		return 0, fmt.Errorf("container[%s] cpu requests is empty", c.Name)
	}
	return float64(c.Resources.Requests.Cpu().MilliValue()) / 1000.0, nil
}
