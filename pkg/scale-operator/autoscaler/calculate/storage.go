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
	"github.com/pingcap/tidb-operator/pkg/apis/pingcap/v1alpha1"
	promClient "github.com/prometheus/client_golang/api"
	sldbv1 "github.com/tidb-incubator/Serverlessdb-for-HTAP/pkg/sldb-operator/apis/bcrds/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/klog"
	"os"
	"strconv"
	"strings"
)

var tikv_average_util = os.Getenv("TIKV_AVERAGE_UTILIZATION")

const (
	parseError = "[%s/%s] parse size failed,err:%v"
)

func CalculateStorageSize(sldb *sldbv1.ServerlessDB, usedSq *SingleQuery,
	client promClient.Client) (uint64, error) {
	resp := &Response{}
	err := queryMetricsFromPrometheus(sldb, client, usedSq, resp)
	if err != nil {
		return 0, err
	}
	var usedSize uint64
	for _, r := range resp.Data.Result {
		if r.Metric.Cluster == sldb.Name {
			usedSize, err = strconv.ParseUint(r.Value[1].(string), 10, 64)
			if err != nil {
				return 0, fmt.Errorf(parseError, sldb.Namespace, sldb.Name, err)
			}
		}
	}
	return usedSize, nil
}

func CalculateStoragePressureBaseTc(sldb *sldbv1.ServerlessDB, tc *v1alpha1.TidbCluster) (bool, error) {
	if maxUsed, ok := sldb.Status.StorageUsage["TiKVSingleStoreMax"]; ok {
		if tc.Spec.TiKV != nil {
			req := tc.Spec.TiKV.Requests
			var capacitySize int64
			var total resource.Quantity
			var ok bool
			if req != nil {
				if total, ok = req[corev1.ResourceStorage]; ok {
					capacitySize, _ = total.AsInt64()
				}
			}
			used, _ := maxUsed.AsInt64()
			v, err := strconv.Atoi(tikv_average_util)
			if err != nil {
				klog.Infof("[%s/%s] CalculateWhetherStoragePressure tikv_average_util is %v", sldb.Namespace, sldb.Name, tikv_average_util)
				return false, err
			}
			var storagePressure bool
			baselineAvailableSize := (capacitySize * int64(v) / 100)
			if baselineAvailableSize+used > capacitySize {
				storagePressure = true
			}
			klog.Infof("[%s/%s] CalculateWhetherStoragePressure StoragePressure %v,use %s,capaticy %s",
				sldb.Namespace, sldb.Name, storagePressure, maxUsed.String(), total.String())
			return storagePressure, nil
		}
	}
	return false, fmt.Errorf("[%s/%s]TiKVSingleStoreMax no exist", sldb.Namespace, sldb.Name)
}

func CalculateWhetherStoragePressure(sldb *sldbv1.ServerlessDB, capacitySq, availableSq *SingleQuery,
	client promClient.Client) (bool, error) {

	// query total available storage size
	resp := &Response{}
	err := queryMetricsFromPrometheus(sldb, client, availableSq, resp)
	if err != nil {
		return false, err
	}
	var availableSize uint64
	for _, r := range resp.Data.Result {
		if strings.Contains(r.Metric.Instance, sldb.Name+"-tikv") == true {
			var size uint64
			size, err = strconv.ParseUint(r.Value[1].(string), 10, 64)
			if err != nil {
				return false, fmt.Errorf(parseError, sldb.Namespace, sldb.Name, err)
			}
			if size != 0 {
				if availableSize == 0 {
					availableSize = size
				} else if availableSize > size {
					availableSize = size
				}
			}
		}
	}

	// query total capacity storage size
	resp = &Response{}
	err = queryMetricsFromPrometheus(sldb, client, capacitySq, resp)
	if err != nil {
		return false, err
	}
	var capacitySize uint64
	for _, r := range resp.Data.Result {
		if strings.Contains(r.Metric.Instance, sldb.Name+"-tikv") == true {
			var size uint64
			size, err = strconv.ParseUint(r.Value[1].(string), 10, 64)
			if err != nil {
				return false, fmt.Errorf(parseError, sldb.Namespace, sldb.Name, err)
			}
			if size != 0 {
				if capacitySize == 0 {
					capacitySize = size
				} else if capacitySize > size {
					capacitySize = size
				}
			}
		}
	}
	v, err := strconv.Atoi(tikv_average_util)
	if err != nil {
		klog.Infof("[%s/%s] CalculateWhetherStoragePressure tikv_average_util is %v", sldb.Namespace, sldb.Name, tikv_average_util)
		return false, err
	}
	baselineAvailableSize := (capacitySize / 100) * uint64(v)
	storagePressure := false
	if availableSize < baselineAvailableSize {
		storagePressure = true
	}
	//storageMetrics := v1alpha1.StorageMetricsStatus{
	//	StoragePressure:          pointer.BoolPtr(storagePressure),
	//	AvailableStorage:         pointer.StringPtr(humanize.Bytes(availableSize)),
	//	CapacityStorage:          pointer.StringPtr(humanize.Bytes(capacitySize)),
	//	BaselineAvailableStorage: pointer.StringPtr(humanize.Bytes(baselineAvailableSize)),
	//}
	//klog.Infof("[%s/%s] CalculateWhetherStoragePressure StoragePressure %v,AvailableStorage %v,CapacityStorage %v,BaselineAvailableStorage %v",
	//	sldb.Namespace, sldb.Name, *storageMetrics.StoragePressure, *storageMetrics.AvailableStorage, *storageMetrics.CapacityStorage, *storageMetrics.BaselineAvailableStorage)
	return storagePressure, nil
}

// TODO: add unit test
//func isStoragePressureStartTimeRecordAlready(tacStatus v1alpha1.TidbClusterAutoSclaerStatus) bool {
//	if tacStatus.TiKV == nil {
//		return false
//	}
//	if len(tacStatus.TiKV.MetricsStatusList) < 1 {
//		return false
//	}
//	for _, metricsStatus := range tacStatus.TiKV.MetricsStatusList {
//		if metricsStatus.Name == "storage" {
//			if metricsStatus.StoragePressureStartTime != nil {
//				return true
//			}
//		}
//	}
//	return false
//}
