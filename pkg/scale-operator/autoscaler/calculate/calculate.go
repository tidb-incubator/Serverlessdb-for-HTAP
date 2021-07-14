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
	"context"
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"strconv"
	"time"

	promClient "github.com/prometheus/client_golang/api"
	sldbv1 "github.com/tidb-incubator/Serverlessdb-for-HTAP/pkg/sldb-operator/apis/bcrds/v1alpha1"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/klog"
)

const (
	TikvSumStorageMetricsPattern         = `sum(tikv_engine_size_bytes{app_kubernetes_io_instance="%s",app_kubernetes_io_component="tikv",kubernetes_namespace="%s"}) by (app_kubernetes_io_instance)`
	TikvSumStorageMetricsPressurePattern = `sum(tikv_store_size_bytes{app_kubernetes_io_instance="%s",type="%s",app_kubernetes_io_component="tikv",kubernetes_namespace="%s"}) by (statefulset_kubernetes_io_pod_name)`
	TidbSumCpuMetricsPattern             = `sum(increase(process_cpu_seconds_total{app_kubernetes_io_instance=~"%s.*",app_kubernetes_io_component="tidb",kubernetes_namespace="%s"}[%s])) by (statefulset_kubernetes_io_pod_name)`
	TidbSumTpsMetricsPattern             = `sum(increase(tidb_session_transaction_duration_seconds_count{app_kubernetes_io_instance=~"%s.*",app_kubernetes_io_component="tidb",kubernetes_namespace="%s"}[%s])) by (statefulset_kubernetes_io_pod_name)`
	queryPath                            = "/api/v1/query"

	float64EqualityThreshold = 1e-9
	httpRequestTimeout       = 5
)

type SingleQuery struct {
	Endpoint  string
	Timestamp int64
	Query     string
	Instances []string
}

func clientDo(client promClient.Client, req *http.Request) (*http.Response, []byte, error) {
	return client.Do(req.Context(), req)
}

func queryMetricsFromPrometheus(sldb *sldbv1.ServerlessDB, client promClient.Client, sq *SingleQuery, resp *Response) error {
	query := sq.Query
	timestamp := sq.Timestamp

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*httpRequestTimeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, "GET", fmt.Sprintf("%s%s", sq.Endpoint, queryPath), nil)
	if err != nil {
		return err
	}
	q := req.URL.Query()
	q.Add("query", query)
	q.Add("time", fmt.Sprintf("%d", timestamp))
	req.URL.RawQuery = q.Encode()
	r, body, err := clientDo(client, req)
	if err != nil {
		klog.Info(err.Error())
		return err
	}
	if r.StatusCode != http.StatusOK {
		return fmt.Errorf("[%s/%s] query error, status code:%d", sldb.Namespace, sldb.Name, r.StatusCode)
	}
	err = json.Unmarshal(body, resp)
	if err != nil {
		return err
	}
	if resp.Status != statusSuccess {
		return fmt.Errorf("[%s/%s] query error, response status: %v", sldb.Namespace, sldb.Name, resp.Status)
	}
	return nil
}

// sumForEachInstance sum the value in Response of each instance from Prometheus
func sumForEachInstance(instances []string, resp *Response) (float64, error) {
	if resp == nil {
		return 0, fmt.Errorf("metrics response from  can't be empty")
	}
	s := sets.String{}
	for _, instance := range instances {
		s.Insert(instance)
	}
	sum := 0.0
	if len(resp.Data.Result) < 1 {
		return 0, fmt.Errorf("metrics Response return zero info")
	}

	for _, r := range resp.Data.Result {
		if s.Has(r.Metric.Instance) {
			v, err := strconv.ParseFloat(r.Value[1].(string), 64)
			if err != nil {
				return 0.0, err
			}
			sum = sum + v
		}
	}
	return sum, nil
}

// calculate func calculate the recommended replicas by given usageRatio and currentReplicas
func calculate(currentValue float64, targetValue float64) (float64, error) {
	if almostEqual(targetValue, 0.0) {
		return -1, fmt.Errorf("targetValue in calculate func can't be zero")
	}
	usageRatio := currentValue / targetValue
	return usageRatio, nil
}

func almostEqual(a, b float64) bool {
	return math.Abs(a-b) <= float64EqualityThreshold
}
