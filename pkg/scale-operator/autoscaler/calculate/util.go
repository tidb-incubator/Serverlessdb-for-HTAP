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

	appsv1 "github.com/pingcap/advanced-statefulset/client/apis/apps/v1"
	sldbv1 "github.com/tidb-incubator/Serverlessdb-for-HTAP/pkg/sldb-operator/apis/bcrds/v1alpha1"
	corev1 "k8s.io/api/core/v1"
)

// filterContainer is to filter the specific container from the given statefulset(tidb/tikv)
func filterContainer(sldb *sldbv1.ServerlessDB, sts *appsv1.StatefulSet, containerName string) (*corev1.Container, error) {
	for _, c := range sts.Spec.Template.Spec.Containers {
		if c.Name == containerName {
			return &c, nil
		}
	}
	return nil, fmt.Errorf("[%s/%s]'s Target have not %s container", sldb.Namespace, sldb.Name, containerName)
}

const (
	statusSuccess = "success"
)

// Response is used to marshal the data queried from Prometheus
type Response struct {
	Status string `json:"status"`
	Data   Data   `json:"data"`
}

type Data struct {
	ResultType string   `json:"resultType"`
	Result     []Result `json:"result"`
}

type Result struct {
	Metric Metric        `json:"metric"`
	Value  []interface{} `json:"value"`
}

type Metric struct {
	Cluster             string `json:"app_kubernetes_io_instance,omitempty"`
	Instance            string `json:"statefulset_kubernetes_io_pod_name,omitempty"`
	Job                 string `json:"job,omitempty"`
	KubernetesNamespace string `json:"kubernetes_namespace,omitempty"`
}
