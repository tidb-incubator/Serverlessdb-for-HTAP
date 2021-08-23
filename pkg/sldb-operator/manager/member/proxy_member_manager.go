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
	"github.com/tidb-incubator/Serverlessdb-for-HTAP/pkg/sldb-operator/apis/bcrds/v1alpha1"
	"github.com/tidb-incubator/Serverlessdb-for-HTAP/pkg/sldb-operator/controller"
	"github.com/tidb-incubator/Serverlessdb-for-HTAP/pkg/sldb-operator/manager"
	"github.com/tidb-incubator/Serverlessdb-for-HTAP/pkg/sldb-operator/scheme"
	"github.com/tidb-incubator/Serverlessdb-for-HTAP/pkg/sldb-operator/util"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/klog"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
)


type proxyMemberManager struct {
	deps *controller.Dependencies
}

// NewProxyMemberManager returns a *proxyMemberManager
func NewProxyMemberManager(dependencies *controller.Dependencies) manager.Manager {
	return &proxyMemberManager{
		deps: dependencies,
	}
}

func (m *proxyMemberManager) Sync(db *v1alpha1.ServerlessDB) error {
	if db.Status.Phase == v1alpha1.PhaseAvailable {
		return nil
	}
	if err := m.syncProxyTc(db); err != nil {
		return err
	}
	return nil
}


func (m *proxyMemberManager) syncProxyTc(db *v1alpha1.ServerlessDB) error {
	proxyName := util.GetProxyResourceName(db.Name)
	_, err := m.deps.TiDBClusterLister.TidbClusters(db.Namespace).Get(proxyName)
	if err != nil && !errors.IsNotFound(err) {
		return fmt.Errorf("fail to create Deployments %s for cluster %s/%s, error: %s", proxyName, db.Namespace, db.Name, err)
	}
	if errors.IsNotFound(err) {
		tc, _ := m.getNewProxyTc(db)
		if _, err := m.deps.PingcapClientset.PingcapV1alpha1().TidbClusters(db.Namespace).Create(tc); err != nil {
			message := fmt.Sprintf("Create tc %s Failed", proxyName)
			m.deps.Controls.ServerlessControl.RecordServerlessDBEvent("Create", message, db, err)
			return err
		}
		message := fmt.Sprintf("Create tc %s Successful", proxyName)
		m.deps.Controls.ServerlessControl.RecordServerlessDBEvent("Create", message, db, err)
		return nil
	}
	return nil
}

func (m *proxyMemberManager) getNewProxyTc(db *v1alpha1.ServerlessDB) (*pcv1alpha1.TidbCluster, error) {

	tidbReplicas := m.deps.CLIConfig.Config.Proxy.Replicas
	tcName := util.GetProxyResourceName(db.Name)
	pvReclaimPolicy := v1.PersistentVolumeReclaimDelete
	tc := &pcv1alpha1.TidbCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:      tcName,
			Namespace: db.Namespace,
			Labels:    util.LabelsForServerlessDB(db.Name),
		},
		Spec: pcv1alpha1.TidbClusterSpec{
			Version:         defaultTiDBVersion,
			PVReclaimPolicy: &pvReclaimPolicy,
			SchedulerName:   defaultSchedulerName,
			Annotations: map[string]string{
				util.InstanceAnnotationKey: db.Name,
			},
			ImagePullPolicy: db.Spec.ImagePullPolicy,
			TiDB: func(*v1alpha1.ServerlessDB) *pcv1alpha1.TiDBSpec {
				return &pcv1alpha1.TiDBSpec{
					ComponentSpec: pcv1alpha1.ComponentSpec{
						Image: m.deps.CLIConfig.Config.Proxy.Image,
						Labels: map[string]string{
							util.InstanceAnnotationKey: db.Name,
							RoleInstanceLabelKey: "proxy",
						},
					},
					ResourceRequirements: m.deps.CLIConfig.Config.Proxy.ParseResource(),
					ServiceAccount:       "",
					Replicas:             tidbReplicas,
					BaseImage:            "",
					Service: &pcv1alpha1.TiDBServiceSpec{
						ServiceSpec:           pcv1alpha1.ServiceSpec{Type: v1.ServiceTypeNodePort},
						ExternalTrafficPolicy: nil,
						ExposeStatus:          nil,
						MySQLNodePort:         nil,
						StatusNodePort:        nil,
						AdditionalPorts:       nil,
					},
					BinlogEnabled:    nil,
					MaxFailoverCount: nil,
					SeparateSlowLog:  nil,
					SlowLogTailer:    nil,
					TLSClient:        nil,
					Plugins:          nil,
					Config:           &pcv1alpha1.TiDBConfigWraper{},
					Lifecycle:        nil,
					StorageVolumes:   nil,
					ReadinessProbe:   nil,
				}
			}(db),
			Paused: db.Spec.Paused,
			Cluster: &pcv1alpha1.TidbClusterRef{
				Namespace: db.Namespace,
				Name:      db.Name,
			},
		},
	}

	// Set ServerlessDB instance as the owner and controller
	if err := controllerutil.SetControllerReference(db, tc, scheme.Scheme); err != nil {
		klog.Errorf("ServerlessDB %s/%s set controller reference failed.", db.Namespace, db.Name)
		return tc, fmt.Errorf("ServerlessDB %s/%s set controller reference failed", db.Namespace, db.Name)
	}
	return tc, nil
}