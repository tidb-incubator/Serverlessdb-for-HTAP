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
	"k8s.io/api/rbac/v1beta1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/klog"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
)

const ComponentProxy = "proxy"

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
	if err := m.syncClusterRole(db); err != nil {
		return err
	}
	if err := m.syncServiceAccount(db); err != nil {
		return err
	}
	if err := m.syncClusterRoleBinding(db); err != nil {
		return err
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
			Labels:    util.New().Instance(tcName).BcRdsInstance(db.Name),
		},
		Spec: pcv1alpha1.TidbClusterSpec{
			//Version:         defaultTiDBVersion,
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
							RoleInstanceLabelKey: ComponentProxy,
						},
						Env: []v1.EnvVar{{Name: "GOLANG_PROTOBUF_REGISTRATION_CONFLICT", Value: "warn"}},
					},
					ResourceRequirements: m.deps.CLIConfig.Config.Proxy.ParseResource(),
					ServiceAccount:       util.GetProxyResourceName(db.Name),
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

func (m *proxyMemberManager) syncClusterRoleBinding(db *v1alpha1.ServerlessDB) error {
	rb, _ := getNewClusterRoleBinding(db)
	_, err := m.deps.KubeClientset.RbacV1beta1().ClusterRoleBindings().Create(rb)
	if err != nil && !errors.IsAlreadyExists(err) {
		return fmt.Errorf("fail to create RoleBinding %s for cluster %s/%s, error: %s", db.Name, db.Namespace, db.Name, err)
	}
	return nil
}

func (m *proxyMemberManager) syncServiceAccount(db *v1alpha1.ServerlessDB) error {
	sa, _ := getNewServiceAccount(db)
	_, err := m.deps.KubeClientset.CoreV1().ServiceAccounts(db.Namespace).Create(sa)
	if err != nil && !errors.IsAlreadyExists(err) {
		return fmt.Errorf("fail to create ServiceAccount %s for cluster %s/%s, error: %s", db.Name, db.Namespace, db.Name, err)
	}
	return nil
}

func (m *proxyMemberManager) syncClusterRole(db *v1alpha1.ServerlessDB) error {
	cr, _ := getNewClusterRole(db)
	_, err := m.deps.KubeClientset.RbacV1beta1().ClusterRoles().Create(cr)
	if err != nil && !errors.IsAlreadyExists(err) {
		return fmt.Errorf("fail to create ClusterRole %s for cluster %s/%s, error: %s", db.Name, db.Namespace, db.Name, err)
	}
	return nil
}

func getNewClusterRoleBinding(db *v1alpha1.ServerlessDB) (*v1beta1.ClusterRoleBinding, error) {
	roleBinding := &v1beta1.ClusterRoleBinding{
		ObjectMeta: metav1.ObjectMeta{
			Name:      util.GetProxyResourceName(db.Name),
			Namespace: db.Namespace,
			Labels:    util.LabelsComponentForServerlessDB(db.Name, ComponentProxy),
		},
		Subjects: []v1beta1.Subject{
			{
				Kind:      "ServiceAccount",
				Name:      util.GetProxyResourceName(db.Name),
				Namespace: db.Namespace,
			},
		},
		RoleRef: v1beta1.RoleRef{
			APIGroup: "rbac.authorization.k8s.io",
			Kind:     "ClusterRole",
			Name:     util.GetProxyResourceName(db.Name),
		},
	}
	// Set ServerlessDB instance as the owner and controller
	if err := controllerutil.SetControllerReference(db, roleBinding, scheme.Scheme); err != nil {
		klog.Errorf("ServerlessDB %s/%s set controller reference failed.", db.Namespace, db.Name)
		return roleBinding, fmt.Errorf("ServerlessDB %s/%s set controller reference failed", db.Namespace, db.Name)
	}
	return roleBinding, nil
}

func getNewServiceAccount(db *v1alpha1.ServerlessDB) (*v1.ServiceAccount, error) {
	serviceAccount := &v1.ServiceAccount{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: db.Namespace,
			Name:      util.GetProxyResourceName(db.Name),
			Labels:    util.LabelsComponentForServerlessDB(db.Name, ComponentProxy),
		},
	}

	// Set ServerlessDB instance as the owner and controller
	if err := controllerutil.SetControllerReference(db, serviceAccount, scheme.Scheme); err != nil {
		klog.Errorf("ServerlessDB %s/%s set controller reference failed.", db.Namespace, db.Name)
		return serviceAccount, fmt.Errorf("ServerlessDB %s/%s set controller reference failed", db.Namespace, db.Name)
	}
	return serviceAccount, nil
}

func getNewClusterRole(db *v1alpha1.ServerlessDB) (*v1beta1.ClusterRole, error) {
	role := &v1beta1.ClusterRole{
		ObjectMeta: metav1.ObjectMeta{
			Name:      util.GetProxyResourceName(db.Name),
			Namespace: db.Namespace,
			Labels:    util.LabelsComponentForServerlessDB(db.Name, ComponentProxy),
		},
		Rules: []v1beta1.PolicyRule{
			{
				APIGroups: []string{
					"",
				},
				Resources: []string{
					"pods", "namespaces", "services", "configmaps", "nodes", "serviceaccounts", "secrets", "events",
				},
				Verbs: []string{
					"*",
				},
			},
			{
				APIGroups: []string{
					"apps",
				},
				Resources: []string{
					"deployments", "statefulsets",
				},
				Verbs: []string{
					"get", "list", "watch", "create", "update", "patch", "delete",
				},
			},
			{
				APIGroups: []string{
					"bcrds.cmss.com",
				},
				Resources: []string{
					"*",
				},
				Verbs: []string{
					"get", "list", "watch", "create", "update", "patch", "delete",
				},
			},
			{
				APIGroups: []string{
					"pingcap.com",
				},
				Resources: []string{
					"*",
				},
				Verbs: []string{
					"get", "list", "watch", "create", "update", "patch", "delete",
				},
			},
			{
				APIGroups: []string{
					"rbac.authorization.k8s.io",
				},
				Resources: []string{
					"roles", "rolebindings",
				},
				Verbs: []string{
					"get", "list", "watch", "create", "update", "patch", "delete",
				},
			},
		},
	}

	// Set ServerlessDB instance as the owner and controller
	if err := controllerutil.SetControllerReference(db, role, scheme.Scheme); err != nil {
		klog.Errorf("ServerlessDB %s/%s set controller reference failed.", db.Namespace, db.Name)
		return role, fmt.Errorf("ServerlessDB %s/%s set controller reference failed", db.Namespace, db.Name)
	}
	return role, nil
}