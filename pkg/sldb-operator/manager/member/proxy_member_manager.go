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
	"github.com/tidb-incubator/Serverlessdb-for-HTAP/pkg/sldb-operator/scheme"
	"github.com/tidb-incubator/Serverlessdb-for-HTAP/pkg/sldb-operator/util"
	appsv1 "k8s.io/api/apps/v1"
	v1 "k8s.io/api/core/v1"
	"k8s.io/api/rbac/v1beta1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/klog"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
)

const (
	ComponentProxy = "he3proxy"
	ProxyConfData  = `
addr: 0.0.0.0:9696
prometheus_addr: 0.0.0.0:81
user_list:
- user: root
  password: Nz_2@sMw6R
web_addr: 0.0.0.0:9797
web_user: admin
web_password: admin
read_only: "off"
max_conns_limit: 2000
log_level: debug
log_sql: "on"
log_audit_sql: "on"
slow_log_time: 300
allow_ips: ""
blacklist_sql_file: ""
proxy_charset: utf8
silent_period: %s
clusters:
- name: %s
  namespace: %s
  down_after_noalive: 300
  max_conns_limit: 16
  user: root
  password: "Nz_2@sMw6R"
  tidbs: 10.254.10.27:31206@2
`
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
	if err := m.syncClusterRole(db); err != nil {
		return err
	}
	if err := m.syncServiceAccount(db); err != nil {
		return err
	}
	if err := m.syncClusterRoleBinding(db); err != nil {
		return err
	}
	if err := m.syncService(db); err != nil {
		return err
	}
	if err := m.syncConfigMap(db); err != nil {
		return err
	}
	if err := m.syncDeployment(db); err != nil {
		return err
	}
	return nil
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

func (m *proxyMemberManager) syncService(db *v1alpha1.ServerlessDB) error {
	svcName := util.GetProxyResourceName(db.Name)
	_, err := m.deps.ServiceLister.Services(db.Namespace).Get(svcName)
	if err != nil && !errors.IsNotFound(err) {
		return fmt.Errorf("fail to get svc %s for cluster %s/%s, error: %s", svcName, db.Namespace, db.Name, err)
	}
	if errors.IsNotFound(err) {
		svc, err := getNewService(db)
		if _, err = m.deps.KubeClientset.CoreV1().Services(db.Namespace).Create(svc); err != nil {
			message := fmt.Sprintf("Create Services %s Failed", svcName)
			m.deps.Controls.ServerlessControl.RecordServerlessDBEvent("Create", message, db, err)
			return err
		}
		message := fmt.Sprintf("Create Services %s Successful", svcName)
		m.deps.Controls.ServerlessControl.RecordServerlessDBEvent("Create", message, db, err)
		return nil
	}
	return nil
}

func (m *proxyMemberManager) syncConfigMap(db *v1alpha1.ServerlessDB) error {
	cm, _ := getNewConfigMap(db)
	_, err := m.deps.KubeClientset.CoreV1().ConfigMaps(db.Namespace).Create(cm)
	if err != nil && !errors.IsAlreadyExists(err) {
		return fmt.Errorf("fail to create ConfigMaps %s for cluster %s/%s, error: %s", db.Name, db.Namespace, db.Name, err)
	}
	return nil
}

func (m *proxyMemberManager) syncDeployment(db *v1alpha1.ServerlessDB) error {
	svcName := util.GetProxyResourceName(db.Name)
	_, err := m.deps.DeploymentLister.Deployments(db.Namespace).Get(svcName)
	if err != nil && !errors.IsNotFound(err) {
		return fmt.Errorf("fail to create Deployments %s for cluster %s/%s, error: %s", svcName, db.Namespace, db.Name, err)
	}
	if errors.IsNotFound(err) {
		deploy, _ := m.getNewProxyDeployment(db)
		if _, err := m.deps.KubeClientset.AppsV1().Deployments(db.Namespace).Create(deploy); err != nil {
			message := fmt.Sprintf("Create Deployments %s Failed", svcName)
			m.deps.Controls.ServerlessControl.RecordServerlessDBEvent("Create", message, db, err)
			return err
		}
		message := fmt.Sprintf("Create Deployments %s Successful", svcName)
		m.deps.Controls.ServerlessControl.RecordServerlessDBEvent("Create", message, db, err)
		return nil
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

func getNewService(db *v1alpha1.ServerlessDB) (*v1.Service, error) {
	svc := &v1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: db.Namespace,
			Name:      util.GetProxyResourceName(db.Name),
			Labels:    util.LabelsComponentForServerlessDB(db.Name, ComponentProxy),
		},
		Spec: v1.ServiceSpec{
			ExternalTrafficPolicy: v1.ServiceExternalTrafficPolicyTypeCluster,
			SessionAffinity:       v1.ServiceAffinityClientIP,
			Selector:              util.LabelsComponentForServerlessDB(db.Name, ComponentProxy),
			Type:                  v1.ServiceTypeNodePort,
			Ports: []v1.ServicePort{
				{
					Name: "database",
					Port: 9696,
					TargetPort: intstr.IntOrString{
						Type:   intstr.Int,
						IntVal: 9696,
					},
				},
				{
					Name: "proxy-api",
					Port: 9797,
					TargetPort: intstr.IntOrString{
						Type:   intstr.Int,
						IntVal: 9797,
					},
				},
			},
		},
	}

	// Set ServerlessDB instance as the owner and controller
	if err := controllerutil.SetControllerReference(db, svc, scheme.Scheme); err != nil {
		klog.Errorf("ServerlessDB %s/%s set controller reference failed.", db.Namespace, db.Name)
		return svc, fmt.Errorf("ServerlessDB %s/%s set controller reference failed", db.Namespace, db.Name)
	}
	return svc, nil
}

func getNewConfigMap(db *v1alpha1.ServerlessDB) (*v1.ConfigMap, error) {
	cm := &v1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: db.Namespace,
			Name:      util.GetProxyResourceName(db.Name),
			Labels:    util.LabelsComponentForServerlessDB(db.Name, ComponentProxy),
		},
		Data: map[string]string{
			ComponentProxy: getConfigMapData(db),
		},
	}
	// Set ServerlessDB instance as the owner and controller
	if err := controllerutil.SetControllerReference(db, cm, scheme.Scheme); err != nil {
		klog.Errorf("ServerlessDB %s/%s set controller reference failed.", db.Namespace, db.Name)
		return cm, fmt.Errorf("ServerlessDB %s/%s set controller reference failed", db.Namespace, db.Name)
	}
	return cm, nil
}

func (m *proxyMemberManager) getNewProxyDeployment(db *v1alpha1.ServerlessDB) (*appsv1.Deployment, error) {
	deploy := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: db.Namespace,
			Name:      util.GetProxyResourceName(db.Name),
			Labels:    util.LabelsComponentForServerlessDB(db.Name, ComponentProxy),
		},
		Spec: appsv1.DeploymentSpec{
			Replicas: &m.deps.CLIConfig.Config.Proxy.Replicas,
			Selector: &metav1.LabelSelector{MatchLabels: util.LabelsComponentForServerlessDB(db.Name, ComponentProxy)},
			Template: v1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: util.LabelsComponentForServerlessDB(db.Name, ComponentProxy),
					Annotations: map[string]string{
						"prometheus.io/scrape": "true",
						"prometheus.io/path":   "/metrics",
						"prometheus.io/port":   "81",
					},
				},
				Spec: v1.PodSpec{
					//NodeSelector: map[string]string{
					//	nodeSelectorKeyDiskType:     nodeSelectorValueLSSSD,
					//	nodeSelectorKeyServerlessDB: nodeSelectorValueTrue,
					//},
					Containers: []v1.Container{
						{
							Name:            ComponentProxy,
							Image:           m.deps.CLIConfig.Config.Proxy.Image,
							ImagePullPolicy: db.Spec.ImagePullPolicy,
							Command:         []string{"he3proxy"},
							Args:            []string{"--config=/etc/he3proxy/ks.yaml"},
							EnvFrom:         nil,
							Env: []v1.EnvVar{
								{
									Name:  "ROOT_PASSWORD",
									Value: "",
									ValueFrom: &v1.EnvVarSource{
										SecretKeyRef: &v1.SecretKeySelector{
											LocalObjectReference: v1.LocalObjectReference{
												// password secret for serverlessDB
												Name: db.Name,
											},
											Key: util.DefaultRootUser,
										},
									},
								},
							},
							Resources: m.deps.CLIConfig.Config.Proxy.ResourceRequirements.ParseResource(),
							VolumeMounts: []v1.VolumeMount{
								{
									Name:      "he3proxy-volume",
									ReadOnly:  false,
									MountPath: "/etc/he3proxy",
								},
								{
									Name:      "he3proxy-log-volume",
									MountPath: "/var/log/he3proxy",
								},
							},
						},
					},
					ServiceAccountName: util.GetProxyResourceName(db.Name),
					Volumes: []v1.Volume{
						{
							Name: "he3proxy-volume",
							VolumeSource: v1.VolumeSource{
								ConfigMap: &v1.ConfigMapVolumeSource{
									LocalObjectReference: v1.LocalObjectReference{
										Name: util.GetProxyResourceName(db.Name),
									},
									Items: []v1.KeyToPath{
										{
											Key:  ComponentProxy,
											Path: "ks.yaml",
										},
									},
								},
							},
						},
						{
							Name: "he3proxy-log-volume",
							VolumeSource: v1.VolumeSource{
								EmptyDir: &v1.EmptyDirVolumeSource{},
							},
						},
					},
				},
			},
		},
	}
	// Set ServerlessDB instance as the owner and controller
	if err := controllerutil.SetControllerReference(db, deploy, scheme.Scheme); err != nil {
		klog.Errorf("ServerlessDB %s/%s set controller reference failed.", db.Namespace, db.Name)
		return deploy, fmt.Errorf("ServerlessDB %s/%s set controller reference failed", db.Namespace, db.Name)
	}
	return deploy, nil
}

func getConfigMapData(db *v1alpha1.ServerlessDB) string {
	silentPeriod := db.Spec.Proxy.SilentPeriod
	if silentPeriod == "" {
		silentPeriod = "0"
	}
	return fmt.Sprintf(ProxyConfData, silentPeriod, db.Name, db.Namespace)
}
