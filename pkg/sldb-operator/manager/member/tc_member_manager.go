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
	pclabel "github.com/pingcap/tidb-operator/pkg/label"
	corev1 "k8s.io/api/core/v1"
	apiequality "k8s.io/apimachinery/pkg/api/equality"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/klog"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"strconv"
	"time"

	"github.com/tidb-incubator/Serverlessdb-for-HTAP/pkg/sldb-operator/apis/bcrds/v1alpha1"
	"github.com/tidb-incubator/Serverlessdb-for-HTAP/pkg/sldb-operator/controller"
	"github.com/tidb-incubator/Serverlessdb-for-HTAP/pkg/sldb-operator/manager"
	"github.com/tidb-incubator/Serverlessdb-for-HTAP/pkg/sldb-operator/scheme"
	"github.com/tidb-incubator/Serverlessdb-for-HTAP/pkg/sldb-operator/util"
)

const (
	defaultTiDBVersion          = "v5.1.1"
	defaultSchedulerName        = "tidb-scheduler"
	defaultTidbInitializerImage = "tnir/mysqlclient"
	RoleInstanceLabelKey string = "bcrds.cmss.com/role"
)

type tcMemberManager struct {
	deps *controller.Dependencies
}

// NewTCMemberManager returns a *tcMemberManager
func NewTCMemberManager(dependencies *controller.Dependencies) manager.Manager {
	return &tcMemberManager{
		deps: dependencies,
	}
}

func (m *tcMemberManager) Sync(db *v1alpha1.ServerlessDB) error {
	// todo: tidbcluster sync, do sth for tidbcluster
	// 1. create tidb cluster
	if err := m.syncTidbCluster(db); err != nil {
		return err
	}

	// 2. create tidb monitor
	if err := m.syncTidbMonitor(db); err != nil {
		return err
	}

	// 3. create secret
	if err := m.syncSecret(db); err != nil {
		return err
	}

	// 4. create TidbInitializer
	if err := m.syncTidbInitializer(db); err != nil {
		return err
	}

	// todo: load balance todo
	// 5. update node port
	if len(db.Status.NodePort) < 1 {
		if db.Status.NodePort == nil {
			db.Status.NodePort = map[v1alpha1.PortName]int32{}
		}
		// get proxy port
		if tb, err := m.deps.ServiceLister.Services(db.Namespace).Get(util.GetProxyResourceName(db.Name)); err == nil {
			for _, port := range tb.Spec.Ports {
				if port.Name == "mysql-client" && port.NodePort != 0 {
					db.Status.NodePort[v1alpha1.TiDBServer] = port.NodePort
				}
			}
		}
	}

	return nil
}

func (m *tcMemberManager) getDefaultStorageClassName(db *v1alpha1.ServerlessDB) *string {
	storageClass := ""
	if db.Spec.MaxValue.Component.StorageClassName != nil {
		storageClass = *db.Spec.MaxValue.Component.StorageClassName
	} else {
		storageClass = m.deps.CLIConfig.Config.Storage.StorageClassName
	}
	return &storageClass
}

// new tidb cluster for sldb
func (m *tcMemberManager) getNewTidbClusterForServerlessDB(db *v1alpha1.ServerlessDB) (*pcv1alpha1.TidbCluster, error) {
	calKVSize := func() string {
		maxSize := db.Spec.MaxValue.Component.StorageSize
		maxValueStorageSize := resource.MustParse(maxSize)
		tmSize, _ := maxValueStorageSize.AsInt64()
		size := tmSize * 3 / int64(m.deps.CLIConfig.Config.TiDBCluster.TiKV.Replicas)
		return strconv.FormatInt(size>>30, 10) + "Gi"
	}
	tiKVResource := m.deps.CLIConfig.Config.TiDBCluster.TiKV.ResourceRequirements.ParseResource()
	tiKVResource.Limits[corev1.ResourceStorage] = resource.MustParse(calKVSize())
	tiKVResource.Requests[corev1.ResourceStorage] = resource.MustParse(calKVSize())

	annotations := map[string]string{}
	if restartAnnotation, ok := db.Annotations[util.RestartAtAnnotationKey]; ok {
		annotations[util.PingcapRestartAtAnnotationKey] = restartAnnotation
	}

	tidbReplicas := m.deps.CLIConfig.Config.TiDBCluster.TiDB.Replicas
	if db.Spec.Freeze {
		tidbReplicas = 0
	}

	tiKvConfig := pcv1alpha1.NewTiKVConfig()
	tiKvConfig.Set("storage.reserve-space", "0MB")
	pvReclaimPolicy := corev1.PersistentVolumeReclaimDelete
	tc := &pcv1alpha1.TidbCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:      db.Name,
			Namespace: db.Namespace,
			Labels:    util.LabelsForServerlessDB(db.Name),
		},
		Spec: pcv1alpha1.TidbClusterSpec{
			Version:         defaultTiDBVersion,
			PVReclaimPolicy: &pvReclaimPolicy,
			SchedulerName:   defaultSchedulerName,
			//NodeSelector: map[string]string{
			//	nodeSelectorKeyDiskType:     nodeSelectorValueLSSSD,
			//	nodeSelectorKeyServerlessDB: nodeSelectorValueTrue,
			//},
			Annotations: map[string]string{
				util.InstanceAnnotationKey: db.Name,
			},
			ImagePullPolicy: db.Spec.ImagePullPolicy,
			PD: func(*v1alpha1.ServerlessDB) *pcv1alpha1.PDSpec {
				return &pcv1alpha1.PDSpec{
					ComponentSpec: pcv1alpha1.ComponentSpec{
						Annotations: annotations,
						//NodeSelector: map[string]string{
						//	nodeSelectorKeyComponentPD: nodeSelectorValueTrue,
						//},
					},
					ResourceRequirements:         m.deps.CLIConfig.Config.TiDBCluster.PD.ResourceRequirements.ParseResource(),
					ServiceAccount:               "",
					Replicas:                     m.deps.CLIConfig.Config.TiDBCluster.PD.Replicas,
					BaseImage:                    "",
					Service:                      nil,
					MaxFailoverCount:             nil,
					StorageClassName:             m.getDefaultStorageClassName(db),
					StorageVolumes:               nil,
					DataSubDir:                   "",
					Config:                       &pcv1alpha1.PDConfigWraper{},
					TLSClientSecretName:          nil,
					EnableDashboardInternalProxy: nil,
					MountClusterClientSecret:     nil,
				}
			}(db),
			TiDB: func(*v1alpha1.ServerlessDB) *pcv1alpha1.TiDBSpec {
				return &pcv1alpha1.TiDBSpec{
					ComponentSpec: pcv1alpha1.ComponentSpec{
						Annotations: annotations,
						//NodeSelector: map[string]string{
						//	nodeSelectorKeyComponentTiDB: nodeSelectorValueTrue,
						//},
						Labels: map[string]string{
							util.InstanceAnnotationKey: db.Name,
							RoleInstanceLabelKey: "tp",
						},
					},
					ResourceRequirements: m.deps.CLIConfig.Config.TiDBCluster.TiDB.ResourceRequirements.ParseResource(),
					ServiceAccount:       "",
					Replicas:             tidbReplicas,
					BaseImage:            "",
					Service: &pcv1alpha1.TiDBServiceSpec{
						ServiceSpec:           pcv1alpha1.ServiceSpec{Type: corev1.ServiceTypeNodePort},
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
					StorageClassName: m.getDefaultStorageClassName(db),
					ReadinessProbe:   nil,
				}
			}(db),
			TiKV: &pcv1alpha1.TiKVSpec{
				ComponentSpec: pcv1alpha1.ComponentSpec{
					Annotations: annotations,
					//NodeSelector: map[string]string{
					//	nodeSelectorKeyComponentTiKV: nodeSelectorValueTrue,
					//},
				},
				ResourceRequirements:     tiKVResource,
				ServiceAccount:           "",
				Replicas:                 m.deps.CLIConfig.Config.TiDBCluster.TiKV.Replicas,
				BaseImage:                "",
				Privileged:               nil,
				MaxFailoverCount:         nil,
				StorageClassName:         m.getDefaultStorageClassName(db),
				DataSubDir:               "",
				Config:                   tiKvConfig,
				RecoverFailover:          false,
				MountClusterClientSecret: nil,
				EvictLeaderTimeout:       nil,
				StorageVolumes:           nil,
			},
			Paused: db.Spec.Paused,
		},
	}
	if db.Spec.UseTiFlash {
		tc.Spec.TiFlash = &pcv1alpha1.TiFlashSpec{
			ComponentSpec: pcv1alpha1.ComponentSpec{
				Annotations: annotations,
			},
			ResourceRequirements: corev1.ResourceRequirements{},
			ServiceAccount:       "",
			Replicas:             3,
			BaseImage:            "",
			Privileged:           nil,
			MaxFailoverCount:     nil,
			StorageClaims:        nil,
			Config:               nil,
			LogTailer:            nil,
			RecoverFailover:      false,
		}
	}

	// Set ServerlessDB instance as the owner and controller
	if err := controllerutil.SetControllerReference(db, tc, scheme.Scheme); err != nil {
		klog.Errorf("ServerlessDB %s/%s set controller reference failed.", db.Namespace, db.Name)
		return tc, fmt.Errorf("ServerlessDB %s/%s set controller reference failed", db.Namespace, db.Name)
	}
	return tc, nil
}

// new tidb monitor for sldb
func (m *tcMemberManager) getNewTidbMonitorForServerlessDB(db *v1alpha1.ServerlessDB) (*pcv1alpha1.TidbMonitor, error) {
	portName := "http-prometheus"
	PVReclaimPolicy := corev1.PersistentVolumeReclaimDelete
	tm := &pcv1alpha1.TidbMonitor{
		ObjectMeta: metav1.ObjectMeta{
			Name:      db.Name,
			Namespace: db.Namespace,
			Labels:    util.LabelsForServerlessDB(db.Name),
		},
		Spec: pcv1alpha1.TidbMonitorSpec{
			Clusters: []pcv1alpha1.TidbClusterRef{
				{
					Name:      db.Name,
					Namespace: db.Namespace,
				},
			},
			Prometheus: pcv1alpha1.PrometheusSpec{
				MonitorContainer: pcv1alpha1.MonitorContainer{
					BaseImage: "prom/prometheus",
					Version:   "v2.18.1",
				},
				LogLevel: "info",
				Service: pcv1alpha1.ServiceSpec{
					Type:     corev1.ServiceTypeNodePort,
					PortName: &portName,
				},
				ReserveDays: m.deps.CLIConfig.Config.Monitor.PrometheusReserveDays,
			},
			Grafana: &pcv1alpha1.GrafanaSpec{
				MonitorContainer: pcv1alpha1.MonitorContainer{
					BaseImage: "grafana/grafana",
					Version:   "6.1.6",
				},
				Service: pcv1alpha1.ServiceSpec{
					Type: corev1.ServiceTypeNodePort,
				},
			},
			Reloader: pcv1alpha1.ReloaderSpec{
				MonitorContainer: pcv1alpha1.MonitorContainer{
					BaseImage: "registry.cn-beijing.aliyuncs.com/tidb/tidb-monitor-reloader",
					Version:   "v1.0.1",
				},
			},
			Initializer: pcv1alpha1.InitializerSpec{
				MonitorContainer: pcv1alpha1.MonitorContainer{
					BaseImage: "registry.cn-beijing.aliyuncs.com/tidb/tidb-monitor-initializer",
					Version:   "v4.0.9",
				},
			},
			PVReclaimPolicy: &PVReclaimPolicy,
			//StorageClassName: m.getDefaultStorageClassName(db),
			//Storage: m.deps.CLIConfig.Config.Monitor.Storage,
			//Persistent: true,
			ImagePullPolicy: db.Spec.ImagePullPolicy,
		},
	}

	// Set ServerlessDB instance as the owner and controller
	if err := controllerutil.SetControllerReference(db, tm, scheme.Scheme); err != nil {
		klog.Errorf("ServerlessDB %s/%s set controller reference failed.", db.Namespace, db.Name)
		return tm, fmt.Errorf("ServerlessDB %s/%s set controller reference failed", db.Namespace, db.Name)
	}
	return tm, nil
}

// new tidb monitor for sldb
func (m *tcMemberManager) getNewTidbInitializerForServerlessDB(db *v1alpha1.ServerlessDB) (*pcv1alpha1.TidbInitializer, error) {
	ti := &pcv1alpha1.TidbInitializer{
		TypeMeta: metav1.TypeMeta{},
		ObjectMeta: metav1.ObjectMeta{
			Name:      db.Name,
			Namespace: db.Namespace,
			Labels:    util.LabelsForServerlessDB(db.Name),
		},
		Spec: pcv1alpha1.TidbInitializerSpec{
			Image: defaultTidbInitializerImage,
			Clusters: pcv1alpha1.TidbClusterRef{
				Name:      db.Name,
				Namespace: db.Namespace,
			},
			ImagePullPolicy:  &db.Spec.ImagePullPolicy,
			ImagePullSecrets: nil,
			PermitHost:       nil,
			InitSql:          nil,
			InitSqlConfigMap: nil,
			PasswordSecret:   &db.Name,
		},
		Status: pcv1alpha1.TidbInitializerStatus{},
	}
	// Set ServerlessDB instance as the owner and controller
	if err := controllerutil.SetControllerReference(db, ti, scheme.Scheme); err != nil {
		klog.Errorf("ServerlessDB %s/%s set controller reference failed.", db.Namespace, db.Name)
		return ti, fmt.Errorf("ServerlessDB %s/%s set controller reference failed", db.Namespace, db.Name)
	}
	return ti, nil
}

func (m *tcMemberManager) syncSecret(db *v1alpha1.ServerlessDB) error {
	if err := m.newSecretForServerlessDB(db, getRootSecret); err != nil {
		return err
	}
	return nil
}

func (m *tcMemberManager) newSecretForServerlessDB(db *v1alpha1.ServerlessDB, f func(*v1alpha1.ServerlessDB) (secret *corev1.Secret, err error)) error {
	secret, err := f(db)
	if secret == nil {
		return fmt.Errorf("fail to get generate secret for cluster %s/%s, error: secret is nil", db.Namespace, db.Name)
	}
	if err != nil {
		return fmt.Errorf("fail to get generate secret %s for cluster %s/%s, error: %s", secret.Name, db.Namespace, db.Name, err)
	}

	_, err = m.deps.SecretLister.Secrets(db.Namespace).Get(secret.Name)
	if err != nil && !errors.IsNotFound(err) {
		return fmt.Errorf("fail to get create secret %s for cluster %s/%s, error: %s", db.Name, db.Namespace, db.Name, err)
	}
	if errors.IsNotFound(err) {
		if _, err := m.deps.KubeClientset.CoreV1().Secrets(db.Namespace).Create(secret); err != nil {
			message := fmt.Sprintf("Create Secret %s Failed", db.Name)
			m.deps.Controls.ServerlessControl.RecordServerlessDBEvent("Create", message, db, err)
			return err
		}
		message := fmt.Sprintf("Create Secret %s Successful", db.Name)
		m.deps.Controls.ServerlessControl.RecordServerlessDBEvent("Create", message, db, nil)
		return nil
	}
	return nil
}

func (m *tcMemberManager) syncTidbCluster(db *v1alpha1.ServerlessDB) error {
	oldTidbCluster, err := m.deps.TiDBClusterLister.TidbClusters(db.Namespace).Get(db.Name)
	if err != nil && !errors.IsNotFound(err) {
		return fmt.Errorf("fail to get tc %s for cluster %s/%s, error: %s", db.Name, db.Namespace, db.Name, err)
	}
	if errors.IsNotFound(err) {
		tc, err := m.getNewTidbClusterForServerlessDB(db)
		if _, err = m.deps.PingcapClientset.PingcapV1alpha1().TidbClusters(db.Namespace).Create(tc); err != nil {
			message := fmt.Sprintf("Create TidbCluster %s Failed", db.Name)
			m.deps.Controls.ServerlessControl.RecordServerlessDBEvent("Create", message, db, err)
			return err
		}
		message := fmt.Sprintf("Create TidbCluster %s Successful", db.Name)
		m.deps.Controls.ServerlessControl.RecordServerlessDBEvent("Create", message, db, err)
		return util.RequeueErrorf("ServerlessDB: [%s/%s], waiting for tidb cluster running", db.Namespace, db.Name)
	}
	// TODO: update
	newTidb := oldTidbCluster.DeepCopy()

	if err := m.syncTidbClusterRestart(db, newTidb); err != nil {
		return err
	}

	if err := m.updateTidbCluster(db, newTidb); err != nil {
		return err
	}
	return nil
}

func (m *tcMemberManager) updateTidbCluster(db *v1alpha1.ServerlessDB, tidb *pcv1alpha1.TidbCluster) error {
	selector, _ := util.Label{}.BcRdsInstance(db.Name).Selector()
	tcs, err := m.deps.TiDBClusterLister.TidbClusters(db.Namespace).List(selector)
	if err != nil {
		klog.Errorf("List TidbClusters %s/%s faild: %v", db.Namespace, db.Name, err)
		return err
	}

	// update tcs
	for _, tc := range tcs {
		if !m.decideTiDBClusterChanged(db, tc) {
			continue
		}
		if _, err := m.deps.PingcapClientset.PingcapV1alpha1().TidbClusters(db.Namespace).Update(tc); err != nil {
			klog.Errorf("Update TidbClusters %s/%s faild: %v", db.Namespace, db.Name, err)
			return err
		}
	}
	return nil
}

func (m *tcMemberManager) decideTiDBClusterChanged(db *v1alpha1.ServerlessDB, tc *pcv1alpha1.TidbCluster) bool {
	var changed = false
	if db == nil || tc == nil {
		return false
	}
	// 1. restart annotations
	if restartAnnotationDB, ok := db.Annotations[util.RestartAtAnnotationKey]; ok {
		annotations := map[string]string{
			util.PingcapRestartAtAnnotationKey: restartAnnotationDB,
		}

		if tc.Spec.PD != nil {
			if !apiequality.Semantic.DeepEqual(annotations, tc.Spec.PD.Annotations) {
				tc.Spec.PD.Annotations = annotations
				changed = true
			}
		}

		if tc.Spec.TiDB != nil {
			if !apiequality.Semantic.DeepEqual(annotations, tc.Spec.TiDB.Annotations) {
				tc.Spec.TiDB.Annotations = annotations
				changed = true
			}
		}

		if tc.Spec.TiKV != nil {
			if !apiequality.Semantic.DeepEqual(annotations, tc.Spec.TiKV.Annotations) {
				tc.Spec.TiKV.Annotations = annotations
				changed = true
			}
		}

		if tc.Spec.TiFlash != nil {
			if !apiequality.Semantic.DeepEqual(annotations, tc.Spec.TiFlash.Annotations) {
				tc.Spec.TiFlash.Annotations = annotations
				changed = true
			}
		}
	}
	// 2. pause cluster
	if db.Spec.Paused {
		if !tc.Spec.Paused {
			tc.Spec.Paused = true
			changed = true
		}
	}

	return changed
}

func (m *tcMemberManager) syncTidbClusterRestart(db *v1alpha1.ServerlessDB, tc *pcv1alpha1.TidbCluster) error {
	if restartCondition := util.GetServerlessDBCondition(db.Status, v1alpha1.TiDBRestart); restartCondition != nil {
		// Watching each pod is very troublesome, not worth it, it will takes a long time to restart the whole pod
		internal, err := time.ParseDuration(m.deps.CLIConfig.Config.Duration.RestartCheck)
		if err != nil {
			internal, _ = time.ParseDuration("3m")
		}
		if restartCondition.LastUpdateTime.Add(internal).Before(time.Now().Local()) {
			if completed, err := m.IsTiDBRestartCompleted(db); err != nil {
				klog.Errorf("Failed to detect the completion of cluster restart for %s/%s: %v", db.Namespace, db.Name, err)
			} else {
				if completed {
					klog.Infof("Check that the cluster restart is complete for ServerlessDB %s/%s, filter restart condition.", db.Namespace, db.Name)
					db.Status.Conditions = util.FilterOutCondition(db.Status.Conditions, v1alpha1.TiDBRestart)
				} else {
					klog.Warningf("Check that the cluster restart is not complete for ServerlessDB %s/%s", db.Namespace, db.Name)
				}
			}
		}
	}
	return nil
}

func (m *tcMemberManager) IsTiDBRestartCompleted(db *v1alpha1.ServerlessDB) (bool, error) {
	selector, _ := util.Label{}.BcRdsInstance(db.Name).Selector()
	tcs, err := m.deps.TiDBClusterLister.TidbClusters(db.Namespace).List(selector)
	if err != nil {
		klog.Errorf("List TidbClusters %s/%s faild: %v", db.Namespace, db.Name, err)
		return false, err
	}

	restartAnnotation, ok := db.Annotations[util.RestartAtAnnotationKey]
	if !ok {
		klog.Warningf("ServerlessDB %s/%s has no restart annotation", db.Namespace, db.Name)
		return true, nil
	}

	// update tcs
	for _, tc := range tcs {
		selector, _ := util.Label{}.Instance(tc.Name).Selector()
		pods, err := m.deps.PodLister.Pods(db.Namespace).List(selector)
		if err != nil {
			return false, err
		}
		if IsTiDBAllComponentsHealthy(tc) {
			message := fmt.Sprintf("All components are normal for TidbClusters %s/%s when checking restart completed.", tc.Namespace, tc.Name)
			klog.Infof(message)
			m.deps.Controls.ServerlessControl.RecordServerlessDBEvent("CheckStatus", message, db, nil)
			continue
		}
		klog.Warningf("Components of TidbClusters %s/%s not restart well, now check pod whether restart completed.", tc.Namespace, tc.Name)
		pd, tidb, tikv := int32(0), int32(0), int32(0)
		for _, pod := range pods {
			// pod annotation is not newest, pod has not started to restart
			if podRestartAnnotation, _ := pod.Annotations[util.PingcapRestartAtAnnotationKey]; podRestartAnnotation != restartAnnotation {
				continue
			}
			if component, exist := pod.Labels[pclabel.ComponentLabelKey]; exist {
				switch component {
				case pclabel.PDLabelVal:
					if pod.Status.Phase == corev1.PodRunning {
						pd++
					}
				case pclabel.TiKVLabelVal:
					if pod.Status.Phase == corev1.PodRunning {
						tikv++
					}
				case pclabel.TiDBLabelVal:
					if pod.Status.Phase == corev1.PodRunning {
						tidb++
					}
				default:
					klog.Warningf("TiDBCluster %s/%s pod not support component labels [%s]", tc.Namespace, tc.Name, component)
				}
			}
		}
		if tc.Spec.PD != nil && pd < tc.Spec.PD.Replicas {
			message := fmt.Sprintf("Restart Components [%s] of TidbClusters %s/%s not completed.", pclabel.PDLabelVal, tc.Namespace, tc.Name)
			klog.Warningf(message)
			m.deps.Controls.ServerlessControl.RecordServerlessDBEvent("CheckStatus", "CheckRestart", db, fmt.Errorf(message))
			return false, nil
		}
		if tc.Spec.TiKV != nil && tikv < tc.Spec.TiKV.Replicas {
			message := fmt.Sprintf("Restart Components [%s] of TidbClusters %s/%s not completed.", pclabel.TiKVLabelVal, tc.Namespace, tc.Name)
			klog.Warningf(message)
			m.deps.Controls.ServerlessControl.RecordServerlessDBEvent("CheckStatus", "CheckRestart", db, fmt.Errorf(message))
			return false, nil
		}
		if tc.Spec.TiDB != nil && tidb < tc.Spec.TiDB.Replicas {
			message := fmt.Sprintf("Restart Components [%s] of TidbClusters %s/%s not completed.", pclabel.TiDBLabelVal, tc.Namespace, tc.Name)
			klog.Warningf(message)
			m.deps.Controls.ServerlessControl.RecordServerlessDBEvent("CheckStatus", "CheckRestart", db, fmt.Errorf(message))
			return false, nil
		}
	}
	return true, nil
}

func (m *tcMemberManager) syncTidbMonitor(db *v1alpha1.ServerlessDB) error {
	// if enable is false, skipping syn
	if !m.deps.CLIConfig.Config.Monitor.Enable {
		return nil
	}
	_, err := m.deps.TiDBMonitorLister.TidbMonitors(db.Namespace).Get(db.Name)
	if err != nil && !errors.IsNotFound(err) {
		return fmt.Errorf("fail to get tm %s for cluster %s/%s, error: %s", db.Name, db.Namespace, db.Name, err)
	}
	if errors.IsNotFound(err) {
		tm, err := m.getNewTidbMonitorForServerlessDB(db)
		if _, err = m.deps.PingcapClientset.PingcapV1alpha1().TidbMonitors(db.Namespace).Create(tm); err != nil {
			message := fmt.Sprintf("Create TidbMonitor %s Failed", db.Name)
			m.deps.Controls.ServerlessControl.RecordServerlessDBEvent("Create", message, db, err)
			return err
		}
		message := fmt.Sprintf("Create TidbMonitor %s Successful", db.Name)
		m.deps.Controls.ServerlessControl.RecordServerlessDBEvent("Create", message, db, err)
		return util.RequeueErrorf("ServerlessDB: [%s/%s], waiting for tidb monitor running", db.Namespace, db.Name)
	}
	return nil
}

func (m *tcMemberManager) syncTidbInitializer(db *v1alpha1.ServerlessDB) error {
	if db.Status.Phase != v1alpha1.PhaseCreating {
		return nil
	}
	tiGet, err := m.deps.TiDBInitializerLister.TidbInitializers(db.Namespace).Get(db.Name)
	if err != nil && !errors.IsNotFound(err) {
		return fmt.Errorf("fail to get tidbinitializer %s for cluster %s/%s, error: %s", db.Name, db.Namespace, db.Name, err)
	}
	if errors.IsNotFound(err) {
		ti, err := m.getNewTidbInitializerForServerlessDB(db)
		if _, err = m.deps.PingcapClientset.PingcapV1alpha1().TidbInitializers(db.Namespace).Create(ti); err != nil {
			message := fmt.Sprintf("Create TidbInitializer %s Failed", db.Name)
			m.deps.Controls.ServerlessControl.RecordServerlessDBEvent("Create", message, db, err)
			return err
		}
		message := fmt.Sprintf("Create TidbInitializer %s Successful", db.Name)
		m.deps.Controls.ServerlessControl.RecordServerlessDBEvent("Create", message, db, err)
		return util.RequeueErrorf("ServerlessDB: [%s/%s], waiting for tidb TidbInitializer finished", db.Namespace, db.Name)
	}

	// if initializer failed, retry
	if tiGet.Status.Phase == pcv1alpha1.InitializePhaseFailed {
		if err = m.deps.PingcapClientset.PingcapV1alpha1().TidbInitializers(db.Namespace).Delete(tiGet.Name, &metav1.DeleteOptions{}); err != nil {
			return err
		}
		message := fmt.Sprintf("Delete TidbInitializer %s for recreating ti", db.Name)
		m.deps.Controls.ServerlessControl.RecordServerlessDBEvent("Create", message, db, err)
		return util.RequeueErrorf("ServerlessDB: [%s/%s], waiting for TidbInitializer complete", db.Namespace, db.Name)
	}
	if tiGet.Status.Phase == pcv1alpha1.InitializePhaseCompleted {
		db.Status.Conditions = util.FilterOutCondition(db.Status.Conditions, v1alpha1.TiDBInitial)
	}
	return nil
}

func getRootSecret(db *v1alpha1.ServerlessDB) (secret *corev1.Secret, err error) {
	secret = &corev1.Secret{
		TypeMeta: metav1.TypeMeta{},
		ObjectMeta: metav1.ObjectMeta{
			Name:      db.Name,
			Namespace: db.Namespace,
			Labels:    util.LabelsForServerlessDB(db.Name),
		},
		Data: map[string][]byte{
			util.DefaultRootUser: []byte(util.DefaultRootPassword),
		},
	}
	// Set ServerlessDB instance as the owner and controller
	if err := controllerutil.SetControllerReference(db, secret, scheme.Scheme); err != nil {
		klog.Errorf("ServerlessDB %s/%s set controller reference failed.", db.Namespace, db.Name)
		return secret, fmt.Errorf("ServerlessDB %s/%s set controller reference failed", db.Namespace, db.Name)
	}
	return secret, nil
}

var _ manager.Manager = &tcMemberManager{}

type FakeTcMemberManager struct {
	err error
}

func NewFakeTcMemberManager() *FakeTcMemberManager {
	return &FakeTcMemberManager{}
}

func (m *FakeTcMemberManager) SetSyncError(err error) {
	m.err = err
}

func (m *FakeTcMemberManager) Sync(_ *v1alpha1.ServerlessDB) error {
	return m.err
}
