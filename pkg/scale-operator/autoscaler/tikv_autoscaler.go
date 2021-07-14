package autoscaler

import (
	"fmt"
	appsv1 "github.com/pingcap/advanced-statefulset/client/apis/apps/v1"
	"github.com/pingcap/tidb-operator/pkg/apis/pingcap/v1alpha1"
	"github.com/pingcap/tidb-operator/pkg/label"
	operatorUtils "github.com/pingcap/tidb-operator/pkg/util"
	promClient "github.com/prometheus/client_golang/api"
	"github.com/tidb-incubator/Serverlessdb-for-HTAP/pkg/scale-operator/autoscaler/calculate"
	"github.com/tidb-incubator/Serverlessdb-for-HTAP/pkg/scale-operator/utils"
	sldbv1 "github.com/tidb-incubator/Serverlessdb-for-HTAP/pkg/sldb-operator/apis/bcrds/v1alpha1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/klog"
	"os"
	"strconv"
	"time"
)

var tikv_check_interval = os.Getenv("TIKV_CHECK_INTERVAL")

func (am *AutoScalerManager) GetStorageSize(sldb *sldbv1.ServerlessDB, instances []string) (uint64, error) {
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

func getTikvStatefulSets(am *AutoScalerManager, tc *v1alpha1.TidbCluster) (*appsv1.StatefulSet, error) {
	return am.Sldbclient.AsCli.AppsV1().StatefulSets(tc.Namespace).Get(operatorUtils.GetStatefulSetName(tc, v1alpha1.TiKVMemberType), metav1.GetOptions{})
}

func (am *AutoScalerManager) SyncTiKV(tc *v1alpha1.TidbCluster, sldb *sldbv1.ServerlessDB) error {

	sts, err := getTikvStatefulSets(am, tc)
	if err != nil {
		return err
	}
	if !checkAutoScalingPrerequisites(tc, sts, v1alpha1.TiKVMemberType) {
		return nil
	}
	instances := filterTiKVInstances(tc)
	return am.calculateTiKVMetrics(sldb, tc, sts, instances, am.Sldbclient.KubeCli)
}

//TODO: fetch tikv instances info from pdapi in future
func filterTiKVInstances(tc *v1alpha1.TidbCluster) []string {
	var instances []string
	for _, store := range tc.Status.TiKV.Stores {
		if store.State == v1alpha1.TiKVStateUp {
			instances = append(instances, store.PodName)
		}
	}
	return instances
}

func (am *AutoScalerManager) calculateTiKVMetrics(sldb *sldbv1.ServerlessDB, tc *v1alpha1.TidbCluster, sts *appsv1.StatefulSet, instances []string, kubecli kubernetes.Interface) error {
	ep, err := genMetricsEndpoint(sldb)
	if err != nil {
		return err
	}
	client, err := promClient.NewClient(promClient.Config{Address: ep})
	if err != nil {
		return err
	}
	// check storage
	now := time.Now().Unix()
	capacitySq := &calculate.SingleQuery{
		Endpoint:  ep,
		Timestamp: now,
		Instances: instances,
		Query:     fmt.Sprintf(calculate.TikvSumStorageMetricsPressurePattern, sldb.Name, "capacity", sldb.Namespace),
	}
	availableSq := &calculate.SingleQuery{
		Endpoint:  ep,
		Timestamp: now,
		Instances: instances,
		Query:     fmt.Sprintf(calculate.TikvSumStorageMetricsPressurePattern, sldb.Name, "available", sldb.Namespace),
	}
	return am.calculateTiKVStorageMetrics(sldb, tc, capacitySq, availableSq, client)
}

// checkTiKVAutoScalingInterval check the each 2 auto-scaling interval depends on the scaling-in and scaling-out
// Note that for the storage scaling, we will check scale-out interval before we start to scraping metrics,
// and for the cpu scaling, we will check scale-in/scale-out interval after we finish calculating metrics.
func checkTiKVAutoScalingInterval(sldb *sldbv1.ServerlessDB, intervalSeconds int32) (bool, error) {
	if sldb.Annotations == nil {
		sldb.Annotations = map[string]string{}
	}
	ableToScale, err := utils.CheckStsAutoScalingInterval(sldb, intervalSeconds, v1alpha1.TiKVMemberType)
	if err != nil {
		return false, err
	}
	if !ableToScale {
		return false, nil
	}
	return true, nil
}

func (am *AutoScalerManager) calculateTiKVStorageMetrics(sldb *sldbv1.ServerlessDB, tc *v1alpha1.TidbCluster,
	capSq, avaSq *calculate.SingleQuery, client promClient.Client) error {
	// 20 mintues check,debug 60s
	intervalSeconds := 60
	var err error
	if tikv_check_interval == "" {
		intervalSeconds, err = strconv.Atoi(tikv_check_interval)
		if err != nil {
			return fmt.Errorf("tikv_check_interval Atoi failed %s", tikv_check_interval)
		}
	}
	ableToScale, err := checkTiKVAutoScalingInterval(sldb, int32(intervalSeconds))
	if err != nil {
		return err
	}
	if !ableToScale {
		klog.Infof("[%s/%s]'s tikv won't scale out by storage pressure due to scale-out cool-down interval", sldb.Namespace, sldb.Name)
		return nil
	}
	var storagePressure bool
	storagePressure, err = calculate.CalculateStoragePressureBaseTc(sldb, tc)
	if err != nil {
		return err
	}
	/*
		storagePressure, err = calculate.CalculateWhetherStoragePressure(sldb, capSq, avaSq, client)
		if err != nil {
			return err
		}*/
	if !storagePressure {
		return nil
	}
	currentReplicas := tc.Spec.TiKV.Replicas
	targetReplicas := currentReplicas + 1
	return updateTacIfTiKVScale(tc, sldb, targetReplicas)
}

// updateTacIfTiKVScale update the tac status and syncing annotations if tikv scale-in/out
func updateTacIfTiKVScale(tc *v1alpha1.TidbCluster, sldb *sldbv1.ServerlessDB, recommendedReplicas int32) error {
	sldb.Annotations[label.AnnTiKVLastAutoScalingTimestamp] = fmt.Sprintf("%d", time.Now().Unix())
	tc.Spec.TiKV.Replicas = recommendedReplicas
	return nil
}
