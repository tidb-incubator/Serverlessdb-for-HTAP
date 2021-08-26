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
	"github.com/pingcap/tidb-operator/pkg/apis/pingcap/v1alpha1"
	slcluster "github.com/tidb-incubator/Serverlessdb-for-HTAP/pkg/scale-operator/sldbcluster"
	"github.com/tidb-incubator/Serverlessdb-for-HTAP/pkg/scale-operator/utils"
	"math"
	"os"
	"strconv"
	//slcluster "github.com/tidb-incubator/Serverlessdb-for-HTAP/pkg/scale-operator/utils"
	sldbv1 "github.com/tidb-incubator/Serverlessdb-for-HTAP/pkg/sldb-operator/apis/bcrds/v1alpha1"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/client-go/util/retry"
	"k8s.io/klog"
	"time"
)

type AutoScalerManager struct {
	Sldbclient *slcluster.SlabInterface
}

type AutoScalerAPI interface {
	SCalerOutInHandler(sldb *sldbv1.ServerlessDB) (bool, error)
	TiKVSCalerHandler(sldb *sldbv1.ServerlessDB) (bool, error)
	ScalerBaseOnMidWareTP(sldb *sldbv1.ServerlessDB) (bool,error)
	ScalerBaseOnMidWareAP(sldb *sldbv1.ServerlessDB) (bool,error)
}

func NewAutoScalerAPI() AutoScalerAPI {
	return &AutoScalerManager{
		Sldbclient: &slcluster.SldbClient,
	}
}

func (am *AutoScalerManager) SCalerOutInHandler(sldb *sldbv1.ServerlessDB) (bool, error) {
	return true, am.autoScalerHander(sldb, v1alpha1.TiDBMemberType)
}

func (am *AutoScalerManager) TiKVSCalerHandler(sldb *sldbv1.ServerlessDB) (bool, error) {
	return true, am.autoScalerHander(sldb, v1alpha1.TiKVMemberType)
}

func (am *AutoScalerManager) ScalerBaseOnMidWareAP(sldb *sldbv1.ServerlessDB) (bool,error) {
	//get all tc
	tclus,tc,err := utils.CloneMutiRevsionTc(sldb,utils.AP)
	if tclus == nil {
		klog.Infof("[%s/%s] CloneMutiRevsionTc problem err %v", sldb.Namespace, sldb.Name, err)
		return false, fmt.Errorf("[%s/%s] CloneMutiRevsionTc problem err %v",sldb.Namespace, sldb.Name,err)
	}
	// check AP tc status
	if tclus.OldTc[0].Tc.Status.TiDB.Phase != v1alpha1.NormalPhase || tc.Status.TiKV.Phase != v1alpha1.NormalPhase {
		return false, fmt.Errorf("[%s/%s] AP tc is no normal",sldb.Namespace,sldb.Name)
	}
	//get proxy hashrate
	v,err := utils.GetLastData(sldb.Name+"-"+utils.AP,sldb.Namespace)
	if err != nil {
		return false,err
	}
	klog.Infof("[%s/%s] ScalerBaseOnMidWareAP %v,%v",sldb.Namespace,sldb.Name,v)
	if v.ScalerFlag == utils.ScalerIn {
		if tclus.NewHashRate < v.ScalerNeedCore {
			tclus.NewHashRate = v.ScalerNeedCore
		}
		utils.ChangeScalerStatus(sldb.Name+"-"+utils.AP,sldb.Namespace)
	} else {
		//正常扩缩
		if v.ScalerNeedCore > tclus.NewHashRate {
			tclus.NewHashRate = v.ScalerNeedCore
		}
	}
	// AP have at least one replicas,promit expand only tclus.NewHashRate greater than
	//one HashRate of replicas
	if tclus.OldTc[0].Replicas == 1 {
		if tclus.NewHashRate <= tclus.OldHashRate {
			return true,nil
		}
	}
	if err := am.SyncTidbClusterReplicas(sldb, tclus, v1alpha1.TiDBMemberType); err != nil {
		return false,err
	}
	return true,nil
}

func (am *AutoScalerManager) ScalerBaseOnMidWareTP(sldb *sldbv1.ServerlessDB) (bool,error) {
	tclus,_ := utils.ClusterStatusCheck(sldb,v1alpha1.TiDBMemberType)
	if tclus == nil {
		return false,fmt.Errorf("[%s/%s] ScalerBaseOnMidWareTP clusterStatusCheck problem",sldb.Namespace, sldb.Name)
	}
	v,err := utils.GetLastData(sldb.Name+"-"+utils.TP,sldb.Namespace)
	if err != nil {
		return false,err
	}
	klog.Infof("[%s/%s] ScalerBaseOnMidWareTP %v,%v",sldb.Namespace,sldb.Name,v,tclus.NewHashRate)
	var firstSucess bool
	if v.ScalerFlag == utils.ScalerIn {
		klog.Infof("[%s/%s] scaler in v %v",sldb.Namespace,sldb.Name,v)
		if err := am.SyncAutoScaling(tclus, sldb, v1alpha1.TiDBMemberType); err != nil {
			return false,err
		}
		if tclus.NewHashRate < v.ScalerNeedCore {
			tclus.NewHashRate = v.ScalerNeedCore
		}
		utils.ChangeScalerStatus(sldb.Name,sldb.Namespace)
	} else {
		//正常扩缩
		if v.ScalerNeedCore > tclus.NewHashRate {
			if utils.FEqual(tclus.NewHashRate, 0.5) == true {
				firstSucess = true
				tclus.NewHashRate = math.Ceil(v.ScalerNeedCore) * 4.0
			} else {
				tclus.NewHashRate = v.ScalerNeedCore
			}
		}
		if tclus.NewHashRate == 0  {
			return true,nil
		}
	}
	if err := am.SyncTidbClusterReplicas(sldb, tclus, v1alpha1.TiDBMemberType); err != nil {
		return false,err
	}
	err = am.updateAutoScaling(sldb)
	if err != nil {
		return false,err
	}
	if firstSucess == true {
		v.FirstExpandTime = time.Now().Unix()
	}
	return true,nil
}

func noMidwareMessageToScalerIn(sldb *sldbv1.ServerlessDB,tclus *utils.TClus,sldbType v1alpha1.MemberType) error {
	if sldbType == v1alpha1.TiDBMemberType {
		v, err := utils.GetLastData(sldb.Name, sldb.Namespace)
		if err != nil {
			return err
		}
		if utils.FSmaller(tclus.NewHashRate,tclus.OldHashRate) == true {
			//900s no midware message to scaler in base cpu
			intervalSeconds := int32(900)
			if intervalSeconds > int32(time.Now().Sub(time.Unix(v.ScalerCurtime, 0)).Seconds()) {
				return fmt.Errorf("[%s/%s] sldbType %s wait CheckStsAutoScalingInterval", sldb.Namespace, sldb.Name, string(sldbType))
			}
		}
		return nil
	}
	return nil
}

func (am *AutoScalerManager) autoScalerHander(sldb *sldbv1.ServerlessDB, sldbType v1alpha1.MemberType) error {
	tclus,_ := utils.ClusterStatusCheck(sldb,sldbType)
	if tclus == nil {
		return fmt.Errorf("[%s/%s] clusterStatusCheck problem",sldb.Namespace, sldb.Name)
	}
	if sldbType == v1alpha1.TiDBMemberType {
		err := utils.SyncReplicasToMidWare(tclus,utils.TP)
		if err != nil {
			return fmt.Errorf("[%s/%s]%v", sldb.Namespace, sldb.Name, err)
		}
	}
	if err := am.SyncAutoScaling(tclus, sldb, sldbType); err != nil {
		return err
	}
	//cpu only scaler out, ScalerBaseOnMidWareTP will scaler in base on midware
	if utils.FSmaller(tclus.NewHashRate,tclus.OldHashRate) == true {
		tclus.NewHashRate = tclus.OldHashRate
	}
	if tclus.NewHashRate == 0 && sldbType == v1alpha1.TiDBMemberType {
		return nil
	}
	//midware no message,scaler base on cpu load
	/*
	if err := noMidwareMessageToScalerIn(sldb,tclus,sldbType) ;err!=nil{
		return err
	}*/
	if err := am.SyncTidbClusterReplicas(sldb, tclus, sldbType); err != nil {
		return err
	}
	err := am.updateAutoScaling(sldb)
	if err != nil {
		return err
	}
	return nil
}


func NewAutoScalerManager() *AutoScalerManager {
	autoScaler := &AutoScalerManager{}
	autoScaler.Sldbclient = &slcluster.SldbClient
	return autoScaler
}

func tiDBAllMembersReady(tc *v1alpha1.TidbCluster) bool {
	if int(tc.TiDBStsDesiredReplicas()) != len(tc.Status.TiDB.Members) {
		return false
	}

	for key, member := range tc.Status.TiDB.Members {
		if _, ok := tc.Status.TiDB.FailureMembers[key];ok {
			continue
		}
		if !member.Health {
			return false
		}
	}
	return true
}

func tiKVAllStoresReady(tc *v1alpha1.TidbCluster) bool {
	if int(tc.TiKVStsDesiredReplicas()) != len(tc.Status.TiKV.Stores) {
		return false
	}

	for key, store := range tc.Status.TiKV.Stores {
		if _,ok := tc.Status.TiKV.FailureStores[key];ok {
			continue
		}
		if store.State != v1alpha1.TiKVStateUp {
			return false
		}
	}

	return true
}

func (am *AutoScalerManager) CheckLastAutoScalerCompleted(tcArr *utils.TClus,sldb *sldbv1.ServerlessDB,sldbType v1alpha1.MemberType) error {
	if sldbType == v1alpha1.TiDBMemberType || sldbType == v1alpha1.TiKVMemberType {
		if sldbType == v1alpha1.TiDBMemberType {
			for _,tc := range tcArr.NewTc {
				if false == tiDBAllMembersReady(&tc.Tc) {
					return fmt.Errorf("[%s/%s] sldbType %s tc name %s TiDB not ready",sldb.Namespace,sldb.Name,string(sldbType),tc.Tc.Name)
				}
			}
		} else if sldbType == v1alpha1.TiKVMemberType {
			if false == tiKVAllStoresReady(&tcArr.NewTc[0].Tc) {
				return fmt.Errorf("[%s/%s] sldbType %s tc name %s TiKV not ready",sldb.Namespace,sldb.Name,string(sldbType),tcArr.NewTc[0].Tc.Name)
			}
		}
	}
	return nil
}

func (am *AutoScalerManager) SyncAutoScaling(tcArr *utils.TClus, sldb *sldbv1.ServerlessDB, sldbType v1alpha1.MemberType) error {
	defaultTAC(sldb)
	if err := am.CheckLastAutoScalerCompleted(tcArr,sldb,sldbType);err!=nil {
		return err
	}
	if sldbType == v1alpha1.TiDBMemberType {
		if err := am.SyncTiDB(tcArr, sldb); err != nil {
			klog.Errorf("[%s/%s] sync failed, continue to sync next, err:%v", sldb.Namespace, sldb.Name, err)
		}
	} else if sldbType == v1alpha1.TiKVMemberType {
		oldTikvReplicas := tcArr.OldTc[0].Tc.Spec.TiKV.Replicas
		if err := am.SyncTiKV(&tcArr.NewTc[0].Tc, sldb); err != nil {
			tcArr.NewTc[0].Tc.Spec.TiKV.Replicas = oldTikvReplicas
			klog.Errorf("[%s/%s] tikv sync failed, continue to sync next, err:%v", sldb.Namespace, sldb.Name, err)
		}
	}
	klog.Infof("[%s/%s]'s Sldb[%s/%s] synced", tcArr.NewTc[0].Tc.Namespace, tcArr.NewTc[0].Tc.Name, sldb.Namespace, sldb.Name)
	return nil
}

var tikv_max_replias = os.Getenv("TIKV_MAX_REPLIAS")
func TikvPvcExpand(tc *v1alpha1.TidbCluster, oldTc *v1alpha1.TidbCluster,sldbType v1alpha1.MemberType) (bool,error) {
	if sldbType == v1alpha1.TiKVMemberType {
		maxReplias,err := strconv.Atoi(tikv_max_replias)
		if err != nil {
			klog.Infof("[%s/%s] calculateTiKVStorageMetrics tikv_max_replias is error format %v",tc.Namespace,tc.Name,tikv_max_replias)
			return false,err
		}
		if oldTc.Spec.TiKV.Replicas == int32(maxReplias) && tc.Spec.TiKV.Replicas > int32(maxReplias) {
			tc.Spec.TiKV.Replicas = oldTc.Spec.TiKV.Replicas
			klog.Infof("[%s/%s] tikvPvcExpand",tc.Namespace,tc.Name)
			utils.PvcExpand(tc)
			tc.Spec.TiKV.Replicas = oldTc.Spec.TiKV.Replicas
			err = utils.UpdateTC(tc, sldbType, true)
			if err != nil {
				return false,err
			}
			time.Sleep(time.Minute)
			return true,nil
		}
	}
	return false,nil
}

func tikvScalerOutAndIn(tcArr *utils.TClus,sldb *sldbv1.ServerlessDB,sldbType v1alpha1.MemberType)  error{
	err := utils.GetNewAnnoAndReplias(sldb, &tcArr.NewTc[0].Tc, &tcArr.OldTc[0].Tc, sldbType)
	if err != nil {
		return err
	}
	err = utils.UpdateTC(&tcArr.NewTc[0].Tc, sldbType, true)
	if err != nil {
		return err
	}
	klog.Infof("[%s/%s] UpdateTC err %v  sldbType %v tcArr.NewTc[0].Tc.TIKV %v",sldb.Namespace,sldb.Name,err,sldbType,tcArr.NewTc[0].Tc.Spec.TiKV.Replicas)
	err = utils.PendingHander(sldb,  &tcArr.NewTc[0].Tc, &tcArr.OldTc[0].Tc, sldbType, true)
	if err != nil {
		return err
	}
	klog.Infof("[%s/%s] PendingHander err %v  sldbType %v tcArr.NewTc[0].Tc.TIKV %v,old.TiKV %v",sldb.Namespace,sldb.Name,err,sldbType,tcArr.NewTc[0].Tc.Spec.TiKV.Replicas,
		tcArr.OldTc[0].Tc.Spec.TiKV.Replicas)
	return nil
}

func (am *AutoScalerManager) SyncTidbClusterReplicas(sldb *sldbv1.ServerlessDB, tcArr *utils.TClus,sldbType v1alpha1.MemberType) error {
	if utils.FEqual(tcArr.NewHashRate,tcArr.OldHashRate) == true && tcArr.NewTc[0].Tc.Spec.TiKV.Replicas == tcArr.OldTc[0].Tc.Spec.TiKV.Replicas {
		return nil
	}
	klog.Infof("[%s/%s] NewHashRate %v,OldHashRate %v,newTiKV.Replicas %v,oldTiKV.Replicas %v sldbType %v",sldb.Namespace,sldb.Name,
		tcArr.NewHashRate, tcArr.OldHashRate,tcArr.NewTc[0].Tc.Spec.TiKV.Replicas,tcArr.OldTc[0].Tc.Spec.TiKV.Replicas,sldbType)
	if utils.FSmaller(tcArr.NewHashRate,tcArr.OldHashRate) == true {
		//klog.Infof("[%s/%s] SyncTidbClusterReplicas scale in %v,%v",sldb.Namespace,sldb.Name,tcArr.NewHashRate,tcArr.OldHashRate)
		if err := utils.RecalculateScaleIn(sldb,tcArr,true);err != nil {
			return err
		}
	} else if  utils.FGreater(tcArr.NewHashRate,tcArr.OldHashRate) == true {
		if err := utils.RecalculateScaleOut(sldb,tcArr,true);err != nil {
			return err
		}
	}
	//tikv == tikv_max_replias expand pvc
	explanFlag,err:=TikvPvcExpand(&tcArr.NewTc[0].Tc,&tcArr.OldTc[0].Tc,sldbType)
	if err != nil {
		klog.Infof("[%s/%s] TikvPvcExpand %v sldbType %v",sldb.Namespace,sldb.Name,err,sldbType)
		return err
	}
	if explanFlag == true {
		klog.Infof("[%s/%s] TikvPvcExpand success  sldbType %v",sldb.Namespace,sldb.Name,sldbType)
		return nil
	}
	//scaler in no resouce problem
	promit, err := utils.ResourceProblemCheck(sldb, tcArr, sldbType)
	if err != nil {
		klog.Infof("[%s/%s] ResourceProblemCheck err %v  sldbType %v",sldb.Namespace,sldb.Name,err,sldbType)
		return err
	}
	if promit == false {
		klog.Infof("[%s/%s] ResourceProblemCheck err %v  sldbType %v",sldb.Namespace,sldb.Name,err,sldbType)
		return nil
	}
	if sldbType == v1alpha1.TiKVMemberType {
		if err := tikvScalerOutAndIn(tcArr,sldb,sldbType);err!= nil {
			return err
		}
	} else {

		if err = utils.HanderAllScalerOut(tcArr, sldb, sldbType, true); err != nil {
			return err
		}
		if err = utils.HanderAllScalerIn(tcArr, sldb, sldbType, true); err != nil {
			return err
		}
	}
	//modify pvc
	return nil
}

func (am *AutoScalerManager) updateAutoScaling(sldb *sldbv1.ServerlessDB) error {
	if sldb.Annotations == nil {
		sldb.Annotations = map[string]string{}
	}
	now := time.Now()
	sldb.Annotations[utils.AnnLastSyncingTimestamp] = fmt.Sprintf("%d", now.Unix())
	return am.UpdateSldbClusterAutoScaler(sldb)
}

func updateServerlessDB(am *AutoScalerManager,ns string,sldb *sldbv1.ServerlessDB) (*sldbv1.ServerlessDB, error){
	return am.Sldbclient.Client.BcrdsV1alpha1().ServerlessDBs(ns).Update(sldb)
}

func listerServerlessDB(am *AutoScalerManager,ns string,tacName string)(*sldbv1.ServerlessDB, error) {
	return am.Sldbclient.Lister.ServerlessDBs(ns).Get(tacName);
}

func (am *AutoScalerManager) UpdateSldbClusterAutoScaler(sldb *sldbv1.ServerlessDB) error {
	ns := sldb.Namespace
	tacName := sldb.Name
	oldTac := sldb.DeepCopy()
	var oldV, oldV1, oldV2 string
	oldV, _ = sldb.Annotations[utils.AnnLastSyncingTimestamp]
	oldV1, _ = sldb.Annotations[utils.AnnTiDBLastAutoScalingTimestamp]
	oldV2, _ = sldb.Annotations[utils.AnnTiKVLastAutoScalingTimestamp]
	// don't wait due to limited number of clients, but backoff after the default number of steps
	err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		var updateErr error
		_, updateErr = updateServerlessDB(am,ns,sldb)
		if updateErr == nil {
			klog.Infof("[%s/%s] SldbClusterAutoScaler: updated successfully", ns, tacName)
			return nil
		}
		klog.V(4).Infof("[%s/%s] failed to update SldbClusterAutoScaler: , error: %v", ns, tacName, updateErr)
		if updated, err := listerServerlessDB(am,ns,tacName); err == nil {
			// make a copy so we don't mutate the shared cache
			sldb = updated.DeepCopy()
			if sldb.Annotations == nil {
				sldb.Annotations = map[string]string{}
			}
			if oldV != "" {
				sldb.Annotations[utils.AnnLastSyncingTimestamp] = oldV
			}
			if oldV1 != "" {
				sldb.Annotations[utils.AnnTiDBLastAutoScalingTimestamp] = oldV1
			}
			if oldV2 != "" {
				sldb.Annotations[utils.AnnTiKVLastAutoScalingTimestamp] = oldV2
			}
			sldb.Status = oldTac.Status
		} else {
			utilruntime.HandleError(fmt.Errorf("[%s/%s] error getting updated SldbClusterAutoScaler from lister: %v", ns, tacName, err))
		}
		return updateErr
	})
	if err != nil {
		klog.Errorf("[%s/%s] failed to update SldbClusterAutoScaler: error: %v", ns, tacName, err)
	}
	return err
}
