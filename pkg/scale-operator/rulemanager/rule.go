package rulemanager

import (
	"fmt"
	tidbv1 "github.com/pingcap/tidb-operator/pkg/apis/pingcap/v1alpha1"
	"github.com/tidb-incubator/Serverlessdb-for-HTAP/pkg/scale-operator/sldbcluster"
	"github.com/tidb-incubator/Serverlessdb-for-HTAP/pkg/scale-operator/utils"
	"github.com/tidb-incubator/Serverlessdb-for-HTAP/pkg/sldb-operator/apis/bcrds/v1alpha1"
	"github.com/tidb-incubator/Serverlessdb-for-HTAP/pkg/sldb-operator/util"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/client-go/util/retry"
	"k8s.io/klog"
	"time"
)

const PreScaleTime = 30

func AutoscaleByRule(sldb *v1alpha1.ServerlessDB) (bool, error) {
	now := time.Now().Add(time.Minute).Truncate(time.Minute)
	if sldb.Spec.Paused || sldb.Spec.Freeze {
		klog.Infof("sldb %s/%s is pause(%v) or frozen(%v), skip autoscale.", sldb.Namespace, sldb.Name, sldb.Spec.Freeze, sldb.Spec.Paused)
		return true, nil
	}
	klog.Infof("sldb %s/%s start check if exist auto scale rule.", sldb.Namespace, sldb.Name)
	if sldb.Spec.Rule != nil {
		var totalHashrate int
		var clusterInRule = false
		var rulelist []string
		for ruleid, rule := range sldb.Spec.Rule {
			inrule, hashrate := CheckIfInRule(rule, now)
			if inrule {
				totalHashrate += hashrate
				clusterInRule = true
				rulelist = append(rulelist, ruleid)

			}
		}
		if !clusterInRule {
			if len(sldb.Status.Rule) > 0 {
				klog.Infof("sldb %s/%s don't exist effective rule, clear rules in sldb's status after %v second.", sldb.Namespace, sldb.Name, PreScaleTime)
				time.Sleep(PreScaleTime * time.Second)
				rules := make([]string, 0)
				updateRuleStatus(sldb, rules)
			}
			klog.Infof("sldb %s/%s don't exist effective rule, skip autoscale.", sldb.Namespace, sldb.Name)
			return false, nil
		}

		updateRuleStatus(sldb, rulelist)

		//check cluster's status
		var times int
		overtimes := 60
		for {
			if times > overtimes {
				klog.Errorf("[%s/%s] CheckClusterStatus sldb is not permit to scale, timeout.", sldb.Namespace, sldb.Name)
				break
			}
			currentsldb, err := getSldb(sldb.Namespace, sldb.Name)
			if err != nil {
				klog.Errorf("[%s/%s] CheckClusterStatus  SldbLister failed %v", sldb.Namespace, sldb.Name, err)
			}
			if currentsldb.Status.Phase == v1alpha1.PhaseAvailable && !util.IsServerlessDBRestart(sldb) {
				break
			}
			time.Sleep(10 * time.Second)
			times++
		}

		totalHashrate = utils.CompareResource(sldb.Spec.MaxValue.Metric, totalHashrate)

		tclist,_,err := utils.CloneMutiRevsionTc(sldb,utils.TP)
		if err != nil {
			klog.Errorf("sldb %s/%s clone multi revision TC failed: %v.", sldb.Namespace, sldb.Name, err)
			return false, err
		}
		currentHashrate := int(tclist.OldHashRate)
		tclist.NewHashRate = float64(totalHashrate)

		if totalHashrate == currentHashrate {
			klog.Infof("current replica is equal with target replica, no need scale.")
			return false, nil
		}

		if totalHashrate > currentHashrate {
			//to scale out tidb
			if currentHashrate == 0 {
				conds := util.FilterOutCondition(sldb.Status.Conditions, v1alpha1.TiDBSilence)
				sldb.Status.Conditions = conds
				if err = utils.UpdateSldbCondStatus(sldb); err != nil {
					klog.Errorf("[%s/%s]fail to update sldb conditions: %s", sldb.Namespace, sldb.Name, err)
					return false, err
				}
			}
			klog.Infof("target replica more than replica in tc, start to scale out.")
			if err := utils.RecalculateScaleOut(sldb, tclist, false); err != nil {
				return false, err
			}

		} else if totalHashrate < currentHashrate {
			//todo: set prescale time to be configurable
			//need wait 30s when scale in, because of cron is 59m and 30s per hour.
			time.Sleep(PreScaleTime * time.Second)
			//to scale in tidb
			klog.Infof("target replica less than replica in tc, start to scale in.")
			if err := utils.RecalculateScaleIn(sldb, tclist, false); err != nil {
				return false, err
			}
		}

		promit, err := utils.ResourceProblemCheck(sldb, tclist, tidbv1.TiDBMemberType)
		if err != nil {
			return false, err
		}
		if promit == false {
			return false, nil
		}

		if err = utils.HanderAllScalerOut(tclist, sldb, tidbv1.TiDBMemberType, false); err != nil {
			return false, err
		}
		if err = utils.HanderAllScalerIn(tclist, sldb, tidbv1.TiDBMemberType, false); err != nil {
			return false, err
		}
		var newhr float64
		for _, tc := range tclist.NewTc {
			newhr += float64(tc.Replicas) * tc.HashratePerTidb
		}

		return true, nil
	}
	if len(sldb.Status.Rule) > 0 {
		klog.Infof("sldb %s/%s don't exist rule, but need clear rules in sldb's status after %v second.", sldb.Namespace, sldb.Name, PreScaleTime)
		time.Sleep(PreScaleTime * time.Second)
		rules := make([]string, 0)
		updateRuleStatus(sldb, rules)
	}
	klog.Infof("sldb %s/%s don't exist rule, skip autoscale.", sldb.Namespace, sldb.Name)

	return false, nil
}

func CheckIfInRule(rule v1alpha1.Rule, now time.Time) (bool, int) {
	loc, _ := time.LoadLocation("Asia/Shanghai")
	starttime := rule.StartTime.Time.In(loc)
	endtime := rule.EndTime.Time.In(loc)

	klog.Infof("now is %v, start is %v, end is %v.", now, starttime, endtime)
	//if now before starttime, so the rule is not valid.
	if now.Before(starttime) {
		return false, 0
	}

	//if now later starttime and before endtime, so the rule is valid. then calc the resource that the rule need.
	if now.Before(endtime) {
		return true, utils.CalcMaxPerfResource(rule.Metric)
	}

	//if now later endtime, so need judge the rule is vaild or not by period.
	switch rule.Period {
	case v1alpha1.CUSTOMIZE:
		return false, 0
	case v1alpha1.DAY:
		if now.Hour() >= starttime.Hour() && now.Hour() < endtime.Hour() {
			return true, utils.CalcMaxPerfResource(rule.Metric)
		}
		return false, 0
	case v1alpha1.MONTH:
		if now.Day() == starttime.Day() && now.Hour() >= starttime.Hour() && now.Hour() < endtime.Hour() {
			return true, utils.CalcMaxPerfResource(rule.Metric)
		}
		return false, 0
	case v1alpha1.WEEK:
		if now.Weekday() == starttime.Weekday() && now.Hour() >= starttime.Hour() && now.Hour() < endtime.Hour() {
			return true, utils.CalcMaxPerfResource(rule.Metric)
		}
		return false, 0
	default:
		return false, 0
	}
}

func updateRuleStatus(sldb *v1alpha1.ServerlessDB, rulelist []string) error {
	klog.Infof("sldb %s/%s rulelist is %v, status rule is %v", sldb.Namespace, sldb.Name, rulelist, sldb.Status.Rule)
	sldb.Status.Rule = rulelist

	// don't wait due to limited number of clients, but backoff after the default number of steps
	err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		var updateErr error
		_, updateErr = sldbcluster.SldbClient.Client.BcrdsV1alpha1().ServerlessDBs(sldb.Namespace).UpdateStatus(sldb)
		if updateErr == nil {
			klog.Infof("ServerlessDB: [%s/%s] updated successfully", sldb.Namespace, sldb.Name)
			return nil
		}
		klog.Infof("failed to update ServerlessDB: [%s/%s], error: %v", sldb.Namespace, sldb.Name, updateErr)
		if updated, err := sldbcluster.SldbClient.Lister.ServerlessDBs(sldb.Namespace).Get(sldb.Name); err == nil {
			// make a copy so we don't mutate the shared cache
			sldb = updated.DeepCopy()
			sldb.Status.Rule = rulelist
		} else {
			utilruntime.HandleError(fmt.Errorf("error getting updated ServerlessDB %s/%s from lister: %v", sldb.Namespace, sldb.Name, err))
		}
		return updateErr
	})
	if err != nil {
		klog.Errorf("failed to update ServerlessDB: [%s/%s], error: %v", sldb.Namespace, sldb.Name, err)
	}
	return err
}

func getSldb(ns, clus string) (*v1alpha1.ServerlessDB, error) {
	return sldbcluster.SldbClient.Lister.ServerlessDBs(ns).Get(clus)
}
