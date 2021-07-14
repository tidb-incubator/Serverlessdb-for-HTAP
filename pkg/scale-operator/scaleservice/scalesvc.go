package scaleservice

import (
	"context"
	"fmt"
	tidbv1 "github.com/pingcap/tidb-operator/pkg/apis/pingcap/v1alpha1"
	"github.com/pingcap/tidb-operator/pkg/label"
	"github.com/tidb-incubator/Serverlessdb-for-HTAP/pkg/scale-operator/scalepb"
	"github.com/tidb-incubator/Serverlessdb-for-HTAP/pkg/scale-operator/sldbcluster"
	"github.com/tidb-incubator/Serverlessdb-for-HTAP/pkg/scale-operator/utils"
	"github.com/tidb-incubator/Serverlessdb-for-HTAP/pkg/sldb-operator/apis/bcrds/v1alpha1"
	"github.com/tidb-incubator/Serverlessdb-for-HTAP/pkg/sldb-operator/util"
	"google.golang.org/grpc/peer"
	corev1 "k8s.io/api/core/v1"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/client-go/util/retry"
	"k8s.io/klog"
	"time"
)

const initTiDBCount = 1
const ComponentLabelKey string = "app.kubernetes.io/component"
const AllInstanceLabelKey string = "bcrds.cmss.com/instance"

type Service struct{}

//UpdateRule immediately updates instance's hashrates, only deal with active rules which need increase.
func (*Service) UpdateRule(ctx context.Context, req *scalepb.UpdateRequest) (*scalepb.UpdateReply, error) {
	klog.Infoln("updateRule method is called")
	now := time.Now()
	reply := &scalepb.UpdateReply{
		Success: false,
	}
	clus := req.GetClustername()
	ns := req.GetNamespace()
	p, _ := peer.FromContext(ctx)
	klog.Infof("[%s/%s]UpdateRule method is called remote ip %v ", ns, clus, p)
	sldb, err := utils.GetSldb(clus, ns)
	if err != nil {
		klog.Errorf("sldb %v/%v err is %v", ns, clus, err)
		return reply, err
	}
	var totalHashrate int
	for _, ruleid := range sldb.Status.Rule {
		rule := sldb.Spec.Rule[ruleid]
		totalHashrate = totalHashrate + utils.CalcMaxPerfResource(rule.Metric)
	}
	totalHashrate = utils.CompareResource(sldb.Spec.MaxValue.Metric, totalHashrate)

	tclist, err := utils.CloneMutiRevsionTc(sldb)
	if err != nil {
		klog.Errorf("sldb %s/%s clone multi revision TC failed: %v.", sldb.Namespace, sldb.Name, err)
		return reply, err
	}
	currentHashrate := int(tclist.OldHashRate)
	tclist.NewHashRate = float64(totalHashrate)
	klog.Infof("new hr is %v, old hr is %v.", totalHashrate, currentHashrate)
	if totalHashrate > currentHashrate {
		//to scale out tidb
		klog.Infof("target replica more than replica in tc, start to scale out.")
		if err := utils.RecalculateScaleOut(sldb, tclist, false); err != nil {
			return reply, err
		}
	}
	go CheckClusterReady(sldb, tclist, now)

	reply.Success = true
	return reply, nil
}

func CheckClusterReady(sldb *v1alpha1.ServerlessDB, tclist *utils.TClus, now time.Time) {
	promit, err := utils.ResourceProblemCheck(sldb, tclist, tidbv1.TiDBMemberType)
	if err != nil || promit == false {
		return
	}

	if err = utils.HanderAllScalerOut(tclist, sldb, tidbv1.TiDBMemberType, false); err != nil {
		return
	}
	if err = utils.HanderAllScalerIn(tclist, sldb, tidbv1.TiDBMemberType, false); err != nil {
		return
	}

}

func (*Service) AutoScalerCluster(ctx context.Context, req *scalepb.AutoScaleRequest) (*scalepb.UpdateReply, error) {

	name := req.GetClustername()
	ns := req.GetNamespace()
	hashrate := req.GetHashrate()
	p, _ := peer.FromContext(ctx)
	klog.Infof("[%s/%s]AutoScalerCluster method is called remote ip %s hashrate %v\n", ns, name, p, hashrate)
	autoScalerFlag := req.GetAutoscaler()
	curtime := req.GetCurtime()
	data := utils.ScalerData{
		ScalerNeedCore: float64(hashrate),
		ScalerCurtime:  curtime,
	}
	utils.UpdateLastData(name, ns, &data, int(autoScalerFlag))
	reply := &scalepb.UpdateReply{
		Success: true,
	}
	return reply, nil
}

//ScaleCluster awakes or silences instance.
func (*Service) ScaleCluster(ctx context.Context, req *scalepb.ScaleRequest) (*scalepb.UpdateReply, error) {
	reply := &scalepb.UpdateReply{
		Success: false,
	}
	clus := req.GetClustername()
	ns := req.GetNamespace()
	hashrate := req.GetHashrate()
	p, _ := peer.FromContext(ctx)
	klog.Infof("[%s/%s]ScaleCluster method is called remote ip %s hashrate %v\n", ns, clus, p, hashrate)

	sldb, err := utils.GetSldb(clus, ns)
	if err != nil {
		klog.Errorf("[%s/%s]get sldb failed: %s", ns, clus, err)
		return reply, err
	}

	//check cluster if can scale.If cluster is frozen or pause or in rule, cannot scale.
	if sldb.Spec.Paused == true || sldb.Spec.Freeze == true || len(sldb.Status.Rule) > 0 {
		klog.Errorf("[%s/%s]cluster is not permit to scale into %v", ns, clus, hashrate)
		return reply, fmt.Errorf("cluster is not permit to scale")
	}

	listOptions := metav1.ListOptions{
		LabelSelector: fmt.Sprintf("%s=%s", AllInstanceLabelKey, sldb.Name),
	}
	tclist, err := utils.ListTidbcluster(ns, listOptions)

	if err != nil {
		klog.Errorf("[%s/%s]get tc failed: %s", ns, clus, err)
		return reply, fmt.Errorf("not exist cluster")
	}
	cpu := resource.MustParse(fmt.Sprintf("%v", hashrate/utils.HashratePerTidb))
	for i, tc := range tclist.Items {
		klog.Infof("[%s/%s]ScaleCluster check tc", tc.Namespace, tc.Name)
		if hashrate == 0 {
			if i == 0 {
				utils.CleanHashrate(&tc)
			}
			if tc.Annotations != nil {
				if _, ok := tc.Annotations[label.AnnTiDBDeleteSlots]; ok {
					delete(tc.Annotations, label.AnnTiDBDeleteSlots)
				}
			}
			//set all tcs of sldb replica to 0.
			if tc.Spec.TiDB.Replicas == 0 {
				continue
			}
			tc.Spec.TiDB.Replicas = 0

			if err = updateTc(&tc, tc.Spec.TiDB.Replicas, cpu); err != nil {
				klog.Errorf("[%s/%s]update tc failed: %s", ns, tc.Name, err)
				return reply, fmt.Errorf("update tc %s/%s failed.", tc.Namespace, tc.Name)
			}
		} else {
			//set replica to 1 and cpu to request of tc which is same name with sldb.
			if tc.Spec.TiDB.Replicas != 0 {
				klog.Infof("[%s/%s]ScaleCluster tidb exist %d pod(s)", tc.Namespace, tc.Name, tc.Spec.TiDB.Replicas)
				reply.Success = true
				return reply, nil
			}
			if tc.Name != sldb.Name {
				if tc.Spec.TiDB.Replicas == 0 {
					continue
				}
				tc.Spec.TiDB.Replicas = 0
				if err = updateTc(&tc, tc.Spec.TiDB.Replicas, cpu); err != nil {
					klog.Errorf("[%s/%s]update tc failed: %s", ns, tc.Name, err)
					return reply, fmt.Errorf("update tc %s/%s failed.", tc.Namespace, tc.Name)
				}
			} else {
				tc.Spec.TiDB.Replicas = 1
				utils.OneHashrate(&tc)
				if err = updateTc(&tc, tc.Spec.TiDB.Replicas, cpu); err != nil {
					klog.Errorf("[%s/%s]update tc failed: %s", ns, tc.Name, err)
					return reply, fmt.Errorf("update tc %s/%s failed.", tc.Namespace, tc.Name)
				}
			}
		}
	}

	//hashrate represent set sldb slience, need set sldb's condition to silence.
	if hashrate == 0 {
		con := util.NewServerlessDBCondition(v1alpha1.TiDBSilence, corev1.ConditionTrue, "", "")
		util.SetServerlessDBCondition(&sldb.Status, *con)
		if err = utils.UpdateSldbCondStatus(sldb); err != nil {
			klog.Errorf("[%s/%s]fail to update sldb conditions: %s", sldb.Namespace, sldb.Name, err)
			return reply, err
		}

		reply.Success = true
		return reply, nil
	}

	var times int
	var listerr error
	var pods []*v1.Pod
	podrunning := false
	//wait for pod is running
	var readyPod *corev1.Pod
	for {
		times++
		if times > 120 || podrunning {
			klog.Infof("[%s/%s] check more than 60s or pod is running %v", sldb.Namespace, sldb.Name, podrunning)
			break
		}

		pods, listerr = utils.GetK8sAllPodArray(sldb.Name, sldb.Namespace, tidbv1.TiDBMemberType)
		if listerr != nil {
			klog.Errorf("[%s/%s] get pods list failed: %s", sldb.Namespace, sldb.Name, listerr)
			break
		}
		for i, pod := range pods {
			if utils.IsPodReady(pod) {
				podrunning = true
				readyPod = pods[i]
				break
			}
		}
		time.Sleep(500 * time.Millisecond)
	}
	if listerr != nil || times >= 120 {
		return reply, listerr
	}
	//dns check
	if readyPod != nil {
		if err = utils.DnsCheck(readyPod); err != nil {
			klog.Errorf("[%s/%s] ScaleCluster DnsCheck check failed %v", sldb.Namespace, sldb.Name, err)
		} else {
			klog.Infof("[%s/%s] ScaleCluster DnsCheck success,pod %s", sldb.Namespace, sldb.Name, readyPod.Name)
		}
		var podList []*corev1.Pod
		podList = append(podList, readyPod)
		if err := utils.CallupTidb(podList, sldb.Name, sldb.Namespace); err != nil {
			return reply, err
		}
	}

	conds := util.FilterOutCondition(sldb.Status.Conditions, v1alpha1.TiDBSilence)
	sldb.Status.Conditions = conds
	if err = utils.UpdateSldbCondStatus(sldb); err != nil {
		klog.Errorf("[%s/%s]fail to update sldb conditions: %s", sldb.Namespace, sldb.Name, err)
		return reply, err
	}

	reply.Success = true
	return reply, nil
}

func updateTc(tc *tidbv1.TidbCluster, replica int32, cpu resource.Quantity) error {
	var err error
	oldAnnon := tc.Annotations
	//complete resource
	norms, _ := resource.ParseQuantity("0.5")
	oneMemNorms, _ := resource.ParseQuantity("1Gi")
	max := cpu.MilliValue() / norms.MilliValue()
	var memNorms resource.Quantity
	for i := 0; i < int(max); i++ {
		memNorms.Add(oneMemNorms)
	}
	klog.Infof("[%s/%s] TidbCluster reps %d cpu %s memory %s\n", tc.Namespace, tc.Name, replica, cpu.String(), memNorms.String())
	var updateFlag = true
	if replica != 0 {
		if v, ok := tc.Spec.TiDB.Limits[corev1.ResourceCPU]; ok {
			if v.String() == cpu.String() {
				updateFlag = false
			}
		} else {
			if tc.Spec.TiDB.Limits == nil {
				tc.Spec.TiDB.Limits = make(corev1.ResourceList)
				tc.Spec.TiDB.Requests = make(corev1.ResourceList)
			}
		}
		tc.Spec.TiDB.Limits[corev1.ResourceCPU] = cpu
		tc.Spec.TiDB.Limits[corev1.ResourceMemory] = memNorms
		tc.Spec.TiDB.Requests[corev1.ResourceCPU] = cpu
		tc.Spec.TiDB.Requests[corev1.ResourceMemory] = memNorms
	}
	if updateFlag == true && replica != 0 {
		err := utils.UpdateOneTcToNewNorms(tc, 0)
		if err != nil {
			return err
		}
	}
	err = retry.RetryOnConflict(retry.DefaultRetry, func() error {
		var updateErr error
		_, updateErr = sldbcluster.SldbClient.PingCapCli.PingcapV1alpha1().TidbClusters(tc.Namespace).Update(tc)
		if updateErr == nil {
			klog.Infof("TiDBCluster: [%s/%s] updated successfully", tc.Namespace, tc.Name)
			return nil
		}
		klog.Errorf("failed to update TiDBCluster: [%s/%s], error: %v", tc.Namespace, tc.Name, updateErr)
		if updated, err := sldbcluster.SldbClient.PingCapLister.TidbClusters(tc.Namespace).Get(tc.Name); err == nil {
			// make a copy so we don't mutate the shared cache
			tc = updated.DeepCopy()
			tc.Spec.TiDB.Replicas = replica
			tc.Annotations = oldAnnon
			if replica != 0 {
				tc.Spec.TiDB.Limits[corev1.ResourceCPU] = cpu
				tc.Spec.TiDB.Limits[corev1.ResourceMemory] = memNorms
				tc.Spec.TiDB.Requests[corev1.ResourceCPU] = cpu
				tc.Spec.TiDB.Requests[corev1.ResourceMemory] = memNorms
			}
		} else {
			utilruntime.HandleError(fmt.Errorf("error getting updated ServerlessDB %s/%s from lister: %v", tc.Namespace, tc.Name, err))
		}
		return updateErr
	})
	return err
}
