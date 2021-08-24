package scaleservice

import (
	"context"
	"encoding/json"
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
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/client-go/util/retry"
	"k8s.io/klog"
	"math"
	"sort"
	"strconv"
	"strings"
	"time"
)

const initTiDBCount = 1
const ComponentLabelKey string = "app.kubernetes.io/component"
const AllInstanceLabelKey string = "bcrds.cmss.com/instance"
const RoleInstanceLabelKey string = "bcrds.cmss.com/role"

type Service struct{}

func (s *Service) ScaleTempCluster(ctx context.Context, request *scalepb.TempClusterRequest) (*scalepb.TempClusterReply, error) {
	klog.Infof("[%s/%s]HandleLargeTc method is called, the svcName is %s\n", request.Namespace, request.Clustername)
	var res scalepb.TempClusterReply
	if request.Start == true {
		svcName, err := StartLargeTc(request.Clustername, request.Namespace)
		res.StartAddr = svcName
		if err != nil {
			klog.Errorf("[%s/%s]StartLargeTc failed: %s", request.Namespace, request.Clustername, err)
			res.Success = false
			return &res, err
		}
		klog.Infof("[%s/%s]finish create large tc--------Addr %s\n", request.Namespace, request.Clustername,svcName)
		res.Success = true
		return &res, err
	} else if request.StopAddr != "" {
		err := StopLargeTc(request.Clustername, request.Namespace, request.StopAddr)
		if err != nil {
			klog.Errorf("[%s/%s]StopLargeTc failed: %s", request.Namespace, request.Clustername, err)
			res.Success = false
			return &res, err
		}
		klog.Infof("[%s/%s]finish delete large tc--------Addr %s\n", request.Namespace, request.Clustername,request.StopAddr)
		res.Success = true
		return &res, err
	}
	return &res, fmt.Errorf("invaild request")
}

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
		totalHashrate = totalHashrate + int(utils.CalcMaxPerfResource(rule.Metric))
	}
	totalHashrate = utils.CompareResource(sldb.Spec.MaxValue.Metric, totalHashrate)

	tclist,_,err := utils.CloneMutiRevsionTc(sldb,utils.TP)
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
	scaletype := req.GetScaletype()
	p, _ := peer.FromContext(ctx)
	klog.Infof("[%s/%s]AutoScalerCluster method is called remote ip %s hashrate %v\n", ns, name, p, hashrate)
	autoScalerFlag := req.GetAutoscaler()
	curtime := req.GetCurtime()
	data := utils.ScalerData{
		ScalerNeedCore: float64(hashrate),
		ScalerCurtime:  curtime,
	}
	utils.UpdateLastData(name + "-" + scaletype, ns, &data, int(autoScalerFlag))
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
	scaletype := req.GetScaletype()
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
				utils.CleanHashrate(&tc,scaletype)
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
				utils.OneHashrate(&tc,scaletype)
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

		pods, listerr = utils.GetK8sAllPodArray(sldb.Name, sldb.Namespace, tidbv1.TiDBMemberType,scaletype)
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
		podList = append(podList,readyPod)
		if err := utils.CallupTidb(podList,sldb.Name,sldb.Namespace,scaletype);err!=nil {
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
	hashrate := 0.5*float64(max)
	memNorms = utils.GetMemory(hashrate,oneMemNorms)
	var limitCpu,limitMem resource.Quantity
	if max == 1 {
		nameArr := strings.Split(tc.Name,"-new")
		sldb, err := utils.GetSldb(nameArr[0], tc.Namespace)
		if err != nil {
			return err
		}
		//0.5 hashrate use 1/4 max resource
		hashrate := utils.CalcMaxPerfResource(sldb.Spec.MaxValue.Metric)
		hashrateMode := math.Ceil(float64(hashrate)/4.0)
		cpustr := fmt.Sprintf("%g", hashrateMode)
		limitCpu,err = resource.ParseQuantity(cpustr)
		if err != nil {
			klog.Errorf("[%s/%s] err %v updateTc limit cpustr %s", tc.Namespace, tc.Name, err, cpustr)
			return err
		}
		limitMem = utils.GetMemory(hashrateMode,oneMemNorms)
	} else {
		limitCpu = cpu
		limitMem = memNorms
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
		tc.Spec.TiDB.Limits[corev1.ResourceCPU] = limitCpu
		tc.Spec.TiDB.Limits[corev1.ResourceMemory] = limitMem
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
				tc.Spec.TiDB.Limits[corev1.ResourceCPU] = limitCpu
				tc.Spec.TiDB.Limits[corev1.ResourceMemory] = limitMem
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

func CreateLargeTc(clusName, ns, largeTCName string) (*tidbv1.TidbCluster, error) {
	tc, err := sldbcluster.SldbClient.PingCapLister.TidbClusters(ns).Get(clusName)
	if err != nil {
		klog.Errorf("[%s/%s] get TidbClusters failed", ns, clusName)
		return nil, fmt.Errorf("get tc fail", err)
	}
	var newtc = &tidbv1.TidbCluster{}

	newtc.Name = largeTCName
	newtc.Namespace = ns
	if newtc.Labels == nil {
		newtc.Labels = make(map[string]string)
	}
	newtc.Labels = util.New().Instance(largeTCName)
	newtc.Spec.TiDB = tc.Spec.TiDB.DeepCopy()
	newtc.Spec.TiDB.Labels[RoleInstanceLabelKey] = "bigcost"
	newtc.Spec.TiDB.Replicas = 1
	var limit = make(corev1.ResourceList)
	limit[corev1.ResourceMemory] = resource.MustParse("2Gi")
	limit[corev1.ResourceCPU] = resource.MustParse("1")

	newtc.Spec.TiDB.Limits = limit
	newtc.Spec.Version = tc.Spec.Version
	newtc.Spec.TiDB.Service = nil
	newtc.Spec.TiDB.StorageClassName = tc.Spec.TiDB.StorageClassName
	newtc.Spec.SchedulerName = tc.Spec.SchedulerName
	newtc.Spec.TiDB.Requests = limit.DeepCopy()
	newtc.Spec.Annotations = map[string]string{
		util.InstanceAnnotationKey: largeTCName,
	}
	newtc.Spec.Cluster = &tidbv1.TidbClusterRef{
		Name:      clusName,
		Namespace: tc.Namespace,
	}
	// set owner references
	newtc.OwnerReferences = tc.OwnerReferences
	_, err = sldbcluster.SldbClient.PingCapCli.PingcapV1alpha1().TidbClusters(tc.Namespace).Create(newtc)
	if err != nil {
		klog.Infof("[%s/%s]-----Create tc fail------------ %v\n", ns, largeTCName, err)
		if errors.IsAlreadyExists(err) {
			newtc, err = sldbcluster.SldbClient.PingCapCli.PingcapV1alpha1().TidbClusters(tc.Namespace).Get(largeTCName, metav1.GetOptions{})
			if err != nil {
				klog.Errorf("[%s/%s] get TidbClusters failed", tc.Namespace, clusName)
				return nil, err
			}
		} else {
			klog.Errorf("[%s/%s] create TidbClusters failed", tc.Namespace, clusName)
			return nil, err
		}
	}
	klog.Infof("[%s/%s]-------------------finish create------------\n", ns, largeTCName)

	return newtc, nil
}

func PodStatusHander(ns, largeTCName, index string) bool {
	var timeCount int
	podStatus := false
	for {
		timeCount++
		time.Sleep(600 * time.Millisecond)
		if timeCount > 200 {
			klog.Infof("[%s/%s] start more than 120s", ns, largeTCName)
			break
		}
		var currtc *tidbv1.TidbCluster
		var err error
		if currtc, err = sldbcluster.SldbClient.PingCapLister.TidbClusters(ns).Get(largeTCName); err != nil {
			klog.Infof("[%s/%s] PodStatusCheckRollback  PingCapLister failed %v", ns, largeTCName, err)
			continue
		}
		var podlist []*corev1.Pod
		if podlist, err = utils.GetK8sPodArray(currtc, tidbv1.TiDBMemberType,utils.TP); err != nil {
			klog.Infof("[%s/%s] GetK8sPodArray failed %v", ns, largeTCName, err)
			continue
		}
		for i, pod := range podlist {
			if pod.Name == largeTCName+"-tidb-"+index {
				if utils.IsPodReady(pod) == true {
					klog.Infof("[%s/%s]-------pod is ready------------%s\n", ns, largeTCName, pod.Name)
					err := utils.DnsCheck(podlist[i])
					if err != nil {
						klog.Errorf("[%s/%s] PodStatusHander DnsCheck check failed %v", podlist[i].Namespace, podlist[i].Name, err)
					} else {
						klog.Infof("[%s/%s] PodStatusHander DnsCheck check success", podlist[i].Namespace, podlist[i].Name)
						podStatus = true
					}
				}
			}
		}
		if podStatus == true {
			break
		}
	}
	return podStatus
}

func GetAnnoIndex(tc *tidbv1.TidbCluster) (string, map[string]string, error) {
	anno := tc.GetAnnotations()
	var index string
	if anno == nil {
		anno = map[string]string{}
	}
	value, ok := anno[label.AnnTiDBDeleteSlots]
	if ok {
		klog.Infof("[%s/%s] index is--------ok to find--------", tc.Namespace, tc.Name)
		slice := make([]int, 0)
		if value != "" {
			err := json.Unmarshal([]byte(value), &slice)
			if err != nil {
				return index, anno, fmt.Errorf("[%s/%s] unmarshal value %s failed: %s", tc.Namespace, tc.Name, value, err)
			}
			index = strconv.Itoa(slice[0])
			if 1 >= len(slice) {
				delete(anno, label.AnnTiDBDeleteSlots)
			} else {
				slice = slice[1:]
				s, err := json.Marshal(slice)
				if err != nil {
					return index, anno, fmt.Errorf("[%s/%s] marshal slice %v failed when acutal is less than target: %s", tc.Namespace, tc.Name, slice, err)
				}
				anno[label.AnnTiDBDeleteSlots] = string(s)
			}
		}
	}
	if index == "" {
		klog.Infof("[%s/%s] index is null--------", tc.Namespace, tc.Name)
		var podlist []*corev1.Pod
		var err error
		if podlist, err = utils.GetK8sPodArray(tc, tidbv1.TiDBMemberType,utils.TP); err != nil {
			klog.Infof("[%s/%s] GetK8sPodArray failed %v", tc.Namespace, tc.Name, err)
			return index, anno, err
		}
		index = strconv.Itoa(len(podlist))
	}

	return index, anno, nil
}

func NormalOrNot(clusName, ns string) bool {
	var timeCount int
	var tcStatus bool
	for {
		timeCount++
		time.Sleep(600 * time.Millisecond)
		if timeCount > 100 {
			klog.Infof("[%s/%s] check status more than 60s", ns, clusName)
			tcStatus = false
			break
		}
		largeTcNow, err := sldbcluster.SldbClient.PingCapLister.TidbClusters(ns).Get(clusName)
		if err != nil {
			tcStatus = false
			continue
		}
		if largeTcNow.Status.TiDB.Phase == tidbv1.NormalPhase {
			tcStatus = true
			break
		}
		tcStatus = false
	}
	return tcStatus
}

func deletePod(largeTc *tidbv1.TidbCluster, index string) error {
	if index == "" {
		return fmt.Errorf("the index is null!!!!")
	}
	slice := make([]int, 0)
	anno := largeTc.GetAnnotations()
	if anno == nil {
		anno = map[string]string{}
	} else {
		if v, ok := anno[label.AnnTiDBDeleteSlots]; ok {
			if v != "" {
				err := json.Unmarshal([]byte(v), &slice)
				if err != nil {
					return fmt.Errorf("[%s/%s] deletePod unmarshal value %s failed: %s", largeTc.Namespace, largeTc.Name, v, err)
				}
			}
		}
	}
	indexNum, _ := strconv.Atoi(index)
	slice = append(slice, indexNum)
	sort.Ints(slice)
	s, err := json.Marshal(slice)
	if err != nil {
		return fmt.Errorf("[%s/%s] deletePod marshal slice %v failed when StopLargeTc: %s", largeTc.Namespace, largeTc.Name, slice, err)
	}
	anno[label.AnnTiDBDeleteSlots] = string(s)
	largeTc.Annotations = anno

	largeTc.Spec.TiDB.Replicas = largeTc.Spec.TiDB.Replicas - 1
	err = utils.UpdateTC(largeTc, tidbv1.TiDBMemberType, false)
	if err != nil {
		klog.Errorf("[%s/%s] deletePod Update large TidbClusters failed", largeTc.Namespace, largeTc.Name)
		return err
	}
	return nil
}

func StartLargeTc(clusName, ns string) (string, error) {
	largeTCName := clusName + "-large"
	var svcName string
	var index string
	largeTc, err := sldbcluster.SldbClient.PingCapLister.TidbClusters(ns).Get(largeTCName)
	if err != nil && !errors.IsNotFound(err) {
		klog.Errorf("[%s/%s] get TidbClusters failed", ns, clusName)
		return svcName, err
	}
	if errors.IsNotFound(err) {
		klog.Infof("[%s/%s]------------come to create newtc------------\n", ns, largeTCName)
		index = "0"
		largeTc, err := CreateLargeTc(clusName, ns, largeTCName)
		if err != nil {
			klog.Errorf("[%s/%s] Create large TidbClusters failed", ns, clusName)
			return svcName, err
		}
		oldTc := largeTc.DeepCopy()
		oldTc.Spec.TiDB.Replicas = 0
		podStatus := PodStatusHander(ns, largeTCName, "0")
		if podStatus == true {
			svcName = largeTCName + "-tidb-" + index + "." + largeTCName + "-tidb-" + "peer" + "." + ns
			return svcName, nil
		} else {
			err = deletePod(largeTc, index)
			if err != nil {
				klog.Errorf("[%s/%s] deletePod failed when pod not ready", ns, clusName)
				return svcName, err
			}
			return svcName, fmt.Errorf("the largetc pod not ready")
		}

	} else {
		klog.Infof("[%s/%s]-----already have tc-------\n", ns, largeTCName)
		tcStatus := NormalOrNot(largeTCName, ns)
		if tcStatus == false {
			return svcName, fmt.Errorf("the large TC status is not normal")
		}
		index, anno, err := GetAnnoIndex(largeTc)
		if err != nil || index == "" {
			klog.Errorf("[%s/%s] GetAnnoIndex failed: %s. the index is %s", ns, clusName, err, index)
			return svcName, err
		}
		largeTc.Annotations = anno
		largeTc.Spec.TiDB.Replicas = largeTc.Spec.TiDB.Replicas + 1
		err = utils.UpdateTC(largeTc, tidbv1.TiDBMemberType, false)
		if err != nil {
			klog.Errorf("[%s/%s] Update large TidbClusters failed", ns, clusName)
			return svcName, err
		}
		podStatus := PodStatusHander(ns, largeTCName, index)
		if podStatus == true {
			svcName := largeTCName + "-tidb-" + index + "." + largeTCName + "-tidb-" + "peer" + "." + ns
			return svcName, nil
		} else {
			klog.Infof("[%s/%s]pod-----status not ok-------%s---%s\n", ns, largeTCName, svcName, index)
			err = deletePod(largeTc, index)
			if err != nil {
				klog.Errorf("[%s/%s] deletePod failed when pod not ready", ns, clusName)
				return svcName, err
			}
			klog.Infof("[%s/%s]-----finish delete-------%s----%s\n", ns, largeTCName, svcName, index)
			return svcName, fmt.Errorf("the largetc pod not ready")
		}
	}

}

func StopLargeTc(clusName, ns, addr string) error {
	largeTCName := clusName + "-large"
	largeTc, err := sldbcluster.SldbClient.PingCapLister.TidbClusters(ns).Get(largeTCName)
	if err != nil {
		klog.Errorf("[%s/%s] get TidbClusters failed", ns, clusName)
		return err
	}

	tcStatus := NormalOrNot(largeTCName, ns)
	if tcStatus == false {
		return fmt.Errorf("the large TC status is not normal")
	}

	index := strings.Split(addr, ".")[0]
	index = strings.Split(index, "-")[2]
	err = deletePod(largeTc, index)
	if err != nil {
		klog.Errorf("[%s/%s] deletePod failed when pod not ready", ns, clusName)
		return err
	}

	return nil

}
