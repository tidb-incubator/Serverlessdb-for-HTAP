/*
 *
 *  Copyright 2019.  The CloudDB Authors
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 *
 */
package utils

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	apps "github.com/pingcap/advanced-statefulset/client/apis/apps/v1"
	"github.com/pingcap/tidb-operator/pkg/label"
	operatorUtils "github.com/pingcap/tidb-operator/pkg/util"
	"github.com/tidb-incubator/Serverlessdb-for-HTAP/pkg/sldb-operator/util"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/selection"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/klog"
	"reflect"

	//"k8s.io/kubernetes/pkg/api/v1/resource"
	tcv1 "github.com/pingcap/tidb-operator/pkg/apis/pingcap/v1alpha1"
	sldbcluster "github.com/tidb-incubator/Serverlessdb-for-HTAP/pkg/scale-operator/sldbcluster"
	webClient "github.com/tidb-incubator/Serverlessdb-for-HTAP/pkg/scale-operator/web"
	"github.com/tidb-incubator/Serverlessdb-for-HTAP/pkg/sldb-operator/apis/bcrds/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/client-go/util/retry"
	"math"
	"os"
	"os/exec"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	HashratePerTidb     = 1
	ScalerOut           = 1
	ScalerIn            = 2
	ZeroHashrate        = 0.0
	MinHashraterPerTidb = 0.5
	NormalHashrate1     = 1.0
	NormalHashrate2     = 2.0
	NormalHashrate3     = 4.0
	NormalHashrate4     = 8.0
	NormalHashrate5     = 16.0
)

const (
	// AnnTiDBLastAutoScalingTimestamp is annotation key of tidbcluster to indicate the last timestamp for tidb auto-scaling
	AnnTiDBLastAutoScalingTimestamp = "tidb.tidb.pingcap.com/last-autoscaling-timestamp"
	// AnnTiKVLastAutoScalingTimestamp is annotation key of tidbclusterto which ordinal is created by tikv auto-scaling
	AnnTiKVLastAutoScalingTimestamp = "tikv.tidb.pingcap.com/last-autoscaling-timestamp"
	// AnnLastSyncingTimestamp records last sync timestamp
	AnnLastSyncingTimestamp = "tidb.pingcap.com/last-syncing-timestamp"
)

type ScalerData struct {
	ScalerNeedCore float64
	ScalerCurtime  int64
	FirstExpandTime int64
	ScalerFlag     int
}
const (
	TP string = "tp"
	AP string = "ap"
	BIGAP string = "large"
	RoleInstanceLabelKey string = "bcrds.cmss.com/role"
)

var AllScalerOutData = make(map[string]*ScalerData)
var GMapMutex sync.Mutex

func UpdateLastData(name string, namesp string, data *ScalerData, ty int) {
	key := name + "-" + namesp
	GMapMutex.Lock()
	var ptr *ScalerData
	if v, ok := AllScalerOutData[key]; !ok {
		ptr = &ScalerData{}
		AllScalerOutData[key] = ptr
		ptr.ScalerFlag = ScalerOut
		ptr.ScalerCurtime = time.Now().Unix()
	} else {
		ptr = v
	}
	ptr.ScalerCurtime = data.ScalerCurtime
	if ty == ScalerOut {
		//60s find load fall to  low level，ScalerIn
		if ptr.FirstExpandTime != 0 &&  ptr.ScalerCurtime - ptr.FirstExpandTime > 60 {
			if data.ScalerNeedCore < ptr.ScalerNeedCore {
				ptr.ScalerNeedCore = data.ScalerNeedCore
				ptr.ScalerFlag = ScalerIn
			}
			ptr.FirstExpandTime = 0
		} else {
			if ptr.ScalerNeedCore < data.ScalerNeedCore {
				ptr.ScalerNeedCore = data.ScalerNeedCore
			}
		}
	} else {
		ptr.ScalerFlag = ty
		ptr.ScalerNeedCore = data.ScalerNeedCore
	}
	GMapMutex.Unlock()
}

func CleanHashrate(tc *tcv1.TidbCluster,scaletype string) {
	key := tc.Name + "-" + scaletype + "-" + tc.Namespace
	GMapMutex.Lock()
	if v, ok := AllScalerOutData[key]; ok {
		v.ScalerCurtime = time.Now().Unix()
		v.ScalerNeedCore = 0
		v.ScalerFlag = ScalerOut
	}
	GMapMutex.Unlock()
}

func OneHashrate(tc *tcv1.TidbCluster,scaletype string) {
	key := tc.Name + "-" + scaletype +"-" + tc.Namespace
	GMapMutex.Lock()
	if v, ok := AllScalerOutData[key]; ok {
		v.ScalerNeedCore = 1.0
		v.ScalerFlag = ScalerOut
		v.ScalerCurtime = time.Now().Unix()
	}
	GMapMutex.Unlock()
}

func ChangeScalerStatus(name string, namesp string) {
	key := name + "-" + namesp
	GMapMutex.Lock()
	if v, ok := AllScalerOutData[key]; ok {
		v.ScalerFlag = ScalerOut
		v.ScalerCurtime = time.Now().Unix()
	}
	GMapMutex.Unlock()
}

func GetLastData(name string, namesp string) (ScalerData, error) {
	key := name + "-" + namesp
	GMapMutex.Lock()
	if v, ok := AllScalerOutData[key]; !ok {
		var ptr = &ScalerData{}
		AllScalerOutData[key] = ptr
		ptr.ScalerFlag = ScalerOut
		ptr.ScalerCurtime = time.Now().Unix()
		GMapMutex.Unlock()
		return ScalerData{}, fmt.Errorf("get data failed")
	} else {
		tmp := *v
		GMapMutex.Unlock()
		return tmp, nil
	}
}
func CleanScalerMap(name string, namesp string) {
	key := name + "-" + namesp
	GMapMutex.Lock()
	delete(AllScalerOutData, key)
	GMapMutex.Unlock()
}


type DBStatus struct {
	Cluster         string `json:"cluster"`
	Address         string `json:"address"`
	Type            string `json:"type"`
	Status          string `json:"status"`
	LastPing        string `json:"laste_ping"`
	MaxConn         int    `json:"max_conn"`
	IdleConn        int    `json:"idle_conn"`
	CacheConn       int    `json:"cache_conn"`
	PushConnCount   int64  `json:"push_conn_count"`
	PopConnCount    int64  `json:"pop_conn_count"`
	UsingConnsCount int64  `json:"using_conn_count"`
}

var rtpasswd = os.Getenv("ROOT_PASSWORD")
var SplitReplicas = os.Getenv("SPLIT_REPLICAS")
var TwoPattern = "2"
var MaxHashrate = os.Getenv("MAX_HASHRATE")


func GetSldb(clus, ns string) (*v1alpha1.ServerlessDB, error) {
	sldb, err := sldbcluster.SldbClient.Client.BcrdsV1alpha1().ServerlessDBs(ns).Get(clus, metav1.GetOptions{})
	if err != nil {
		klog.Errorf("sldb %v/%v err is %v", ns, clus, err)
	}
	return sldb, err
}

//transfer hashrate to tidb replicas
func TransHashrateToReplica(hashrate int) int {
	return int(math.Ceil(float64(hashrate) / HashratePerTidb))
}

//calcute max hashrate based on metric.
func CalcMaxPerfResource(metrics v1alpha1.Metric) int {
	if metrics.HashRate != "" {
		maxHashrate, _ := strconv.Atoi(metrics.HashRate)
		return maxHashrate
	} else {
		return 8
	}
}

//caculated resource cannot exceed cluster limit.
func CompareResource(metrics v1alpha1.Metric, expectTotal int) int {
	limit := CalcMaxPerfResource(metrics)
	if limit >= expectTotal {
		return expectTotal
	}
	return limit
}

func MergeSlice(slice1 []int, slice2 []int) []int {
	var sliceMap = make(map[int]bool)
	for _, v := range slice1 {
		sliceMap[v] = true
	}
	for _, v := range slice2 {
		sliceMap[v] = true
	}
	var slice []int
	for k, _ := range sliceMap {
		slice = append(slice, k)
	}
	sort.Ints(slice)
	return slice
}

func UpdateAnno(tc *tcv1.TidbCluster, component string, actualReplica, targetReplica int) (map[string]string, error) {
	var key string
	anno := tc.GetAnnotations()
	if anno == nil {
		anno = map[string]string{}
	}
	if component == label.PDLabelVal {
		key = label.AnnPDDeleteSlots
	} else if component == label.TiDBLabelVal {
		key = label.AnnTiDBDeleteSlots
	} else if component == label.TiKVLabelVal {
		key = label.AnnTiKVDeleteSlots
	} else if component == label.TiFlashLabelVal {
		key = label.AnnTiFlashDeleteSlots
	} else {
		return anno, fmt.Errorf("[%s/%s] component %s is not found", tc.Namespace, tc.Name, component)
	}
	value, ok := anno[key]
	if !ok && actualReplica <= targetReplica {
		return anno, fmt.Errorf("[%s/%s] key %s is not exist in annotation", tc.Namespace, tc.Name, key)
	}
	slice := make([]int, 0)
	if value != "" {
		err := json.Unmarshal([]byte(value), &slice)
		if err != nil {
			return anno, fmt.Errorf("[%s/%s] unmarshal value %s failed: %s", tc.Namespace, tc.Name, value, err)
		}
	}

	if actualReplica > targetReplica {
		//to do: get tidb pod's sequence of scale in.
		diff := int(actualReplica - targetReplica)
		deleteslots, err := FilterNeedReduceTidbReplicasIndex(tc, diff, component)
		if err != nil {
			return anno, fmt.Errorf("[%s/%s] filter deleted slots %v failed which need reduce: %s", tc.Namespace, tc.Name, deleteslots, err)
		}
		slice = MergeSlice(slice, deleteslots)
		s, err := json.Marshal(slice)
		if err != nil {
			return anno, fmt.Errorf("[%s/%s] marshal slice %v failed when actual is more than target: %s", tc.Namespace, tc.Name, slice, err)
		}
		anno[key] = string(s)

	} else if actualReplica < targetReplica {
		sort.Ints(slice)
		//to remove tidb pod's sequence in anno for scale out.
		diff := targetReplica - actualReplica
		if diff >= len(slice) {
			delete(anno, key)
		} else {
			slice = slice[diff:]
			s, err := json.Marshal(slice)
			if err != nil {
				return anno, fmt.Errorf("[%s/%s] marshal slice %v failed when acutal is less than target: %s", tc.Namespace, tc.Name, slice, err)
			}
			anno[key] = string(s)
		}
	}
	return anno, nil
}

type ChoiceScalerInstance struct {
	ip       string
	port     int32
	trans    int32
	instance string
	idx      int
}

func sortChoiceScalerInstance(arr []ChoiceScalerInstance) {
	for i := 0; i < len(arr)-1; i++ {
		for j := i + 1; j < len(arr); j++ {
			if arr[i].trans > arr[j].trans || (arr[i].trans == arr[j].trans && arr[i].idx < arr[j].idx) {
				arr[i], arr[j] = arr[j], arr[i]
			}
		}
	}
}

func choiceNeedReduceReplicas(tc *tcv1.TidbCluster, normalInstances []string, NeedReduceReplicas int32, component string) []string {
	podList, err := GetK8sPodArray(tc, tcv1.MemberType(component),TP)
	if err != nil {
		klog.Errorf("[%s/%s] k8s query pod failed,needReduce %v", tc.Namespace, tc.Name, normalInstances[0:NeedReduceReplicas:NeedReduceReplicas])
		return normalInstances[0:NeedReduceReplicas:NeedReduceReplicas]
	}
	choiceArr := make([]ChoiceScalerInstance, len(normalInstances))
	klog.Infof("[%s/%s] normalInstances[%v],NeedReduceReplicas[%v]\n", tc.Namespace, tc.Name, normalInstances, NeedReduceReplicas)
	wg := sync.WaitGroup{}
	wg.Add(len(normalInstances))
	for i := 0; i < len(normalInstances); i++ {
		go func(i int) {
			defer wg.Done()
			for _, pod := range podList {
				if pod.Name == normalInstances[i] {
					choiceArr[i].ip = pod.Status.PodIP
					choiceArr[i].instance = normalInstances[i]
					choiceArr[i].idx = i
				OutLoop:
					for _, container := range pod.Spec.Containers {
						if container.Name == "tidb" {
							for _, ports := range container.Ports {
								if ports.Name == "server" {
									choiceArr[i].port = ports.ContainerPort
									break OutLoop
								}
							}
						}
					}
				}
			}
			osCmd := "/usr/bin/mysql -uroot --connect-timeout=5 -P" + strconv.Itoa(int(choiceArr[i].port)) + " -p" + rtpasswd + " -h" +
				choiceArr[i].ip + " -Ne \"select count(*) from INFORMATION_SCHEMA.PROCESSLIST where TxnStart <> ''\""
			cmd1 := exec.Command("/bin/bash", "-c", osCmd)
			var out bytes.Buffer
			cmd1.Stdout = &out
			if err = cmd1.Run(); err != nil {
				klog.Errorf("[%s/%s] connect mysql failed %v", tc.Namespace, tc.Name, err)
				choiceArr[i].trans = 0
			} else {
				strCounts := strings.Fields(out.String())
				for _, count := range strCounts {
					if m, _ := regexp.MatchString("^[0-9]+$", count); m {
						trans, _ := strconv.Atoi(count)
						choiceArr[i].trans = int32(trans)
						break
					}
				}
			}
		}(i)
	}
	wg.Wait()
	//choice min trans to scaler in
	sortChoiceScalerInstance(choiceArr)
	var instance []string
	for i := 0; i < int(NeedReduceReplicas); i++ {
		instance = append(instance, choiceArr[i].instance)
	}
	return instance
}

func indexToInstance(indexarr []int, name string, component string) []string {
	var instance []string
	for _, idx := range indexarr {
		instance = append(instance, name+"-"+component+"-"+strconv.Itoa(idx))
	}
	return instance
}

func FilterNeedReduceTidbReplicas(tc *tcv1.TidbCluster, reduceReplicas int, component string) ([]string, error) {
	sldb := &v1alpha1.ServerlessDB{}
	sldb.Name = tc.Name
	sldb.Namespace = tc.Namespace
	failederr, pendarr, sucessarr, err := FilterFailedAndNoHealthInstance(sldb, tc, tcv1.MemberType(component))
	if err != nil {
		return nil, err
	}

	var reduceInstance []string
	if tcv1.MemberType(component) == tcv1.TiDBMemberType {
		newRep := tc.Spec.TiDB.Replicas
		length := len(sucessarr)
		var deleteInstance []string
		if int32(length) > newRep {
			sucessInstance := indexToInstance(sucessarr, tc.Name, component)
			deleteInstance = choiceNeedReduceReplicas(tc, sucessInstance, int32(length)-newRep, component)
		}
		failedInstance := indexToInstance(failederr, tc.Name, component)
		pendingInstance := indexToInstance(pendarr, tc.Name, component)
		reduceInstance = append(reduceInstance, failedInstance...)
		reduceInstance = append(reduceInstance, pendingInstance...)
		reduceInstance = append(reduceInstance, deleteInstance...)
	} else if tcv1.MemberType(component) == tcv1.TiKVMemberType {
		if int(tc.Spec.TiKV.Replicas)-reduceReplicas < 3 {
			return nil, fmt.Errorf("[%s/%s] tikv replicas %d, reduceRep %d lt 3 replicas", tc.Namespace, tc.Name, int(tc.Spec.TiKV.Replicas), reduceReplicas)
		}
		newRep := tc.Spec.TiKV.Replicas
		length := len(sucessarr)
		if length > int(newRep) {
			return nil, fmt.Errorf("[%s/%s] tikv newRep %d less than sucess replicas,tikv no support scaler in", tc.Namespace, tc.Name, newRep)
		} else {
			failedInstance := indexToInstance(failederr, tc.Name, component)
			pendingInstance := indexToInstance(pendarr, tc.Name, component)
			reduceInstance = append(reduceInstance, failedInstance...)
			reduceInstance = append(reduceInstance, pendingInstance...)
		}
	}
	return reduceInstance, nil
}

func FilterIdx(instances []string) ([]int, error) {
	//排重
	s := sets.String{}
	for _, instance := range instances {
		s.Insert(instance)
	}
	//get idx
	var indexArr []int
	for strIdx, _ := range s {
		strArr := strings.Split(strIdx, "-")
		ilen := len(strArr)
		if ilen != 0 {
			idx, err := strconv.Atoi(strArr[ilen-1])
			if err != nil {
				klog.Errorf("strarr is %v", strArr)
				return nil, err
			}
			indexArr = append(indexArr, idx)
		}
	}
	sort.Ints(indexArr)
	return indexArr, nil
}

func FilterNeedReduceTidbReplicasIndex(tc *tcv1.TidbCluster, reduceReplica int, component string) ([]int, error) {
	instances, err := FilterNeedReduceTidbReplicas(tc, reduceReplica, component)
	if err != nil {
		return nil, err
	}
	fArr, err := FilterIdx(instances)
	return fArr, err
}

func GetK8sAllPodArray(name string, namesp string, sldbType tcv1.MemberType,scalertype string) ([]*corev1.Pod, error) {
	labelkv:= fmt.Sprintf(`%s=%s,%s=%s,%s=%s`, "app.kubernetes.io/component",string(sldbType),
		"bcrds.cmss.com/instance",name,"app.kubernetes.io/managed-by","tidb-operator")
	listopt := metav1.ListOptions{
		LabelSelector: labelkv,
	}
	podlist, err := sldbcluster.SldbClient.KubeCli.CoreV1().Pods(namesp).List(listopt)
	if err != nil {
		return nil, err
	}
	var podarr []*corev1.Pod
	for i, v := range podlist.Items {
		if v.DeletionTimestamp == nil {
			if v,ok := v.Labels["predelete"];ok {
				if v == "true" {
					continue
				}
			}
			if scalertype == TP && strings.Contains(v.Name,name+"-t") == true{
				podarr = append(podarr, &podlist.Items[i])
			} else if strings.Contains(v.Name,name+"-"+scalertype) == true {
				podarr = append(podarr, &podlist.Items[i])
			}
		}
	}
	return podarr, nil
}

func GetK8sPodArray(tc *tcv1.TidbCluster, sldbType tcv1.MemberType,scalertype string) ([]*corev1.Pod, error) {
	labelkv := fmt.Sprintf(`%s=%s,%s=%s,%s=%s`, "app.kubernetes.io/component", string(sldbType), "app.kubernetes.io/instance", tc.Name,
		"app.kubernetes.io/managed-by", "tidb-operator")
	listopt := metav1.ListOptions{
		LabelSelector: labelkv,
	}
	podlist, err := sldbcluster.SldbClient.KubeCli.CoreV1().Pods(tc.Namespace).List(listopt)
	if err != nil {
		return nil, err
	}
	var podarr []*corev1.Pod
	for i, v := range podlist.Items {
		if v.DeletionTimestamp == nil {
			if v,ok := v.Labels["predelete"];ok {
				if v == "true" {
					continue
				}
			}
			nameArr := strings.Split(v.Name,"-")
			name := nameArr[0]
			if scalertype == TP && strings.Contains(v.Name,name+"-t") == true{
				podarr = append(podarr, &podlist.Items[i])
			} else if strings.Contains(v.Name,name+"-"+scalertype) == true {
				podarr = append(podarr, &podlist.Items[i])
			}
		}
	}
	return podarr, nil
}

func GetPodArray(tc *tcv1.TidbCluster, sldbType tcv1.MemberType) ([]*corev1.Pod, error) {
	selector := labels.NewSelector()
	r1, err1 := labels.NewRequirement("app.kubernetes.io/component", selection.Equals, []string{string(sldbType)})
	r2, err2 := labels.NewRequirement("app.kubernetes.io/instance", selection.Equals, []string{tc.Name})
	r3, err3 := labels.NewRequirement("app.kubernetes.io/managed-by", selection.Equals, []string{"tidb-operator"})
	if err1 != nil || err2 != nil || err3 != nil {
		return nil, fmt.Errorf("[%s/%s] podStatusCheck err1 %v,err2 %v,err3 %v", tc.Namespace, tc.Name, err1, err2, err3)
	}
	selector = selector.Add(*r1, *r2, *r3)
	return sldbcluster.SldbClient.KubeInformerFactory.Core().V1().Pods().Lister().Pods(tc.Namespace).List(selector)
}

func PodStatusCheckRollback(sldb *v1alpha1.ServerlessDB, tc *tcv1.TidbCluster, oldTc *tcv1.TidbCluster, sldbType tcv1.MemberType) (map[string]string, int, error) {
	var newRep, oldRep int32
	newTcAnno := tc.Annotations
	oldTcAnno := oldTc.Annotations
	var key string
	var anno = map[string]string{}
	if sldbType == tcv1.TiDBMemberType {
		newRep = tc.Spec.TiDB.Replicas
		oldRep = oldTc.Spec.TiDB.Replicas
		key = label.AnnTiDBDeleteSlots
	} else if sldbType == tcv1.TiKVMemberType {
		newRep = tc.Spec.TiKV.Replicas
		oldRep = oldTc.Spec.TiKV.Replicas
		key = label.AnnTiKVDeleteSlots
	} else {
		return nil, 0, fmt.Errorf("[%s/%s] PodStatusCheckRollback sldbType %v", sldb.Namespace, sldb.Name, sldbType)
	}

	if newRep < oldRep {
		return nil, 0, fmt.Errorf("[%s/%s] newRep lt oldRep", sldb.Namespace, sldb.Name)
	}
	newSlice := make([]int, 0)
	if newstr, ok := newTcAnno[key]; ok {
		err := json.Unmarshal([]byte(newstr), &newSlice)
		if err != nil {
			klog.Errorf("[%s/%s] json Unmarshal newTcAnno failed %s", sldb.Namespace, sldb.Name, newTcAnno)
			return nil, 0, err
		}
	}
	oldSlice := make([]int, 0)
	if oldstr, ok := oldTcAnno[key]; ok {
		err := json.Unmarshal([]byte(oldstr), &oldSlice)
		if err != nil {
			klog.Errorf("[%s/%s] json Unmarshal newTcAnno failed %s", sldb.Namespace, sldb.Name, oldTcAnno)
			return nil, 0, err
		}
	}
	klog.Infof("[%s/%s] newRep %d,oldRep %d", sldb.Namespace, sldb.Name, newRep, oldRep)
	var timeCount int
	var pendCount int
	var failedArr, pendArr, sucessArr []int
	for {
		timeCount++
		time.Sleep(600 * time.Millisecond)
		if timeCount > 200 {
			klog.Infof("[%s/%s] start more than 120s", sldb.Namespace, sldb.Name)
			break
		}
		var currtc *tcv1.TidbCluster
		var err error
		if currtc, err = sldbcluster.SldbClient.PingCapLister.TidbClusters(tc.Namespace).Get(tc.Name); err != nil {
			klog.Infof("[%s/%s] PodStatusCheckRollback  PingCapLister failed %v", sldb.Namespace, sldb.Name, err)
			continue
		}
		failedarr, pendingarr, sucessarr, err := FilterFailedAndNoHealthInstance(sldb, currtc, sldbType)
		if err != nil {
			klog.Infof("[%s/%s] FilterFailedAndNoHealthInstance get pod failed %v", sldb.Namespace, sldb.Name, err)
			continue
		}
		failedArr = failedarr
		pendArr = pendingarr
		sucessArr = sucessarr
		if len(sucessarr) == int(newRep) {
			klog.Infof("[%s/%s] PodStatusCheckRollback newRep sucessArr %v", tc.Namespace, tc.Name, sucessArr)
			break
		} else {
			if len(pendingarr)+len(sucessarr) == int(newRep) {
				pendCount++
				continue
			}
			if len(pendingarr) != 0 && pendCount > 0 {
				pendCount++
			}
			if pendCount > 100 {
				klog.Infof("[%s/%s] pending more than 60s", sldb.Namespace, sldb.Name)
				break
			}
		}
	}
	var addCount int
	if len(sucessArr) > int(oldRep) {
		addCount = len(sucessArr) - int(oldRep)
		if addCount > int(newRep-oldRep) {
			addCount = int(newRep - oldRep)
		}
		var slice1 []int
		slice1 = append(slice1, failedArr...)
		slice1 = append(slice1, pendArr...)
		slice := MergeSlice(slice1, newSlice)
		if len(slice) != 0 {
			s, err := json.Marshal(slice)
			if err != nil {
				klog.Errorf("[%s/%s] json.Marshal slice failed %v", sldb.Namespace, sldb.Name, slice)
				anno = oldTcAnno
				addCount = 0
			} else {
				anno[key] = string(s)
			}
		}
		// dns check
		if podlist, err := GetK8sPodArray(tc, sldbType,TP); err == nil {
			wg := sync.WaitGroup{}
			klog.Infof("[%s/%s] PodStatusCheckRollback sucessArr %v", tc.Namespace, tc.Name, sucessArr)
			for _, v := range sucessArr {
				for j, pod := range podlist {
					if pod.Name == tc.Name+"-tidb-"+strconv.Itoa(v) {
						wg.Add(1)
						go func(j int) {
							defer wg.Done()
							err := DnsCheck(podlist[j])
							if err != nil {
								klog.Errorf("[%s/%s] PodStatusCheckRollback DnsCheck check failed %v", podlist[j].Namespace, podlist[j].Name, err)
							} else {
								klog.Infof("[%s/%s] PodStatusCheckRollback DnsCheck check success", podlist[j].Namespace, podlist[j].Name)
							}
						}(j)
					}
				}
			}
			wg.Wait()
		}
	} else {
		anno = oldTcAnno
		addCount = 0
	}
	return anno, addCount, nil
}

func UpdateTC(tc *tcv1.TidbCluster, sldbType tcv1.MemberType, auto bool) error {
	var err error
	// don't wait due to limited number of clients, but backoff after the default number of steps
	newReplicas := tc.Spec.TiDB.Replicas
	var oldv string
	var oldResource resource.Quantity
	if sldbType == tcv1.TiDBMemberType {
		if tc.Annotations != nil {
			if v, ok := tc.Annotations[label.AnnTiDBDeleteSlots]; ok {
				oldv = v
			}
		}
	} else if sldbType == tcv1.TiKVMemberType {
		newReplicas = tc.Spec.TiKV.Replicas
		if tc.Annotations != nil {
			if v, ok := tc.Annotations[label.AnnTiKVDeleteSlots]; ok {
				oldv = v
			}
		}
		oldResource = tc.Spec.TiKV.ResourceRequirements.Requests[corev1.ResourceStorage]
	}
	err = retry.RetryOnConflict(retry.DefaultRetry, func() error {
		if auto && sldbType == tcv1.TiDBMemberType {
			if name, ok := tc.Labels[util.InstanceLabelKey]; ok {
				sldbName := strings.Split(name, "-")
				sldb, err := sldbcluster.SldbClient.Lister.ServerlessDBs(tc.Namespace).Get(sldbName[0])
				if err != nil {
					return err
				}
				if len(sldb.Status.Rule) != 0 {
					klog.Infof("[%s/%s] serverlessdb in rule,stop auto scaler", tc.Namespace, tc.Name)
					return fmt.Errorf("[%s/%s] serverlessdb in rule,stop auto scaler", tc.Namespace, tc.Name)
				}
			}
		}

		var updateErr error
		_, updateErr = sldbcluster.SldbClient.PingCapCli.PingcapV1alpha1().TidbClusters(tc.Namespace).Update(tc)
		if updateErr == nil {
			klog.Infof("[%s/%s] updated successfully, rep %d, tidb reps %d, type %v", tc.Namespace, tc.Name, newReplicas, tc.Spec.TiDB.Replicas, sldbType.String())
			return nil
		}
		klog.V(4).Infof("[%s/%s] failed to update SldbCluster error: %v", tc.Namespace, tc.Name, updateErr)
		if tc, err = sldbcluster.SldbClient.PingCapLister.TidbClusters(tc.Namespace).Get(tc.Name); err == nil {
			// make a copy so we don't mutate the shared cache
			if sldbType == tcv1.TiDBMemberType {
				tc.Spec.TiDB.Replicas = newReplicas
				if oldv != "" {
					if tc.Annotations == nil {
						tc.Annotations = map[string]string{}
					}
					tc.Annotations[label.AnnTiDBDeleteSlots] = oldv
				}
			} else if sldbType == tcv1.TiKVMemberType {
				tc.Spec.TiKV.Replicas = newReplicas
				if oldv != "" {
					if tc.Annotations == nil {
						tc.Annotations = map[string]string{}
					}
					tc.Annotations[label.AnnTiKVDeleteSlots] = oldv
				}
				res := tc.Spec.TiKV.ResourceRequirements.Requests[corev1.ResourceStorage]
				if res.MilliValue() < oldResource.MilliValue() {
					tc.Spec.TiKV.ResourceRequirements.Requests[corev1.ResourceStorage] = oldResource
				}
			}
		} else {
			if errors.IsNotFound(err) {
				return err
			}
			utilruntime.HandleError(fmt.Errorf("[%s/%s] error getting updated SldbCluster from lister: %v", tc.Namespace, tc.Name, err))
		}
		return updateErr
	})
	if err != nil {
		klog.Errorf("[%s/%s] failed to update SldbClusterAutoScaler error: %v", tc.Namespace, tc.Name, err)
		return err
	}
	return nil
}

func FilterFailedAndNoHealthInstance(sldb *v1alpha1.ServerlessDB, tc *tcv1.TidbCluster, sldbType tcv1.MemberType) ([]int, []int, []int, error) {
	var failedInstance, pendingInstance, sucessInstance []string
	var podlist []*corev1.Pod
	var err error
	if podlist, err = GetK8sPodArray(tc, sldbType,TP); err != nil {
		return nil, nil, nil, err
	}
	if sldbType == tcv1.TiDBMemberType {
		for _, pod := range podlist {
			if _, existed := tc.Status.TiDB.FailureMembers[pod.Name]; existed {
				failedInstance = append(failedInstance, pod.Name)
			} else {
				if pod.Status.Phase == corev1.PodPending {
					pendingInstance = append(pendingInstance, pod.Name)
				} else if IsPodReady(pod) == true {
					sucessInstance = append(sucessInstance, pod.Name)
				}
			}
		}
	} else if sldbType == tcv1.TiKVMemberType {
		for _, pod := range podlist {
			for k, v := range tc.Status.TiKV.Stores {
				if _, ok := tc.Status.TiKV.FailureStores[k]; ok {
					failedInstance = append(failedInstance, v.PodName)
				} else {
					if pod.Status.Phase == corev1.PodPending {
						pendingInstance = append(pendingInstance, pod.Name)
					} else if IsPodReady(pod) == true {
						sucessInstance = append(sucessInstance, pod.Name)
					}
				}
			}
		}
	}
	failedarr, err1 := FilterIdx(failedInstance)
	pendingarr, err2 := FilterIdx(pendingInstance)
	sucessarr, err3 := FilterIdx(sucessInstance)
	if err1 != nil || err2 != nil || err3 != nil {
		return failedarr, pendingarr, sucessarr, fmt.Errorf("[%s/%s] err1 %v err2 %v err3 %v", tc.Namespace, tc.Name, err1, err2, err3)
	}
	return failedarr, pendingarr, sucessarr, nil
}

func PvcExpand(tc *tcv1.TidbCluster) {
	resourceRq := tc.Spec.TiKV.ResourceRequirements
	if v, ok := resourceRq.Requests[corev1.ResourceStorage]; ok {
		v.Add(resource.MustParse("20Gi"))
		tc.Spec.TiKV.ResourceRequirements.Requests[corev1.ResourceStorage] = v
	}
}
func IsPodReady(pod *corev1.Pod) bool {
	condition := getPodReadyCondition(&pod.Status)
	return condition != nil && condition.Status == corev1.ConditionTrue
}

func getPodReadyCondition(status *corev1.PodStatus) *corev1.PodCondition {
	for i := range status.Conditions {
		if status.Conditions[i].Type == corev1.PodReady {
			return &status.Conditions[i]
		}
	}
	return nil
}

func DnsCheck(pod *corev1.Pod) error {
	DNSTimeout := int64(90)
	tcName := pod.Labels["app.kubernetes.io/instance"]
	name := pod.Name + "." + tcName + "-tidb-peer" + "." + pod.Namespace
	dnscheck := fmt.Sprintf(`
      TIMEOUT_READY=%d
      while ( ! nslookup %s  || ! mysql -uroot -h%s -p%s  -P4000 --connect-timeout=2 -e "select 1" )
      do
         # If TIMEOUT_READY is 0 we should never time out and exit 
         TIMEOUT_READY=$(( TIMEOUT_READY-1 ))
                     if [ $TIMEOUT_READY -eq 0 ];
           then
               echo "Timed out waiting for DNS entry"
               exit 1
           fi
         sleep 1
      done`, DNSTimeout, name, name, rtpasswd)
	cmd := exec.Command("/bin/sh", "-c", dnscheck)
	cmdReader, err := cmd.StdoutPipe()
	if err != nil {
		klog.Infof("[%s/%s] Error StdoutPipe for Cmd %v", pod.Namespace, tcName, err)
		return fmt.Errorf("StdoutPipe failed")
	}
	defer cmdReader.Close()
	cmd.Stderr = cmd.Stdout
	scanner := bufio.NewScanner(cmdReader)
	go func() {
		for scanner.Scan() {
			str := scanner.Text()
			fmt.Printf("[%s/%s] dns check | %s\n", pod.Namespace, tcName, str)
		}
	}()
	err = cmd.Start()
	if err != nil {
		return err
	}
	err = cmd.Wait()
	return err
}

func PendingHander(sldb *v1alpha1.ServerlessDB, tc *tcv1.TidbCluster, oldTc *tcv1.TidbCluster, sldbType tcv1.MemberType, autoscale bool) error {
	var delAnno map[string]string
	var newCount int
	var err error
	if sldbType == tcv1.TiDBMemberType {
		if tc.Spec.TiDB.Replicas > oldTc.Spec.TiDB.Replicas {
			delAnno, newCount, err = PodStatusCheckRollback(sldb, tc, oldTc, sldbType)
			if err != nil {
				return err
			}
			if tc.Spec.TiDB.Replicas != oldTc.Spec.TiDB.Replicas+int32(newCount) {
				klog.Infof("[%s/%s] auto-scaling sldb tidb from %d to %d Failed rollback newCount %d", tc.Namespace, tc.Name, oldTc.Spec.TiDB.Replicas, tc.Spec.TiDB.Replicas, newCount)
				tc.Spec.TiDB.Replicas = oldTc.Spec.TiDB.Replicas + int32(newCount)
				tc.Annotations = delAnno
				if tc.Annotations == nil {
					tc.Annotations = map[string]string{}
				}
				klog.Errorf("[%s/%s] tidb Failed pending,need wait 15 mintues next check", tc.Namespace, tc.Name)
				tc.Annotations["tidb-resource-problem"] = "true"
				err = UpdateTC(tc, sldbType, autoscale)
				if err != nil {
					return err
				}
			}
		}
		// has new elem add or elem del
	} else if sldbType == tcv1.TiKVMemberType {
		if tc.Spec.TiKV.Replicas > oldTc.Spec.TiKV.Replicas {
			delAnno, newCount, err = PodStatusCheckRollback(sldb, tc, oldTc, sldbType)
			if err != nil {
				return err
			}
			if tc.Spec.TiKV.Replicas != oldTc.Spec.TiKV.Replicas+int32(newCount) {
				klog.Infof("[%s/%s] auto-scaling sldb tikv from %d to %d Failed rollback\n", tc.Namespace, tc.Name, oldTc.Spec.TiKV.Replicas, tc.Spec.TiKV.Replicas)
				tc.Spec.TiKV.Replicas = oldTc.Spec.TiKV.Replicas + int32(newCount)
				tc.Annotations = delAnno
				if tc.Annotations == nil {
					tc.Annotations = map[string]string{}
				}
				klog.Errorf("[%s/%s] tikv Failed pending,need wait 15 mintues next check", tc.Namespace, tc.Name)
				tc.Annotations["tikv-resource-problem"] = "true"
				if newCount == 0 {
					PvcExpand(tc)
					time.Sleep(time.Minute)
				}
				err = UpdateTC(tc, sldbType, autoscale)
				if err != nil {
					return err
				}
			}
		}
		msg := ""
		if newCount != 0 {
			msg = fmt.Sprintf("[%s/%s] auto-scaling sldb tikv from %d to %d", tc.Namespace, tc.Name, tc.Spec.TiKV.Replicas-int32(newCount), tc.Spec.TiKV.Replicas)
			klog.Infoln(msg)
		}
	}
	return nil
}

func ResourceProblemCheck(sldb *v1alpha1.ServerlessDB, tcArr *TClus, sldbType tcv1.MemberType) (bool, error) {
	key := string(sldbType) + "-resource-problem"
	if tcArr.NewHashRate < tcArr.OldHashRate ||
		tcArr.NewTc[0].Tc.Spec.TiKV.Replicas < tcArr.OldTc[0].Tc.Spec.TiKV.Replicas {
		if tcArr.NewTc[0].Tc.Annotations == nil {
			tcArr.NewTc[0].Tc.Annotations = map[string]string{}
		}
		tcArr.NewTc[0].Tc.Annotations[key] = "false"
		return true, nil
	} else {
		// resource problem wait 15 mintues
		findFlag := 0
		for i := range tcArr.NewTc {
			if v, ok := tcArr.NewTc[i].Tc.Annotations[key]; ok {
				if v == "true" {
					findFlag = 1
					break
				}
			}
		}
		if findFlag == 1 {
			//resource problem wait 15 minutes
			intervalSeconds := 900
			oldSldb, err := sldbcluster.SldbClient.Lister.ServerlessDBs(sldb.Namespace).Get(sldb.Name)
			if err != nil {
				return false, err
			}
			ableToScale, err := CheckStsAutoScalingInterval(oldSldb, int32(intervalSeconds), sldbType)
			if err != nil {
				return false, err
			}
			if ableToScale == true {
				if tcArr.NewTc[0].Tc.Annotations == nil {
					tcArr.NewTc[0].Tc.Annotations = map[string]string{}
				}
				tcArr.NewTc[0].Tc.Annotations[key] = "false"
				for i := range tcArr.NewTc {
					if _, ok := tcArr.NewTc[i].Tc.Annotations[key]; ok {
						tcArr.NewTc[i].Tc.Annotations[key] = "false"
					}
				}
			} else {
				return false, nil
			}
		}
	}
	return true, nil
}

// CheckStsAutoScalingInterval would check whether there is enough interval duration between every two auto-scaling
func CheckStsAutoScalingInterval(sldb *v1alpha1.ServerlessDB, intervalSeconds int32, memberType tcv1.MemberType) (bool, error) {
	var lastAutoScalingTimestamp string
	var existed bool
	if memberType == tcv1.TiDBMemberType {
		lastAutoScalingTimestamp, existed = sldb.Annotations[AnnTiDBLastAutoScalingTimestamp]
	} else if memberType == tcv1.TiKVMemberType {
		lastAutoScalingTimestamp, existed = sldb.Annotations[AnnTiKVLastAutoScalingTimestamp]
	}
	if !existed {
		return true, nil
	}
	t, err := strconv.ParseInt(lastAutoScalingTimestamp, 10, 64)
	if err != nil {
		return false, fmt.Errorf("[%s/%s] parse last auto-scaling timestamp failed,err:%v", sldb.Namespace, sldb.Name, err)
	}
	if intervalSeconds > int32(time.Now().Sub(time.Unix(t, 0)).Seconds()) {
		return false, nil
	}
	return true, nil
}

type SigleTc struct {
	Tc              tcv1.TidbCluster
	HashratePerTidb float64
	Replicas        int32
}

type TClus struct {
	OldTc       []SigleTc
	NewTc       []SigleTc
	OldHashRate float64
	NewHashRate float64
}

func GetTcArrayTP(sldb *v1alpha1.ServerlessDB) ([]*tcv1.TidbCluster, error) {
	selector := labels.NewSelector().Add(*util.LabelEq(util.BcRdsInstanceLabelKey, sldb.Name))
	list,err := sldbcluster.SldbClient.PingCapLister.TidbClusters(sldb.Namespace).List(selector)
	if err != nil {
		return list,err
	}
	var tpTcArr  []*tcv1.TidbCluster
	for _,v := range list {
		if v.Name == sldb.Name+"-"+TP  ||
			v.Name == sldb.Name {
			tpTcArr = append(tpTcArr,v.DeepCopy())
		}
	}
	return tpTcArr,nil
}

func GetTcArrayAP(sldb *v1alpha1.ServerlessDB) ([]*tcv1.TidbCluster,*tcv1.TidbCluster,error) {
	selector := labels.NewSelector().Add(*util.LabelEq(util.BcRdsInstanceLabelKey, sldb.Name))
	list,err := sldbcluster.SldbClient.PingCapLister.TidbClusters(sldb.Namespace).List(selector)
	if err != nil {
		return list,nil,err
	}
	var apTcArr  []*tcv1.TidbCluster
	var tc *tcv1.TidbCluster
	for _,v := range list {
		if v.Name == sldb.Name+"-"+AP {
			apTcArr = append(apTcArr,v.DeepCopy())
		} else if v.Name == sldb.Name {
			tc = v.DeepCopy()
		}
	}
	return apTcArr,tc,nil
}

func GetHashRate(tc *tcv1.TidbCluster) (SigleTc, error) {
	var sigle SigleTc
	sigle.Tc = *tc
	cpustr := tc.Spec.TiDB.Requests.Cpu().String()
	cpuarr := strings.Split(cpustr, "m")
	if len(cpuarr) == 2 {
		v, err := strconv.Atoi(cpuarr[0])
		if err != nil {
			cpustr = "0.5"
		} else {
			if v < 1000 {
				cpustr = "0.5"
			} else if v < 2000 {
				cpustr = "1.0"
			} else if v < 4000 {
				cpustr = "2.0"
			} else if v < 8000 {
				cpustr = "4.0"
			} else if v < 16000 {
				cpustr = "8.0"
			} else {
				cpustr = "16.0"
			}
		}
	}
	v, err := strconv.ParseFloat(cpustr, 64)
	if err != nil {
		klog.Errorf("[%s/%s] getHashRate failed err %v cpu %s", tc.Namespace, tc.Name, err, cpustr)
		return sigle, err
	}
	sigle.HashratePerTidb = v * HashratePerTidb
	sigle.Replicas = tc.Spec.TiDB.Replicas
	return sigle, nil
}

func createTC(existClusterMap map[string]*tcv1.TidbCluster, name, tidbtype string,tc *tcv1.TidbCluster,limit corev1.ResourceList) (*tcv1.TidbCluster,error) {
	var newtc *tcv1.TidbCluster
	if v, ok := existClusterMap[name]; !ok {
		newtc = &tcv1.TidbCluster{}
		newtc.Name = name
		newtc.Namespace = tc.Namespace
		if newtc.Labels == nil {
			newtc.Labels = make(map[string]string)
		}
		newtc.Labels = util.New().Instance(name).BcRdsInstance(tc.Name)
		newtc.Spec.TiDB = tc.Spec.TiDB.DeepCopy()
		newtc.Spec.TiDB.Labels[RoleInstanceLabelKey] = tidbtype
		if tidbtype == AP {
			newtc.Spec.TiDB.Replicas = 1
		} else {
			newtc.Spec.TiDB.Replicas = 0
		}
		newtc.Spec.TiDB.Limits = limit
		newtc.Spec.Version = tc.Spec.Version
		newtc.Spec.TiDB.Service = nil
		newtc.Spec.TiDB.StorageClassName = tc.Spec.TiDB.StorageClassName
		newtc.Spec.SchedulerName = tc.Spec.SchedulerName
		newtc.Spec.TiDB.Requests = limit.DeepCopy()
		newtc.Spec.Annotations = map[string]string{
			util.InstanceAnnotationKey: tc.Name,
		}
		newtc.Spec.Cluster = &tcv1.TidbClusterRef{
			Name:      tc.Name,
			Namespace: tc.Namespace,
		}
		// set owner references
		newtc.OwnerReferences = tc.OwnerReferences
		_, err := sldbcluster.SldbClient.PingCapCli.PingcapV1alpha1().TidbClusters(tc.Namespace).Create(newtc)
		if err != nil {
			if errors.IsAlreadyExists(err) {
				newtc, err = sldbcluster.SldbClient.PingCapCli.PingcapV1alpha1().TidbClusters(tc.Namespace).Get(name, metav1.GetOptions{})
				if err != nil {
					klog.Errorf("[%s/%s] get TidbClusters failed", tc.Namespace, name)
					return nil,err
				}
			} else {
				klog.Errorf("[%s/%s] create TidbClusters failed", tc.Namespace, name)
				return nil,err
			}
		}
	} else {
		newtc = v.DeepCopy()
	}
	return newtc,nil
}

func getMutilTC(tcArray []*tcv1.TidbCluster,tcArr *TClus) error {
	for i,vtc := range tcArray {
		sigle, err := GetHashRate(vtc)
		if err != nil {
			return err
		}
		tcArr.OldTc = append(tcArr.OldTc, sigle)
		tcArr.NewTc = append(tcArr.NewTc, sigle)
		tcArr.NewTc[i].Tc = *sigle.Tc.DeepCopy()
		tcArr.OldHashRate = tcArr.OldHashRate + tcArr.OldTc[i].HashratePerTidb*float64(tcArr.OldTc[i].Replicas)
		tcArr.NewHashRate = tcArr.OldHashRate
	}
	return nil
}

func createNoExistAllTcTP(sldb *v1alpha1.ServerlessDB, existClusterMap map[string]*tcv1.TidbCluster, tcArr *TClus, tc *tcv1.TidbCluster) error {
	var newtc *tcv1.TidbCluster
	var err error
	var name string
	name = tc.Name +"-"+TP
	var limit = make(corev1.ResourceList)
	cpuLimit := tc.Spec.TiDB.Limits[corev1.ResourceCPU].DeepCopy()
	memLimit := tc.Spec.TiDB.Limits[corev1.ResourceMemory].DeepCopy()
	cpuLimit.Add(cpuLimit)
	memLimit.Add(memLimit)
	limit[corev1.ResourceCPU] = cpuLimit
	limit[corev1.ResourceMemory] = memLimit
	if newtc,err =createTC(existClusterMap,name, TP, tc,limit);err!= nil {
		return err
	}
	var tcArray = []*tcv1.TidbCluster{tc,newtc}
	return getMutilTC(tcArray,tcArr)
}

func createNoExistAllTcAP(sldb *v1alpha1.ServerlessDB, existClusterMap map[string]*tcv1.TidbCluster,tcArr *TClus,tc *tcv1.TidbCluster) error {
	var newtc *tcv1.TidbCluster
	var err error
	memstr := "1Gi"
	newMem, _ := resource.ParseQuantity(memstr)
	var name string
	name = tc.Name +"-" + AP
	limitMem := GetMemory(NormalHashrate3,newMem)
	cpustr := fmt.Sprintf("%g", NormalHashrate3)
	limitCpu, _ := resource.ParseQuantity(cpustr)
	var limit = make(corev1.ResourceList)
	limit[corev1.ResourceCPU] = limitCpu
	limit[corev1.ResourceMemory] = limitMem
	if newtc,err = createTC(existClusterMap,name, AP, tc, limit);err != nil {
		return err
	}
	var tcArray = []*tcv1.TidbCluster{newtc}
	return getMutilTC(tcArray,tcArr)
}

func CloneMutiRevsionTc(sldb *v1alpha1.ServerlessDB,role string) (*TClus,*tcv1.TidbCluster,error) {
	var tclist []*tcv1.TidbCluster
	var err error
	var tc *tcv1.TidbCluster
	if role == TP {
		tclist, err = GetTcArrayTP(sldb)
	} else if role == AP {
		tclist,tc,err = GetTcArrayAP(sldb)
	} else {
		err = fmt.Errorf("[%s/%s] role is no exits %s",sldb.Namespace,sldb.Name,role)
	}
	if err != nil {
		return &TClus{},tc,err
	}
	var existClusMap = make(map[string]*tcv1.TidbCluster)
	var tcArr = &TClus{}
	for i, v := range tclist {
		if strings.Index(v.Name, sldb.Name+"-"+role) != -1 {
			existClusMap[v.Name] = tclist[i]
		} else if role == TP && v.Name == sldb.Name {
			tc = tclist[i]
			existClusMap[v.Name] = tclist[i]
		}
	}
	//add replicas
	if role == TP {
		err = createNoExistAllTcTP(sldb, existClusMap, tcArr, tc)
		if err != nil {
			klog.Errorf("[%s/%s] create no exist TP tc failed: %s", sldb.Namespace, sldb.Name, err)
			return &TClus{},tc,err
		}
	} else {
		err = createNoExistAllTcAP(sldb, existClusMap, tcArr, tc)
		if err != nil {
			klog.Errorf("[%s/%s] create no exist AP tc failed: %s", sldb.Namespace, sldb.Name, err)
			return &TClus{},tc,err
		}
	}
	return tcArr,tc,nil
}

func GetTidbStatus(tcArr *TClus) tcv1.MemberPhase {
	// get problem status
	var tidbStatus = tcv1.NormalPhase
	for _, v := range tcArr.OldTc {
		if reflect.DeepEqual(v.Tc.Status, tcv1.TidbClusterStatus{}) {
			fmt.Println("tc is ", v.Tc.Name, " status is ", v.Tc.Status)
			tidbStatus = tcv1.MemberPhase("noStatus")
			break
		} else {
			if v.Tc.Status.TiDB.Phase != tcv1.NormalPhase {
				tidbStatus = v.Tc.Status.TiDB.Phase
				break
			}
		}
	}
	return tidbStatus
}

func GetMaxHashrate(sldb *v1alpha1.ServerlessDB) float64 {
	hashrate := CalcMaxPerfResource(sldb.Spec.MaxValue.Metric)
	maxHashrate := float64(hashrate)
	return maxHashrate
}

func GetHashrateAndReplicasPerTidb(sldb *v1alpha1.ServerlessDB, totalHashrate float64, currentHashrate float64, autoFlag bool) (float64, int32) {
	fRep, err := strconv.ParseFloat(SplitReplicas, 64)
	if err != nil {
		fRep = 4.0
		klog.Errorf("SplitReplicas Problem %s", SplitReplicas)
	}
	fRep = math.Floor(fRep)
	if FGreater(totalHashrate, GetMaxHashrate(sldb)) {
		totalHashrate = GetMaxHashrate(sldb)
	}
	oneHashrate := totalHashrate / fRep
	var norms = []float64{MinHashraterPerTidb, NormalHashrate1, NormalHashrate2, NormalHashrate3, NormalHashrate4, NormalHashrate5}
	var Hashrate = currentHashrate
	var i = 0
	for i = 0; i < len(norms); i++ {
		if i == 0 {
			if FSmaller(totalHashrate, norms[i]) == true || FEqual(totalHashrate, norms[i]) == true {
				return norms[i], 1
			}
		} else {
			if FSmaller(oneHashrate, norms[i]) == true || FEqual(oneHashrate, norms[i]) == true {
				//低于全规格25%，下向扩容
				if autoFlag == true && FSmaller(norms[i], currentHashrate) == true &&
					(FSmaller(totalHashrate, currentHashrate*fRep*0.25) == true ||
						FEqual(totalHashrate, currentHashrate*fRep*0.25) == true) {
					Hashrate = norms[i]
				} else if autoFlag == true && FSmaller(norms[i], currentHashrate) == true &&
					(FSmaller(totalHashrate, currentHashrate*fRep) == true ||
						FEqual(totalHashrate, currentHashrate*fRep) == true) {
					Hashrate = currentHashrate
				} else {
					Hashrate = norms[i]
				}
				break
			}
		}
	}
	if i == len(norms) {
		return norms[i-1], int32(fRep)
	} else {
		reps := int32(math.Ceil(totalHashrate / Hashrate))
		if reps > int32(fRep) {
			reps = int32(fRep)
		}
		return Hashrate, reps
	}
}
func GetMemory(hashratePerTidb float64,newMem resource.Quantity) resource.Quantity {
	max := int(hashratePerTidb/MinHashraterPerTidb) / 2
	var k int
	for i := 0; k < max;i++ {
		k = int(math.Pow(float64(2), float64(i)))
		newMem.Add(newMem)
	}
	return newMem
}

func RecalculateScale(sldb *v1alpha1.ServerlessDB, tcArr *TClus, autoFlag bool) error {
	var current float64
	var pos = 0
	//AP RecalculateScale
	if len(tcArr.NewTc) == 1 {
		if tcArr.NewHashRate > tcArr.OldHashRate  {
			rep := (tcArr.NewHashRate - tcArr.OldHashRate)/tcArr.NewTc[0].HashratePerTidb
			intRep := int(math.Floor(rep))
			//more than 50% one tidb instances,replicas = replicas+1
			if rep - math.Floor(rep) > 0.5 {
				intRep = intRep + 1
			}
			if intRep != 0 {
				tcArr.NewTc[0].Replicas = tcArr.NewTc[0].Replicas + 1
				tcArr.NewTc[0].Tc.Spec.TiDB.Replicas = tcArr.NewTc[0].Replicas
			}
		} else {
			rep := (tcArr.OldHashRate - tcArr.NewHashRate)/tcArr.NewTc[0].HashratePerTidb
			intRep := int(math.Floor(rep))
			//less than 80% one tidb instances,replicas = replicas-1
			if rep - math.Floor(rep) > 0.8 {
				intRep = intRep + 1
			}
			tcArr.NewTc[0].Replicas = tcArr.NewTc[0].Replicas - int32(intRep)
			if tcArr.NewTc[0].Replicas < 1 {
				tcArr.NewTc[0].Replicas = 1
			}
			tcArr.NewTc[0].Tc.Spec.TiDB.Replicas = tcArr.NewTc[0].Replicas
		}
		return nil
	}
	//tp RecalculateScale
	for i, _ := range tcArr.NewTc {
		if tcArr.NewTc[i].Replicas != 0 {
			current = tcArr.NewTc[i].HashratePerTidb
			pos = i
			break
		}
	}
	hashratePerTidb, reps := GetHashrateAndReplicasPerTidb(sldb, tcArr.NewHashRate, current, autoFlag)
	if FEqual(tcArr.NewTc[pos].HashratePerTidb, hashratePerTidb) == true {
		tcArr.NewTc[pos].Replicas = reps
		tcArr.NewTc[pos].Tc.Spec.TiDB.Replicas = reps
		tcArr.NewTc[1-pos].Replicas = 0
		tcArr.NewTc[1-pos].Tc.Spec.TiDB.Replicas = 0
	} else if FEqual(tcArr.NewTc[1-pos].HashratePerTidb, hashratePerTidb) == true {
		tcArr.NewTc[1-pos].Replicas = reps
		tcArr.NewTc[1-pos].Tc.Spec.TiDB.Replicas = reps
		tcArr.NewTc[pos].Replicas = 0
		tcArr.NewTc[pos].Tc.Spec.TiDB.Replicas = 0
	} else {
		tcArr.NewTc[1-pos].HashratePerTidb = hashratePerTidb
		tcArr.NewTc[1-pos].Tc.Spec.TiDB.Replicas = reps
		//complete cpu resource
		cpustr := fmt.Sprintf("%g", hashratePerTidb)
		newCpu, err := resource.ParseQuantity(cpustr)
		if err != nil {
			klog.Errorf("[%s/%s] err %v cpustr %s", tcArr.NewTc[0].Tc.Namespace, tcArr.NewTc[0].Tc.Name, err, cpustr)
			return err
		}
		memstr := "1Gi"
		//complete memory resource
		var newMem,limitCpu,limitMem resource.Quantity
		newMem, _ = resource.ParseQuantity(memstr)
		var hashrateMode float64
		if FGreater(hashratePerTidb,MinHashraterPerTidb) == true {
			hashrateMode = hashratePerTidb
			limitCpu = newCpu
		} else {
			//0.5 hashrate use 1/4 max resource
			hashrate := CalcMaxPerfResource(sldb.Spec.MaxValue.Metric)
			hashrateMode = math.Ceil(float64(hashrate)/4.0)
			cpustr := fmt.Sprintf("%g", hashrateMode)
			limitCpu, err = resource.ParseQuantity(cpustr)
			if err != nil {
				klog.Errorf("[%s/%s] err %v limit cpustr %s", tcArr.NewTc[0].Tc.Namespace, tcArr.NewTc[0].Tc.Name, err, cpustr)
				return err
			}
		}
		reqsMem := GetMemory(hashratePerTidb,newMem)
		limitMem = GetMemory(hashrateMode,newMem)
		tcArr.NewTc[1-pos].Tc.Spec.TiDB.ResourceRequirements.Limits[corev1.ResourceCPU] = limitCpu
		tcArr.NewTc[1-pos].Tc.Spec.TiDB.ResourceRequirements.Limits[corev1.ResourceMemory] = limitMem
		tcArr.NewTc[1-pos].Tc.Spec.TiDB.ResourceRequirements.Requests[corev1.ResourceCPU] = newCpu
		tcArr.NewTc[1-pos].Tc.Spec.TiDB.ResourceRequirements.Requests[corev1.ResourceMemory] = reqsMem
		tcArr.NewTc[1-pos].Replicas = reps
		tcArr.NewTc[1-pos].Tc.Spec.TiDB.Replicas = reps
		tcArr.NewTc[pos].Replicas = 0
		tcArr.NewTc[pos].Tc.Spec.TiDB.Replicas = 0
	}
	return nil
}

func RecalculateScaleIn(sldb *v1alpha1.ServerlessDB, tcArr *TClus, autoFlag bool) error {
	return RecalculateScale(sldb, tcArr, autoFlag)
}

func RecalculateScaleOut(sldb *v1alpha1.ServerlessDB, tcArr *TClus, autoFlag bool) error {
	return RecalculateScale(sldb, tcArr, autoFlag)
}

func isCompleteRollingUpdate(set *apps.StatefulSet, status *apps.StatefulSetStatus, tc *tcv1.TidbCluster) bool {
	var flag bool
	for _, v := range set.Spec.Template.Spec.Containers {
		if v.Name == "tidb" {
			if tc.Spec.TiDB.Requests.Cpu().String() == v.Resources.Requests.Cpu().String() {
				flag = true
				break
			}
		}
	}
	return set.Spec.UpdateStrategy.Type == apps.RollingUpdateStatefulSetStrategyType &&
		status.UpdatedReplicas == status.Replicas &&
		status.ReadyReplicas == status.Replicas &&
		status.CurrentReplicas == status.UpdatedReplicas &&
		status.CurrentRevision == status.UpdateRevision &&
		flag
}

func UpdateOneTcToNewNorms(Tc *tcv1.TidbCluster, reps int32) error {
	limits := Tc.Spec.TiDB.Limits.DeepCopy()
	requets := Tc.Spec.TiDB.Requests.DeepCopy()
	name := Tc.Name
	namesp := Tc.Namespace
	var err error
	err = retry.RetryOnConflict(retry.DefaultRetry, func() error {
		var tc *tcv1.TidbCluster
		if tc, err = sldbcluster.SldbClient.PingCapLister.TidbClusters(namesp).Get(name); err == nil {
			// make a copy so we don't mutate the shared cache
			tc.Spec.TiDB.Limits = limits.DeepCopy()
			tc.Spec.TiDB.Requests = requets.DeepCopy()
			if tc.Spec.TiDB.Replicas != reps {
				return fmt.Errorf("[%s/%s] only replicas is %d update", namesp, name, reps)
			}
			//clear tidb delete information
			if reps == 0 {
				if tc.Annotations != nil {
					if _,ok:=tc.Annotations[label.AnnTiDBDeleteSlots];ok {
						delete(tc.Annotations,label.AnnTiDBDeleteSlots)
					}
				}
			}
		} else {
			if errors.IsNotFound(err) {
				return err
			}
			utilruntime.HandleError(fmt.Errorf("[%s/%s] error getting updated SldbCluster from lister: %v", namesp, name, err))
		}
		var updateErr error
		_, updateErr = sldbcluster.SldbClient.PingCapCli.PingcapV1alpha1().TidbClusters(tc.Namespace).Update(tc)
		if updateErr == nil {
			klog.Infof("[%s/%s] UpdateTCToNewNorms  updated successfully", tc.Namespace, tc.Name)
			var flag bool
			count := 0
			for {
				time.Sleep(time.Second)
				set, err := sldbcluster.SldbClient.AsCli.AppsV1().StatefulSets(tc.Namespace).Get(operatorUtils.GetStatefulSetName(tc, tcv1.TiDBMemberType), metav1.GetOptions{})
				if err != nil {
					klog.Errorf("[%s/%s] get StatefulSets failed err %v", tc.Namespace, tc.Name, err)
				}
				flag = isCompleteRollingUpdate(set, &set.Status, tc)
				if flag == true || count > 60 {
					break
				}
				count++
			}
			if flag == false {
				klog.Errorf("[%s/%s] update StatefulSets failed more than 60s", tc.Namespace, tc.Name)
				return fmt.Errorf("[%s/%s] update StatefulSets failed more than 60s", tc.Namespace, tc.Name)
			}
			return nil
		}
		klog.V(4).Infof("[%s/%s] failed to update SldbCluster error: %v", tc.Namespace, tc.Name, updateErr)
		return updateErr
	})
	if err != nil {
		klog.Errorf("[%s/%s] failed to update UpdateTCToNewNorms error: %v", namesp, name, err)
		return err
	}
	return nil
}

func UpdateTCToNewNorms(tcArr *TClus) error {
	for i := range tcArr.NewTc {
		if tcArr.OldTc[i].Tc.Spec.TiDB.Replicas == int32(0) &&
			tcArr.NewTc[i].Tc.Spec.TiDB.Replicas != int32(0) &&
			tcArr.OldTc[i].Tc.Spec.TiDB.Requests.Cpu().String() != tcArr.NewTc[i].Tc.Spec.TiDB.Requests.Cpu().String() {
			err := UpdateOneTcToNewNorms(&tcArr.NewTc[i].Tc, 0)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func HanderAllScalerOut(tcArr *TClus, sldb *v1alpha1.ServerlessDB, sldbType tcv1.MemberType, autoscale bool) error {
	if sldbType == tcv1.TiDBMemberType {
		if err := UpdateTCToNewNorms(tcArr); err != nil {
			return err
		}
		wg := sync.WaitGroup{}
		var errArr = make([]error, len(tcArr.NewTc), len(tcArr.NewTc))
		var indexArr []int
		for i := range tcArr.NewTc {
			if tcArr.NewTc[i].Tc.Spec.TiDB.Replicas > tcArr.OldTc[i].Tc.Spec.TiDB.Replicas {
				wg.Add(1)
				indexArr = append(indexArr, i)
				go func(idx int) {
					defer wg.Done()
					if tcArr.NewTc[idx].Tc.Annotations == nil {
						tcArr.NewTc[idx].Tc.Annotations = map[string]string{}
					}
					tcArr.NewTc[idx].Tc.Annotations[string(sldbType)+"-resource-problem"] = "false"
					err := GetNewAnnoAndReplias(sldb, &tcArr.NewTc[idx].Tc, &tcArr.OldTc[idx].Tc, sldbType)
					if err != nil {
						errArr[idx] = err
						return
					}
					err = UpdateTC(&tcArr.NewTc[idx].Tc, sldbType, autoscale)
					if err != nil {
						errArr[idx] = err
						return
					}
					err = PendingHander(sldb, &tcArr.NewTc[idx].Tc, &tcArr.OldTc[idx].Tc, sldbType, autoscale)
					if err != nil {
						errArr[idx] = err
						return
					}
				}(i)
			}
		}
		wg.Wait()
		var hasHashrateAdd bool
		for _, idx := range indexArr {
			if v, ok := tcArr.NewTc[idx].Tc.Annotations[string(sldbType)+"-resource-problem"]; ok {
				if v == "true" {
					tcArr.NewTc[idx].Replicas = tcArr.NewTc[idx].Tc.Spec.TiDB.Replicas
					if tcArr.NewTc[idx].Replicas > tcArr.OldTc[idx].Replicas {
						hasHashrateAdd = true
					}
					continue
				}
			}
			if errArr[idx] != nil {
				tcArr.NewTc[idx].Replicas = tcArr.OldTc[idx].Tc.Spec.TiDB.Replicas
				tcArr.NewTc[idx].Tc.Spec.TiDB.Replicas = tcArr.OldTc[idx].Tc.Spec.TiDB.Replicas
			}
			hasHashrateAdd = true
		}
		// autoscaler to midware
		if hasHashrateAdd == true {
			err := AutoScalerToMidWareScalerOut(tcArr)
			if err != nil {
				klog.Errorf("[%s/%s] AutoScalerToMidWare failed err:%v", tcArr.NewTc[0].Tc.Namespace, tcArr.NewTc[0].Tc.Name, err)
				return err
			}
		}
	}
	return nil
}

func HanderAllScalerIn(tcArr *TClus, sldb *v1alpha1.ServerlessDB, sldbType tcv1.MemberType, autoscale bool) error {
	if sldbType == tcv1.TiDBMemberType {
		//klog.Infof("[%s/%s] HanderAllScalerIn scale in %v,%v",sldb.Namespace,sldb.Name,tcArr.NewHashRate,tcArr.OldHashRate)
		var hasReplicas bool
		for i := range tcArr.NewTc {
			if tcArr.NewTc[i].Tc.Spec.TiDB.Replicas >= tcArr.OldTc[i].Tc.Spec.TiDB.Replicas && tcArr.NewTc[i].Tc.Spec.TiDB.Replicas > 0 {
				hasReplicas = true
				break
			}
		}
		var minReps int32
		if FGreater(tcArr.NewHashRate, tcArr.OldHashRate) == true {
			//need add replicas but no replicas add,need keep old status no change
			if hasReplicas == false {
				//keep old hashrate no change
				tcArr.NewTc[0].Tc.Spec.TiDB.Replicas = tcArr.OldTc[0].Tc.Spec.TiDB.Replicas
				tcArr.NewTc[0].Replicas = tcArr.NewTc[0].Tc.Spec.TiDB.Replicas
				tcArr.NewTc[1].Tc.Spec.TiDB.Replicas = tcArr.OldTc[1].Tc.Spec.TiDB.Replicas
				tcArr.NewTc[1].Replicas = tcArr.NewTc[1].Tc.Spec.TiDB.Replicas
				return nil
			}
		} else {
			//at least one replicas
			if FEqual(tcArr.NewHashRate, 0.0) != true {
				if hasReplicas == false {
					minReps = 1
				}
			}
		}

		//calculate reduce num
		wg := sync.WaitGroup{}
		var errArr = make([]error, len(tcArr.NewTc), len(tcArr.NewTc))
		var oldAnno = make([]string, len(tcArr.NewTc))
		var indexArr []int
		for i := range tcArr.NewTc {
			if tcArr.NewTc[i].Tc.Spec.TiDB.Replicas < tcArr.OldTc[i].Tc.Spec.TiDB.Replicas {
				if tcArr.NewTc[i].Tc.Spec.TiDB.Replicas <= minReps {
					tcArr.NewTc[i].Tc.Spec.TiDB.Replicas = minReps
					tcArr.NewTc[i].Replicas = minReps
				}
				if minReps == tcArr.OldTc[i].Tc.Spec.TiDB.Replicas {
					continue
				}
				wg.Add(1)
				indexArr = append(indexArr, i)
				go func(idx int) {
					defer wg.Done()
					if tcArr.NewTc[idx].Tc.Annotations == nil {
						tcArr.NewTc[idx].Tc.Annotations = map[string]string{}
					}
					tcArr.NewTc[idx].Tc.Annotations[string(sldbType)+"-resource-problem"] = "false"
					oldAnno[idx] = tcArr.NewTc[idx].Tc.Annotations[label.AnnTiDBDeleteSlots]
					err := GetNewAnnoAndReplias(sldb, &tcArr.NewTc[idx].Tc, &tcArr.OldTc[idx].Tc, sldbType)
					if err != nil {
						errArr[idx] = err
						return
					}
					if tcArr.NewTc[idx].Tc.Spec.TiDB.Replicas == 0 {
						delete(tcArr.NewTc[idx].Tc.Annotations, label.AnnTiDBDeleteSlots)
					}
					err = UpdateTC(&tcArr.NewTc[idx].Tc, sldbType, autoscale)
					if err != nil {
						errArr[idx] = err
						return
					}
				}(i)
			}
		}
		wg.Wait()
		for _, idx := range indexArr {
			if errArr[idx] != nil {
				tcArr.NewTc[idx].Tc.Spec.TiDB.Replicas = tcArr.OldTc[idx].Tc.Spec.TiDB.Replicas
				tcArr.NewTc[idx].Replicas = tcArr.OldTc[idx].Replicas
			}
		}
	}
	return nil
}



func GetNewAnnoAndReplias(sldb *v1alpha1.ServerlessDB, tc *tcv1.TidbCluster, oldTc *tcv1.TidbCluster,
	sldbType tcv1.MemberType) error {
	var newReplicas int32
	var anno map[string]string
	var err error
	if sldbType == tcv1.TiDBMemberType {
		//version has happen change
		newReplicas = tc.Spec.TiDB.Replicas
		anno, err = UpdateAnno(tc, label.TiDBLabelVal, int(oldTc.Spec.TiDB.Replicas), int(newReplicas))
		if err != nil {
			klog.Errorf("[%s/%s] tidb update annotation for cluster failed: %s.", sldb.Namespace, sldb.Name, err)
		}
		tc.Annotations = anno
	} else if sldbType == tcv1.TiKVMemberType {
		newReplicas = tc.Spec.TiKV.Replicas
		anno, err = UpdateAnno(tc, label.TiKVLabelVal, int(oldTc.Spec.TiKV.Replicas), int(newReplicas))
		if err != nil {
			klog.Errorf("[%s/%s] tikv update annotation for cluster failed: %s.", sldb.Namespace, sldb.Name, err)
		}
		tc.Annotations = anno
	} else {
		return fmt.Errorf("[%s/%s] sldbType is error %v", sldb.Namespace, sldb.Name, sldbType)
	}
	return nil
}

func getMidWareTidb(name, namesp, scalertype string) ([]DBStatus, error) {
	webclient := webClient.NewAutoScalerClientApi()
	//url := "http://" + name + "-" + "he3proxy" + "." + namesp + ".svc:9797/api/v1/clusters/status"
	url := "http://" + name + "-proxy-tidb" + "." + namesp + ".svc:10080/api/v1/clusters/status/" + scalertype
	podInfo, err := webclient.GetAllTidb(url)
	if err != nil {
		klog.Errorf("[%s/%s] SyncReplicasToMidWare GetAllTidb failed %v", namesp, name, err)
		return nil, err
	}
	podDBArr := make([]DBStatus, 0)
	if podInfo != "" {
		err := json.Unmarshal([]byte(podInfo), &podDBArr)
		if err != nil {
			klog.Errorf("[%s/%s] SyncReplicasToMidWare Unmarshal failed %v", namesp, name, err)
			return nil, err
		}
	}
	return podDBArr, nil
}

func getMidwareAndScalerSyncStatus(podList []*corev1.Pod, name, namesp string,scalertype string) ([]string, bool, error) {
	var needAddTidb []string
	var needRegister bool
	podDBArr, err := getMidWareTidb(name, namesp, scalertype)
	if err != nil {
		return nil, false, err
	}
	var clusAddr string
	for _, clus := range podDBArr {
		if clus.Cluster != name {
			continue
		}
		if clus.Address != "" {
			clusAddr = clus.Address
			break
		}
	}
	for _, pod := range podList {
		var podExistInMidWare = true
		if strings.Contains(clusAddr, pod.Name) != true && IsPodReady(pod) == true {
			podExistInMidWare = false
		}
		if podExistInMidWare == false {
			err := DnsCheck(pod)
			if err != nil {
				klog.Errorf("[%s/%s] SyncReplicasToMidWare DnsCheck failed %v", pod.Namespace, pod.Name, err)
			} else {
				if scalertype == TP && strings.Contains(pod.Name,name+"-t") == true ||
					strings.Contains(pod.Name,name+"-"+scalertype) == true {
					needAddTidb = append(needAddTidb, pod.Name)
					needRegister = true
				}
				klog.Infof("[%s/%s] SyncReplicasToMidWare DnsCheck success", pod.Namespace, pod.Name)
			}
		}
	}
	return needAddTidb, needRegister, nil
}

func postAddTidb(podList []*corev1.Pod, name, namesp string,scalertype string) error {
	webclient := webClient.NewAutoScalerClientApi()
	var addCount int
	needAddTidb, needRegister, err := getMidwareAndScalerSyncStatus(podList, name, namesp,scalertype)
	if err != nil {
		klog.Infof("[%s/%s] postAddTidb getMidwareAndScalerSyncStatus failed: %v", namesp, name, err)
		return err
	}
	if needRegister == true {
		//postUrl := "http://" + name + "-he3proxy" + "." + namesp + ".svc:9797/api/v1/clusters/sldb/Tidbs"
		postUrl := "http://" + name + "-proxy-tidb" + "." + namesp + ".svc:10080/api/v1/clusters/sldb/Tidbs"
		var err error
		for {
			err = webclient.PostAddTidb(postUrl, name, namesp, scalertype)
			if err != nil {
				klog.Errorf("[%s/%s] SyncReplicasToMidWare PostAddTidb failed %v", namesp, name, err)
			}
			podDBArr, err := getMidWareTidb(name, namesp, scalertype)
			if err != nil {
				klog.Errorf("[%s/%s] SyncReplicasToMidWare getMidWareTidb failed %v", namesp, name, err)
			}
			addCount = 0
			for _, clus := range podDBArr {
				if clus.Cluster != name {
					continue
				}
				for _, tidb := range needAddTidb {
					if strings.Contains(clus.Address, tidb) == true {
						addCount++
					}
				}
			}
			if addCount == len(needAddTidb) {
				break
			}
			//重新get podList
			podList, err = GetK8sAllPodArray(name, namesp, tcv1.TiDBMemberType,scalertype)
			if err != nil {
				klog.Errorf("[%s/%s] SyncReplicasToMidWare GetAllPodArray failed %v", namesp, name, err)
				return err
			}
			needAddTidb, needRegister, err = getMidwareAndScalerSyncStatus(podList, name, namesp,scalertype)
			if err != nil {
				klog.Infof("[%s/%s] postAddTidb   getMidwareAndScalerSyncStatus failed %v", namesp, name, err)
				return err
			}
			//all register completed
			if needRegister == false {
				break
			}
		}
	}
	return nil
}

func SyncReplicasToMidWare(tcArr *TClus,scalertype string) error {
	if len(tcArr.NewTc) == 0 {
		return fmt.Errorf("has no tc elem")
	}
	name := tcArr.NewTc[0].Tc.Name
	nameArr := strings.Split(name,"-")
	name = nameArr[0]
	namesp := tcArr.NewTc[0].Tc.Namespace
	podList, err := GetK8sAllPodArray(name, namesp, tcv1.TiDBMemberType,scalertype)
	if err != nil {
		klog.Errorf("[%s/%s] SyncReplicasToMidWare GetAllPodArray failed %v", namesp, name, err)
		return err
	}
	podDBArr, err := getMidWareTidb(name, namesp, scalertype)
	if err != nil {
		return err
	}
	//complete need reduce pod
	var reducePod []string
	for _, clus := range podDBArr {
		if clus.Cluster != tcArr.NewTc[0].Tc.Name {
			continue
		}
		if clus.Address != "" {
			clusAddrArr := strings.Split(clus.Address, ",")
			for _, addr := range clusAddrArr {
				if scalertype == TP && strings.Contains(addr,name+"-t") == true ||
					strings.Contains(addr,name+"-"+scalertype) == true {
					var podExist = false
					for _, pod := range podList {
						if strings.Contains(addr, pod.Name) == true {
							podExist = true
							break
						}
					}
					if podExist == false {
						podAddr := strings.Split(addr, ".")
						reducePod = append(reducePod, podAddr[0])
					}
				}
			}
		}
	}
	// add  register node
	if err := postAddTidb(podList, name, namesp,scalertype); err != nil {
		return err
	}
	// delete no register
	webclient := webClient.NewAutoScalerClientApi()
	for _, addr := range reducePod {
		//postUrl := "http://" + name + "-he3proxy" + "." + namesp + ".svc:9797/api/v1/clusters/tidbs"
		postUrl := "http://" + name + "-proxy-tidb" + "." + namesp + ".svc:10080/api/v1/clusters/tidbs"
		if err := webclient.DeleteErrorTidb(postUrl, name, namesp, addr); err != nil {
			klog.Errorf("[%s/%s] SyncReplicasToMidWare DeleteErrorTidb failed %v", namesp, name, err)
			return err
		}
	}
	return nil
}

func CallupTidb(podList []*corev1.Pod, name, namesp string,scaletype string) error {
	err := postAddTidb(podList, name, namesp,scaletype)
	return err
}

func AutoScalerToMidWareScalerOut(tcArr *TClus) error {
	if len(tcArr.NewTc) == 0 {
		return fmt.Errorf("has no tc elem")
	}
	var scalertype string
	if len(tcArr.NewTc) == 1 {
		scalertype = AP
	} else {
		scalertype = TP
	}
	name := tcArr.NewTc[0].Tc.Name
	nameArr := strings.Split(name,"-")
	name = nameArr[0]
	namesp := tcArr.NewTc[0].Tc.Namespace
	var hasPodAdd bool
	var podList []*corev1.Pod
	var err error
	for i := range tcArr.NewTc {
		if tcArr.NewTc[i].Replicas > tcArr.OldTc[i].Replicas {
			podList, err = GetK8sPodArray(&tcArr.NewTc[i].Tc, tcv1.TiDBMemberType,scalertype)
			if err != nil {
				return err
			}
			hasPodAdd = true
			break
		}
	}
	if hasPodAdd == true {
		err = CallupTidb(podList, name, namesp,scalertype)
		if err != nil {
			return err
		}
	}
	return nil
}

func FGreater(a, b float64) bool {
	return math.Max(a, b) == a && math.Abs(a-b) > 0.001
}

func FSmaller(a, b float64) bool {
	return math.Max(a, b) == b && math.Abs(a-b) > 0.001
}

func FEqual(a, b float64) bool {
	return math.Abs(a-b) < 0.001
}

func ClusterStatusCheck(sldb *v1alpha1.ServerlessDB, sldbType tcv1.MemberType) (*TClus, error) {
	if sldb.DeletionTimestamp != nil {
		klog.Infof("[%s/%s] autoScalerOutHander has been deleted\n", sldb.Namespace, sldb.Name)
		return nil, nil
	}
	tclus,_,err := CloneMutiRevsionTc(sldb,TP)
	if err != nil {
		if errors.IsNotFound(err) {
			return nil, nil
		}
		klog.Infof("[%s/%s] CloneMutiRevsionTc problem err %v", sldb.Namespace, sldb.Name, err)
		return nil, err
	}
	tidbStatus := GetTidbStatus(tclus)
	silentCond := util.GetServerlessDBCondition(sldb.Status, v1alpha1.TiDBSilence)
	restartCond := util.GetServerlessDBCondition(sldb.Status, v1alpha1.TiDBRestart)
	if ((tidbStatus != tcv1.NormalPhase || len(sldb.Status.Rule) != 0) && sldbType == tcv1.TiDBMemberType) ||
		(tclus.OldTc[0].Tc.Status.TiKV.Phase != tcv1.NormalPhase && sldbType == tcv1.TiKVMemberType) ||
		sldb.Status.Phase != v1alpha1.PhaseAvailable || sldb.Spec.Paused == true || silentCond != nil || restartCond != nil {
		klog.Infof("[%s/%s] is not premit scaler Phase %s,len rule %v, sldbType %v,tidb phase %v,tikv phase %v\n",
			sldb.Namespace, sldb.Name, sldb.Status.Phase, sldb.Status.Rule, sldbType, tidbStatus, tclus.OldTc[0].Tc.Status.TiKV.Phase)
		return nil, nil
	}
	return tclus, nil
}

func UpdateSldbCondStatus(sldb *v1alpha1.ServerlessDB) error {
	conds := sldb.Status.Conditions
	klog.Infof("sldb %s/%s conditions is %v", sldb.Namespace, sldb.Name, conds)

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
			sldb.Status.Conditions = conds
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

func ListTidbcluster(ns string, opt metav1.ListOptions) (*tcv1.TidbClusterList, error) {
	return sldbcluster.SldbClient.PingCapCli.PingcapV1alpha1().TidbClusters(ns).List(opt)
}
