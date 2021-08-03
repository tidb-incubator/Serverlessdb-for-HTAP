package score

import (
	"fmt"
	"github.com/tidb-incubator/Serverlessdb-for-HTAP/pkg/he3local/scheduler/postfilter"
	"github.com/tidb-incubator/Serverlessdb-for-HTAP/pkg/he3local/scheduler/util"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/klog"
	framework "k8s.io/kubernetes/pkg/scheduler/framework/v1alpha1"
	"k8s.io/kubernetes/pkg/scheduler/nodeinfo"
	"strings"
)

/*

容量超分算法实现


availsocre*系数+（所有PVC(limit-request)）*系数+((vgpoolsize-tatalpvclimit)*系数
三个数字大概范围值 ,相差不大，所以系数不用相差太大，系数的主要目的，是为了突出一个维度的重要性

pvc limit 10G,reqest=5G
NODE 5是空的，计算出来的分数，基于PVC
availvgpvc：50G
（所有PVC(limit-request)/PVC数目） 5G  （limit 10,reque5G）
((vgpoolsize-tatalpvclimit)/所有的THINVG) 40G


node上面之前已经有一个PVC,limit 3G request 2g

availvgpvc：49G
（所有PVC(limit-request)/PVC数目） (5G+1G)/2= 3G
((vgpoolsize-tatalpvclimit)/所有的THINVG)  50G-(10g+3)





为什么要除以PVC数目以及所有thinvg的数目，如果不这样，这两个分数太大，会影响availsocre分数，对我来说，availsocre占比应该最大为5,后面1，2

一共三个系数，也就是三个维度，找一个维度是1,其它维度根据相对值，设置具体的值，例如空间维度是1.     其它维度可以相对设为2,3

验证逻辑：
  1.availsocre：两个node,谁可用空间越大，就调度到那个NODE（计算可用空间，包含超分VG，和正常VG）
  2.所有PVC(limit-request)/PVC数目：两个NODE，谁可以超分越大，越建议调度到那个结点，超分范围由limit-requst决定，如果两个NODE,每个上面一个PVC, 一个（limit-requst）=5.一个(limit-reques)=2,说明5那个NODE关联的thin vg超分比率更大
            limit-requst计算公式：所有的PVC的limit-requst相加/pvc的数目

	    一个node上面没有PVC,一个NODE上面有一个PVC，limit-quest=5. 基于这个公式会优先调试到5那个NODE上面,因为如果没有PVC的NODE,对我来说，超分得到的分数就是0，逻辑上是合理的，


  3.(vgpoolsize-tatalpvclimit)，两个NODE，一个NODE上面已经有一个PVC，alllimit=10 别一个NODE上面没有PVC，没有PVC的NODE得分更高等于vgpoolsize, 有PVC分数得分更底，vgpoolsize-10(如果分配过多，可能是负值)
     设计的目的，就是如果这个VG总容量被超分，超分的越多，减去的分数越大，
*/
func CalculateValueScore(value string, state *framework.CycleState, node *nodeinfo.NodeInfo) (int64, error) {
	d, err := state.Read(framework.StateKey("Max" + value))
	if err != nil {
		klog.V(3).Infof("Error Get CycleState Info: %v", err)
		return 0, err
	}
	size, err := util.GetVGSize(node.Node().Labels[value])
	if err != nil {
		return 0, err
	}
	klog.V(3).Infof("size is %d,max size %d,node is %s ", size, d.(*postfilter.StorageData).Size, node.Node().Name)
	return size * 100 / d.(*postfilter.StorageData).Size, nil
}

func CalculateCollectScore(state *framework.CycleState, node *nodeinfo.NodeInfo, vgs []string, f *framework.FrameworkHandle, p *v1.Pod) (int64, error) {
	var score int64 = 0
	for _, v := range vgs {
		s, err := CalculateValueScore(v, state, node)
		if err != nil {
			return 0, err
		}
		score += s
	}

	fmt.Printf("node is %s,score is %d \n", node.Node().Name, score)

	foundthin := CheckContailThinInVgs(p, f)
	if !foundthin {
		fmt.Printf("not found thin vg for pod %s \n", p.Name)
		return score, nil
	}

	allpvs, err := GetAllPVCForSameThinVG(node, f, p)
	if err != nil {
		fmt.Println(err)
		return score, nil
	}
	fmt.Printf("node is %s,lens(allpvs) is %d \n", node.Node().Name, len(allpvs))
	for thinname, pvs := range allpvs {
		fmt.Printf("node is %s,thins is %s \n", node.Node().Name, thinname)

		for _, pvc := range pvs {
			fmt.Printf("node is %s,pvc name %s \n", node.Node().Name, pvc.Name)
		}
	}
	if len(allpvs) != 0 {
		//score = score*5 + GetLimitPlusRequest(allpvs)*1 + GetAllPVCLimitPlusVGThin(allpvs, node)*1
		score = score*5  + GetAllPVCLimitPlusVGThin(allpvs, node)*1
	}
	return score, nil
}

func GetAllPVCForSameThinVG(node *nodeinfo.NodeInfo, f *framework.FrameworkHandle, p *v1.Pod) (map[string][]*v1.PersistentVolumeClaim, error) {
	pvcLis := (*f).SharedInformerFactory().Core().V1().PersistentVolumeClaims().Lister()
	vgsLis := (*f).SharedInformerFactory().Storage().V1().StorageClasses().Lister()
	pvsforthinvg := make(map[string][]*v1.PersistentVolumeClaim, 0)
	allpods := append(node.Pods(), p)
	for _, pod := range allpods {
		for _, vol := range pod.Spec.Volumes {
			if vol.PersistentVolumeClaim != nil {
				pvc, err := pvcLis.PersistentVolumeClaims(pod.Namespace).Get(vol.PersistentVolumeClaim.ClaimName)
				if err != nil {
					continue
				}
				if strings.Contains(*pvc.Spec.StorageClassName, "lvm-") {
					storage, err := vgsLis.Get(*pvc.Spec.StorageClassName)
					if err != nil {
						continue
					}
					para := storage.Parameters
					if para != nil && para["thinpool"] != "" && para["vgName"] != "" {
						//pvsforthinvg = append(pvsforthinvg, pvc)
						vgthinname := fmt.Sprintf("%s/%s", para["vgName"], para["thinpool"])
						if _, ok := pvsforthinvg[vgthinname]; ok {
							pvsforthinvg[vgthinname] = append(pvsforthinvg[vgthinname], pvc)
						} else {
							pvsforthinvg[vgthinname] = []*v1.PersistentVolumeClaim{pvc}
						}
					}

				}
			}
		}
	}
	return pvsforthinvg, nil
}

/*func GetLimitPlusRequest(allvgpvc map[string][]*v1.PersistentVolumeClaim) int64 {
	var res int64

	for thinname, pvcs := range allvgpvc {
		for _, pvc := range pvcs {
			if _, ok := pvc.Spec.Resources.Limits["storage"]; !ok {
				continue
			} else {
				limit := pvc.Spec.Resources.Limits["storage"]
				request := pvc.Spec.Resources.Requests["storage"]
				if limit.Value() > request.Value() {
					res = res + (limit.Value() - request.Value())
				}
				fmt.Printf("GetLimitPlusRequest thin is %s,limit is %d,requst is %d ,res is %d \n", thinname, limit.Value(), request.Value(), res)
			}
		}
	}

	fmt.Printf("GetLimitPlusRequest total is %d \n", res)

	return res
}
*/
func GetAllPVCLimitPlusVGThin(allvgpvc map[string][]*v1.PersistentVolumeClaim, node *nodeinfo.NodeInfo) int64 {

	var resstorage resource.Quantity
	var scoreres int64

	for thinname, pvcs := range allvgpvc {
		var res int64
		for _, pvc := range pvcs {
			/*if _, ok := pvc.Spec.Resources.Limits["storage"]; ok {
				//resstorage = pvc.Spec.Resources.Limits["storage"]
				//pvc only request been use to set pv.so chose request
				resstorage = pvc.Spec.Resources.Limits["storage"]
			} else {
				resstorage = pvc.Spec.Resources.Requests["storage"]
			}*/
			resstorage = pvc.Spec.Resources.Requests["storage"]
			res = res + resstorage.Value()
		}

		thinpoolname := fmt.Sprintf("%s-total", thinname)
		if _, ok := node.Node().Labels[thinpoolname]; !ok {
			continue
		}

		total, err := util.GetVGSize(node.Node().Labels[thinpoolname])
		if err != nil {
			continue
		}
		fmt.Printf("GetAllPVCLimitPlusVGThin ,node is %s,total is %d,res is %d,total-res=%d \n", node.Node().Name, total, res, (total - res))
		scoreres = scoreres + (total - res)
	}
	fmt.Printf("GetAllPVCLimitPlusVGThin node is %s,scores is %d \n", node.Node().Name, scoreres)
	return scoreres
}

func Score(state *framework.CycleState, node *nodeinfo.NodeInfo, vgs []string, f *framework.FrameworkHandle, p *v1.Pod) (int64, error) {
	s, err := CalculateCollectScore(state, node, vgs, f, p)
	if err != nil {
		return 0, err
	}
	return s, nil
}

func CheckContailThinInVgs(pod *v1.Pod, f *framework.FrameworkHandle) bool {
	var founthin bool

	pvcLis := (*f).SharedInformerFactory().Core().V1().PersistentVolumeClaims().Lister()

	vgsLis := (*f).SharedInformerFactory().Storage().V1().StorageClasses().Lister()

	for _, vol := range pod.Spec.Volumes {
		if vol.PersistentVolumeClaim != nil {
			pvc, err := pvcLis.PersistentVolumeClaims(pod.Namespace).Get(vol.PersistentVolumeClaim.ClaimName)
			if err != nil {
				return false
			}
			if strings.Contains(*pvc.Spec.StorageClassName, "lvm-") {
				storage, err := vgsLis.Get(*pvc.Spec.StorageClassName)
				if err != nil {
					return false
				}
				para := storage.Parameters
				if para != nil && para["thinpool"] != "" {
					founthin = true
					break
				}
			}
		}
	}
	return founthin

}

/*
1. 每个NODE，算实际点用空间，如果实际占用空间越小，分数越大
2. 每个NODE，所有使用本地盘的PVC，request和limit 越
3. 如果NODE分配的空间超过，limit 的值，这个时候，分数要变低


实际占用空间    分数   5
(limit-request) 表示可以超分的可能性越大  3



real*5+ (limit-request)*3- (allpvclimit-vgsize)*5


get all pvc for thin storage
*/
