package filter

import (
	"fmt"
	"github.com/tidb-incubator/Serverlessdb-for-HTAP/pkg/he3local/scheduler/util"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/klog"
	framework "k8s.io/kubernetes/pkg/scheduler/framework/v1alpha1"
	"k8s.io/kubernetes/pkg/scheduler/nodeinfo"
	"strconv"
	"strings"
)

func GetAllVgs(pod *v1.Pod, f *framework.FrameworkHandle) ([]string, error) {
	vgs := make([]string, 0)
	pvcLis := (*f).SharedInformerFactory().Core().V1().PersistentVolumeClaims().Lister()

	vgsLis := (*f).SharedInformerFactory().Storage().V1().StorageClasses().Lister()

	for _, vol := range pod.Spec.Volumes {
		if vol.PersistentVolumeClaim != nil {
			pvc, err := pvcLis.PersistentVolumeClaims(pod.Namespace).Get(vol.PersistentVolumeClaim.ClaimName)
			if err != nil {
				return nil, err
			}
			if strings.Contains(*pvc.Spec.StorageClassName, "lvm-") {
				storage, err := vgsLis.Get(*pvc.Spec.StorageClassName)
				if err != nil {
					return nil, err
				}
				para := storage.Parameters
				if para == nil || para["vgName"] == "" {
					return nil, fmt.Errorf("not found vgname in storageclass %s ", *pvc.Spec.StorageClassName)
				}
				if para["thinpool"] != "" {
					thinpoolvg := fmt.Sprintf("%s/%s", para["vgName"], para["thinpool"])
					vgs = append(vgs, thinpoolvg)
				} else {
					vgs = append(vgs, para["vgName"])
				}
			}
		}
	}
	return vgs, nil
}

func PodIsRestart(pod *v1.Pod, f *framework.FrameworkHandle, node *nodeinfo.NodeInfo) (restart bool, fit bool) {

	pvcLis := (*f).SharedInformerFactory().Core().V1().PersistentVolumeClaims().Lister()
	pvL := (*f).SharedInformerFactory().Core().V1().PersistentVolumes().Lister()
	isstart := false
	pvfit := false
	for _, vol := range pod.Spec.Volumes {
		if vol.PersistentVolumeClaim != nil {
			pvc, err := pvcLis.PersistentVolumeClaims(pod.Namespace).Get(vol.PersistentVolumeClaim.ClaimName)
			if err != nil {
				continue
			}

			if strings.Contains(*pvc.Spec.StorageClassName, "lvm-") {
				pvol, err := pvL.Get(pvc.Spec.VolumeName)
				if err != nil {
					continue
				}

				labs := pvol.Labels
				if labs != nil && labs["NodeName"] != "" {
					isstart = true
					if labs["NodeName"] == node.Node().Name {
						pvfit = true
					} else {
						pvfit = false
					}
					break
				}

			}
		}
	}

	return isstart, pvfit

}

func CheckLVMVGSInNode(node *nodeinfo.NodeInfo, vgs []string) bool {
	for _, vg := range vgs {
		if _, ok := node.Node().Labels[vg]; !ok {
			return false
		}
	}
	return true
}

func NodeFitsVgs(node *nodeinfo.NodeInfo, pod *v1.Pod, f *framework.FrameworkHandle) (bool, error) {
	pvcLis := (*f).SharedInformerFactory().Core().V1().PersistentVolumeClaims().Lister()
	vgsLis := (*f).SharedInformerFactory().Storage().V1().StorageClasses().Lister()

	for _, vol := range pod.Spec.Volumes {
		if vol.PersistentVolumeClaim != nil {
			pvc, err := pvcLis.PersistentVolumeClaims(pod.Namespace).Get(vol.PersistentVolumeClaim.ClaimName)
			if err != nil {
				return false, err
			}
			if strings.Contains(*pvc.Spec.StorageClassName, "lvm-") {

				storage, err := vgsLis.Get(*pvc.Spec.StorageClassName)

				if err != nil {
					return false, err
				}
				para := storage.Parameters
				if para == nil || para["vgName"] == "" {
					return false, fmt.Errorf("not found vgname in storageclass %s ", *pvc.Spec.StorageClassName)
				}

				var storlimit resource.Quantity

				/*	if _, ok := pvc.Spec.Resources.Limits["storage"]; ok {
						//storlimit = pvc.Spec.Resources.Limits["storage"]
						//because pvc only support use request to set pv size.so all size is request size
						storlimit = pvc.Spec.Resources.Limits["storage"]
					} else {
						storlimit = pvc.Spec.Resources.Requests["storage"]
					}
				*/
				storlimit = pvc.Spec.Resources.Requests["storage"]
				pvcsize := storlimit.Value()
				var vs int64
				if para["thinpool"] != "" {
					thinpoolname := fmt.Sprintf("%s/%s", para["vgName"], para["thinpool"])
					vs, err = util.GetVGSize(node.Node().Labels[thinpoolname])
					if err != nil {
						return false, err
					}

					totalsizename := fmt.Sprintf("%s/%s-total", para["vgName"], para["thinpool"])
					ts, err := util.GetVGSize(node.Node().Labels[totalsizename])
					if err != nil {
						return false, err
					}

					minper, err := strconv.ParseInt(para["storageMinAvaiER"], 10, 64)
					if err != nil {
						return false, err
					}
					overper, err := strconv.ParseInt(para["overProvisioningPer"], 10, 64)
					if err != nil {
						return false, err
					}

					fmt.Printf("minper is %d,overper is %d,total size is %d,avail size is %d \n", minper, overper, ts, vs)
                    fmt.Printf("small live is %f \n",float64(ts)*(float64(minper)/100.0))
					if float64(vs) < float64(ts)*(float64(minper)/100.0) {
						return false, fmt.Errorf("avail size is too low,avail size is %d,total size is %d", vs, ts)
					}

					allpvc, err := util.GetAllPVCForSameThinVG(node, f, pod)
					if err != nil {
						return false, err
					}
					fmt.Printf("len of all pvc for vgname %s is %d for vgname \n", thinpoolname, len(allpvc[thinpoolname]))

					allpvcsize := util.GetAllPVCSizeInVGThin(allpvc[thinpoolname])

					fmt.Printf("size for vgname %s : is %d,over size is %d \n", thinpoolname, allpvcsize,ts * overper)
					if (allpvcsize) > (ts * overper) {
						return false, fmt.Errorf("thin pool over provider %s d", allpvcsize)
					}

				} else {
					vs, err = util.GetVGSize(node.Node().Labels[para["vgName"]])
					if err != nil {
						return false, err
					}

					klog.V(3).Infof("vs is  %d \n", vs)
					klog.V(3).Infof("pvsize is %d  \n", pvcsize)

					if vs >= pvcsize {
						continue
					} else {
						fmt.Printf("avavil is %d small for pvcsize \n ", vs, pvcsize)
						return false, nil
					}
				}
			}
		}
	}

	return true, nil
}

func GetVGSize(vg string) (int64, error) {

	if strings.Contains(vg, "Gi") {
		size, err := strconv.ParseInt(strings.Trim(vg, "Gi"), 10, 64)
		if err != nil {
			return 0, err
		}
		return size * 1024 * 1024 * 1024, nil

	} else if strings.Contains(vg, "Mi") {

		size, err := strconv.ParseInt(strings.Trim(vg, "Mi"), 10, 64)
		if err != nil {
			return 0, err
		}
		return size * 1024 * 1024, nil

	} else {
		return 0, fmt.Errorf("not unit in vg lable")
	}

}
