package util

import (
	"fmt"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	//"k8s.io/klog"
	framework "k8s.io/kubernetes/pkg/scheduler/framework/v1alpha1"
	"k8s.io/kubernetes/pkg/scheduler/nodeinfo"
	"strconv"
	"strings"
)

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

func GetAllPVCSizeInVGThin(allvgpvc []*v1.PersistentVolumeClaim) int64 {

	var resstorage resource.Quantity
	var scoreres int64

	var res int64
	for _, pvc := range allvgpvc {
		resstorage = pvc.Spec.Resources.Requests["storage"]
		res = res + resstorage.Value()
	}
	scoreres = scoreres + res

	fmt.Printf("GetAllPVCLimitPlusVGThin scores is %d \n", scoreres)
	return scoreres
}
