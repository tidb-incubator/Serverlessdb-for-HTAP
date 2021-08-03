package util

import (
	"fmt"
	//"github.com/container-storage-interface/spec/lib/go/csi"
	"context"
	clientset "github.com/kubernetes-csi/external-snapshotter/v2/pkg/client/clientset/versioned"
	log "github.com/sirupsen/logrus"
	"k8s.io/api/core/v1"
	//"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"
	"os"
	"strconv"
	"strings"
)

var (
	masterURL  string
	kubeconfig string
	kubeClient kubernetes.Interface
	snapClient clientset.Interface
)

func init() {
	fmt.Println("init client")

	cfg, err := clientcmd.BuildConfigFromFlags(masterURL, kubeconfig)
	if err != nil {
		log.Fatalf("Error building kubeconfig: %s", err.Error())
		os.Exit(1)
	}

	kubeClient, err = kubernetes.NewForConfig(cfg)
	if err != nil {
		log.Fatalf("Error building kubernetes clientset: %s", err.Error())
		os.Exit(1)
	}

	snapClient, err = clientset.NewForConfig(cfg)
	if err != nil {
		log.Fatalf("Error building snapshot clientset: %s", err.Error())
		os.Exit(1)
	}
}

func GetPvSize(ctx context.Context, volumeID string) (int64, string) {
	//var resstorage resource.Quantity
	pv := GetPV(ctx, volumeID)
	if pv == nil {
		return 0, ""
	}

	/*	pvcnamespace := pv.Spec.ClaimRef.Namespace
		pvcname := pv.Spec.ClaimRef.Name

		pvc, err := kubeClient.CoreV1().PersistentVolumeClaims(pvcnamespace).Get(pvcname, metav1.GetOptions{})
		if err != nil {
			log.Errorf("lvcreate: fail to get pv, err: %v", err)
			return 0, ""
		}

		if  _, ok := pvc.Spec.Resources.Limits["storage"]; ok {
			resstorage = pvc.Spec.Resources.Limits["storage"]
		} else {
			resstorage = pvc.Spec.Resources.Requests["storage"]
		}
	*/
	fmt.Printf("pv name =%s \n", pv.Name)
	pvQuantity := pv.Spec.Capacity["storage"]
	pvSize := pvQuantity.Value()
	pvSizeGB := pvSize / (1024 * 1024 * 1024)
	fmt.Printf("pv size =%d \n", pvSize)
	if pvSizeGB == 0 {
		pvSizeMB := pvSize / (1024 * 1024)
		return pvSizeMB, "m"
	}
	return pvSizeGB, "g"
}

func UpdateNode(nodename string, lablename string, add bool, updatesize int64) error {

	nodeobj, err := kubeClient.CoreV1().Nodes().Get(nodename, metav1.GetOptions{})
	if err != nil {
		return err
	}

	var current int64
	var newsize int64
	if nodeobj.Labels == nil {
		nodeobj.Labels = make(map[string]string)
		current = 0
	} else {
		if nodeobj.Labels[lablename] == "" {
			current = 0
		} else {
			oldvalue := nodeobj.Labels[lablename]

			if strings.Contains(oldvalue, "Gi") {
				size, err := strconv.ParseInt(strings.Trim(oldvalue, "Gi"), 10, 64)
				if err != nil {
					return err
				}
				current = size * 1024 * 1024 * 1024

			} else if strings.Contains(oldvalue, "Mi") {

				size, err := strconv.ParseInt(strings.Trim(oldvalue, "Mi"), 10, 64)
				if err != nil {
					return err
				}

				current = size * 1024 * 1024
			}

		}

	}

	if add {
		//add size
		newsize = current + updatesize

	} else {
		//subtraction size
		if current < updatesize {
			return fmt.Errorf("node value %d is small update size %d", current, updatesize)
		}
		newsize = current - updatesize
	}

	var labvalue string
	if newsize >= 1024*1024*1024 {
		labvalue = fmt.Sprintf("%dGi", newsize/(1024*1024*1024))

	} else if newsize >= 1024*1024 {
		labvalue = fmt.Sprintf("%dMi", newsize/(1024*1024))
	} else {
		labvalue = fmt.Sprintf("%d", newsize)
	}

	nodeobj.Labels[lablename] = labvalue

	_, err = kubeClient.CoreV1().Nodes().Update(nodeobj)
	if err != nil {
		log.Errorf("UpdateNode: fail to update node lable, err: %v", err)
		return err
	}

	return nil
}

func GetPV(ctx context.Context, volumeID string) *v1.PersistentVolume {
	/*	pvs, err := kubeClient.CoreV1().PersistentVolumes().List(metav1.ListOptions{})
		if err != nil {
			return nil
		}
		fmt.Printf("lens(pvs)=%d\n", len(pvs.Items))
		found := false
		var rightpv v1.PersistentVolume
		for _, pv := range pvs.Items {
			if pv.Spec.CSI.VolumeHandle == volumeID {
				found = true
				rightpv = pv
			}
		}
		if !found {
			return nil
		}

		return &rightpv
	*/
	listOptions := metav1.ListOptions{LabelSelector: fmt.Sprintf("volumeID=%s", volumeID)}
	fmt.Printf("options=%s\n", listOptions.LabelSelector)
	pvs, err := kubeClient.CoreV1().PersistentVolumes().List(listOptions)
	if err != nil {
		log.Errorf("lvcreate: fail to get pv, err: %v", err)
		return nil
	}
	fmt.Printf("lens(pvs)=%d\n", len(pvs.Items))
	if len(pvs.Items) != 1 {
		return nil
	}
	pv := pvs.Items[0]
	/*	if err != nil {
		log.Errorf("lvcreate: fail to get pv, err: %v", err)
		return nil
	}*/
	return &pv
}

func GetVolContentsource(ctx context.Context, volumeID string) (snapid string, s3env string, err error) {
	pv := GetPV(ctx, volumeID)
	if pv == nil {
		return "", "", fmt.Errorf("can not get pv for get volcontentsource")
	}
	if pv.Spec.ClaimRef == nil {
		return "", "", fmt.Errorf("pv clainref is nil")
	}
	name := pv.Spec.ClaimRef.Name
	namespace := pv.Spec.ClaimRef.Namespace
	if name == "" || namespace == "" {
		return "", "", fmt.Errorf("name or namespace is nil for get pvc")
	}

	pvc, err := kubeClient.CoreV1().PersistentVolumeClaims(namespace).Get(name, metav1.GetOptions{})
	if err != nil {
		log.Errorf("lvcreate: fail to get pv, err: %v", err)
		return "", "", err
	}
	ds := pvc.Spec.DataSource
	if ds == nil {
		return "", "", nil
	}

	if ds.Kind != "VolumeSnapshot" {
		return "", "", fmt.Errorf("VolumeSnapshot is %s,not for snap ", ds.Kind)
	}

	//get snapcontent for datasource
	snap, err := snapClient.SnapshotV1beta1().VolumeSnapshots(namespace).Get(ds.Name, metav1.GetOptions{})
	if err != nil {
		log.Errorf("lvcreate: fail to get snap, err: %v", err)
		return "", "", err
	}

	if !(*snap.Status.ReadyToUse) {
		return "", "", fmt.Errorf("snap is not ready")
	}

	//**************

	//get snapid for datasource
	snapcontent, err := snapClient.SnapshotV1beta1().VolumeSnapshotContents().Get(*snap.Status.BoundVolumeSnapshotContentName, metav1.GetOptions{})
	if err != nil {
		log.Errorf("lvcreate: fail to get snapconten, err: %v", err)
		return "", "", err
	}

	if !(*snapcontent.Status.ReadyToUse) {
		return "", "", fmt.Errorf("snapconten is not ready")
	}

	snapclass, err := snapClient.SnapshotV1beta1().VolumeSnapshotClasses().Get(*snapcontent.Spec.VolumeSnapshotClassName, metav1.GetOptions{})
	if err != nil {
		log.Errorf("fail to get snap storage, err: %v", err)
		return "", "", err
	}

	parms := snapclass.Parameters
	if parms == nil || parms["secretname"] == "" || parms["secretnamespace"] == "" || parms["s3endpoint"] == "" {
		fmt.Println("not secret in snap class \n")
		return "", "", fmt.Errorf("not secret in snap class")
	}
	enss3, err := GetS3ENV(ctx, parms["secretname"], parms["secretnamespace"], parms["s3endpoint"])
	if err != nil {
		return "", "", fmt.Errorf("can not got enss3")
	}

	//***********

	return *snapcontent.Status.SnapshotHandle, enss3, nil
}

func GetNodeAddress(ctx context.Context, volumeID string) (string, error) {
	pv := GetPV(ctx, volumeID)
	if pv == nil {
		return "", fmt.Errorf("can not get pv for get nodeaddress")
	}
	if pv.Labels != nil && pv.Labels["NodeAddress"] != "" {
		return pv.Labels["NodeAddress"], nil
	} else {
		return "", fmt.Errorf("not node address in pv")
	}
}

//*********
func GetVgnameForVolume(ctx context.Context, volumeID string) (string, string, error) {
	pv := GetPV(ctx, volumeID)
	if pv == nil {
		return "", "", fmt.Errorf("can not get pv for get nodeaddress")
	}
	csi := pv.Spec.PersistentVolumeSource.CSI
	if csi == nil {
		return "", "", fmt.Errorf("csi is empty")
	}

	if csi.VolumeAttributes == nil || csi.VolumeAttributes["vgName"] == "" {
		return "", "", fmt.Errorf("not vgname in pv")
	} else {
		return csi.VolumeAttributes["vgName"], csi.VolumeAttributes["fsType"], nil
	}
}

//*************
func SetNodeAddressForPV(ctx context.Context, node string, volumeID string) error {

	pv := GetPV(ctx, volumeID)
	if pv == nil {
		return fmt.Errorf("cant got pv for setnodeaddress")
	}

	nodeob, err := kubeClient.CoreV1().Nodes().Get(node, metav1.GetOptions{})

	if err != nil {
		log.Errorf("lvcreate: fail to get node, err: %v", err)
		return err
	}

	nodeaddres := nodeob.Status.Addresses[0]

	if pv.Labels == nil {
		pv.Labels = make(map[string]string)

	}

	pv.Labels["NodeAddress"] = nodeaddres.Address
	pv.Labels["NodeName"] = nodeob.Name

	_, err = kubeClient.CoreV1().PersistentVolumes().Update(pv)
	if err != nil {
		log.Errorf("lvcreate: fail to update pv, err: %v", err)
		return err
	}
	return nil
}

//****************************
func GetS3ENV(ctx context.Context, name string, namespace string, s3endpoint string) (string, error) {
	sct, err := kubeClient.CoreV1().Secrets(namespace).Get(name, metav1.GetOptions{})
	if err != nil {
		log.Errorf(" get secret, err: %v", err)
		return "", err
	}

	if sct.Data == nil || string(sct.Data["access"]) == "" || string(sct.Data["secret"]) == "" {
		return "", fmt.Errorf("not access or secret in secret")
	}

	s3env := fmt.Sprintf("MC_HOST_minio=http://%s:%s@%s  ", string(sct.Data["access"]), string(sct.Data["secret"]), s3endpoint)
	return s3env, nil
}

//******************************

/*func UpdatePVForNodeAffinity(ctx context.Context, volumeID string, nodename string) error {

	pv := GetPV(ctx, volumeID)
	if pv == nil {
		return fmt.Errorf("cant got pv for setnodeaddress")
	}

	var terms []v1.NodeSelectorTerm

	var expressions []v1.NodeSelectorRequirement

	expressions = append(expressions, v1.NodeSelectorRequirement{
		Key:      "kubernetes.io/hostname",
		Operator: v1.NodeSelectorOpIn,
		Values:   []string{nodename},
	})

	terms = append(terms, v1.NodeSelectorTerm{
		MatchExpressions: expressions,
	})

	na := v1.VolumeNodeAffinity{
		Required: &v1.NodeSelector{
			NodeSelectorTerms: terms,
		},
	}

    _, err := kubeClient.CoreV1().PersistentVolumes().Delete(pv.Name, &metav1.DeleteOptions{})

    pv.ResourceVersion=""
	pv.Spec.NodeAffinity = &na

	_, err := kubeClient.CoreV1().PersistentVolumes().Create(pv)
	if err != nil {
		log.Errorf("update Node Affir: fail to update pv, err: %v", err)
		return err
	}
	return nil

}
*/
func GetDevicePathForVolumue(ctx context.Context, volumeID string) (string, string, string, string, error) {
	pv := GetPV(ctx, volumeID)
	if pv == nil {
		return "", "", "", "", nil
	}

	scname := pv.Spec.StorageClassName

	sc, err := kubeClient.StorageV1().StorageClasses().Get(scname, metav1.GetOptions{})

	if err != nil {
		log.Errorf("get storage classs failed: error: %v", err)
		return "", "", "", "", err
	}
	if sc.Parameters == nil || sc.Parameters["vgName"] == "" {
		return "", "", "", "", fmt.Errorf("not vg been found in sc")
	}

	tranvolumeid := strings.ReplaceAll(volumeID, "-", "--")

	devicepath := fmt.Sprintf("/dev/mapper/%s-%s", sc.Parameters["vgName"], tranvolumeid)

	return devicepath, sc.Parameters["vgName"], sc.Parameters["thinpool"], sc.Parameters["fsType"], nil
}

func GetSnap(ctx context.Context, snapid string) (string, error) {
	snaps, err := snapClient.SnapshotV1beta1().VolumeSnapshotContents().List(metav1.ListOptions{})
	if err != nil {
		return "", err
	}
	fmt.Printf("lens(snaps)=%d\n", len(snaps.Items))
	for _, item := range snaps.Items {
		if  item.Status!=nil && *item.Status.SnapshotHandle == snapid {
			return *item.Spec.VolumeSnapshotClassName, nil
		}
	}
	return "", fmt.Errorf("not found snap for snapid %s", snapid)
}

func GetS3ENSForSnapID(ctx context.Context, snapid string) (string, error) {
	snapsc, err := GetSnap(ctx, snapid)
	if err != nil {
		return "", err
	}

	snapclass, err := snapClient.SnapshotV1beta1().VolumeSnapshotClasses().Get(snapsc, metav1.GetOptions{})
	if err != nil {
		log.Errorf("fail to get snap storage, err: %v", err)
		return "", err
	}

	parms := snapclass.Parameters
	if parms == nil || parms["secretname"] == "" || parms["secretnamespace"] == "" || parms["s3endpoint"] == "" {
		fmt.Println("not secret in snap class \n")
		return "", fmt.Errorf("not secret in snap class")
	}
	enss3, err := GetS3ENV(ctx, parms["secretname"], parms["secretnamespace"], parms["s3endpoint"])
	if err != nil {
		return "", fmt.Errorf("can not got enss3")
	}
	return enss3, nil

}
