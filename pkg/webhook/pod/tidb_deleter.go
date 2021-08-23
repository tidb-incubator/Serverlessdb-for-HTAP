package pod

import (
	"crypto/tls"
	"encoding/json"
	"github.com/kirinlabs/HttpRequest"
	"github.com/pingcap/tidb-operator/pkg/label"
	"github.com/tidb-incubator/Serverlessdb-for-HTAP/pkg/webhook/util"
	admission "k8s.io/api/admission/v1beta1"
	apps "k8s.io/api/apps/v1"
	core "k8s.io/api/core/v1"
	"k8s.io/klog"
	"strings"
)

const (
	sldbLabelKey = "bcrds.cmss.com/instance"
	podForceDelete = "ForceDelete"
	podPreDelete = "predelete"
	RoleInstanceLabelKey string = "bcrds.cmss.com/role"
)


type DBStatus struct {
	Cluster          string `json:"cluster"`
	Address       string `json:"address"`
	Type          string `json:"type"`
	Status        string `json:"status"`
	LastPing      string `json:"laste_ping"`
	MaxConn       int    `json:"max_conn"`
	IdleConn      int    `json:"idle_conn"`
	CacheConn     int    `json:"cache_conn"`
	PushConnCount int64  `json:"push_conn_count"`
	PopConnCount  int64  `json:"pop_conn_count"`
	UsingConnsCount int64 `json:"using_conn_count"`


}

func (pc *PodAdmissionControl) admitDeleteTiDBPods(pod *core.Pod, ownerStatefulSet *apps.StatefulSet) *admission.AdmissionResponse {
	name := pod.Name
	namespace := pod.Namespace

	tcName, exist := pod.Labels[label.InstanceLabelKey]
	if !exist {
		klog.Errorf("pod[%s/%s] has no label: %s", namespace, name, label.InstanceLabelKey)
		return util.ARSuccess()
	}
	adminresp := &admission.AdmissionResponse{
		Allowed: true,
	}
	klog.Infof("[%s/%s]start to validate deleting tidb pod.\n", namespace, name)
	if pod.Labels[podForceDelete] == "true" {
		klog.Infof("[%s/%s]pod has forcedelete label, admit to delete.\n", namespace, name)
		return adminresp
	}

	if pod.Labels[RoleInstanceLabelKey] == "bigcost" {
		klog.Infof("[%s/%s]pod is belong bigcost tidb, admit to delete.\n", namespace, name)
		return adminresp
	}

	if pod.Labels[podPreDelete] == "true" {
		klog.Errorf("[%s/%s]deleting pod in process, cannot admit to delete.\n", namespace, name)
		adminresp.Allowed = false
		return adminresp
	}

	//add predelete tidb label.
	pod.Labels[podPreDelete] = "true"
	newpod, err := pc.kubeCli.CoreV1().Pods(namespace).Update(pod)
	if err != nil {
		klog.Errorf("[%s/%s]pod update predelete label failed, cannot admit to delete.\n", namespace, name)
		adminresp.Allowed = false
		return adminresp
	}

	//url := "http://" + pod.Labels[sldbLabelKey] + "-he3proxy" + "." + namespace + ".svc:9797/api/v1/clusters/deltidb"
	url := "http://" + pod.Labels[sldbLabelKey] + "-proxy" + "." + namespace + ".svc:10080/api/v1/clusters/deltidb"
	addr := name + "." + tcName + "-tidb-peer." + namespace + ":4000@" + "1"
	body := map[string]interface{}{}
	body["cluster"] = pod.Labels[sldbLabelKey]
	body["addr"] = addr
	req := newRequest()
	resp, err := req.Post(url, body)
	if err == nil && resp.StatusCode() == 200 {
		newpod.Labels[podForceDelete] = "true"
		_, err := pc.kubeCli.CoreV1().Pods(namespace).Update(newpod)
		if err != nil {
			klog.Errorf("[%s/%s]pod update forcedelete label failed, but admit to delete.\n", namespace, name)
			return adminresp
		}
		klog.Infof("[%s/%s] delete tidb pod success.\n", namespace, name)
		return adminresp
	}
	//geturl := "http://" + pod.Labels[sldbLabelKey] + "-he3proxy" + "." + namespace + ".svc:9797/api/v1/clusters/status"
	geturl := "http://" + pod.Labels[sldbLabelKey] + "-proxy" + "." + namespace + ".svc:10080/api/v1/clusters/status"
	response, err := req.Get(geturl)
	if err != nil {
		klog.Errorf("[%s/%s] get tidb instance list failed: %s", namespace, name, err)
		newpod.Labels[podPreDelete] = "false"
		_, err := pc.kubeCli.CoreV1().Pods(namespace).Update(newpod)
		if err != nil {
			klog.Errorf("[%s/%s]pod update predelete label failed, but admit to delete.\n", namespace, name)
			return adminresp
		}
		adminresp.Allowed = false
		return adminresp
	}
	var instanceResp []DBStatus
	respbody, _ := response.Body()
	json.Unmarshal(respbody, &instanceResp)
	for _, instance := range instanceResp {
		if strings.Contains(instance.Address, name) {
			klog.Errorf("[%s/%s] get pod from proxy success, but delete failed, cannot admit to delete.", namespace, name)
			newpod.Labels[podPreDelete] = "false"
			_, err := pc.kubeCli.CoreV1().Pods(namespace).Update(newpod)
			if err != nil {
				klog.Errorf("[%s/%s]pod update predelete label failed, but admit to delete.\n", namespace, name)
				return adminresp
			}
			adminresp.Allowed = false
			return adminresp
		}
	}
	klog.Infof("[%s/%s] admit to delete, because it is not available in proxy.", namespace, name)
	return adminresp
}

func newRequest() *HttpRequest.Request {
	req := HttpRequest.NewRequest()
	req.SetTimeout(600)
	req.SetHeaders(map[string]string{"Content-Type": "application/json"})
	req.SetTLSClient(&tls.Config{InsecureSkipVerify: true})
	return req
}
