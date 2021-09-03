package pod

import (
	"crypto/tls"
	"github.com/kirinlabs/HttpRequest"
	admission "k8s.io/api/admission/v1beta1"
	"k8s.io/klog"
)

const sldbLabelKey = "bcrds.cmss.com/instance"
const podForceDelete = "ForceDelete"
const podPreDelete = "predelete"
const RoleInstanceLabelKey string = "bcrds.cmss.com/role"

type AdmiRequest struct {
	Cluster string `json:"cluster"`
	Addr    string `json:"addr"`
}

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

func (pc *PodAdmissionControl) admitDeleteTiDBPods(payload *admitPayload) *admission.AdmissionResponse {
	adminresp := &admission.AdmissionResponse{
		Allowed: true,
	}
	klog.Infof("[%s/%s]start to validate deleting tidb pod.\n", payload.pod.Namespace, payload.pod.Name)
	if payload.pod.Labels[podForceDelete] == "true" {
		klog.Infof("[%s/%s]pod has forcedelete label, admit to delete.\n", payload.pod.Namespace, payload.pod.Name)
		return adminresp
	}

	if payload.pod.Labels[RoleInstanceLabelKey] == "bigcost" || payload.pod.Labels[RoleInstanceLabelKey] == "proxy" {
		klog.Infof("[%s/%s]pod is belong bigcost tidb, admit to delete.\n", payload.pod.Namespace, payload.pod.Name)
		return adminresp
	}

	if payload.pod.Labels[podPreDelete] == "true" {
		klog.Errorf("[%s/%s]deleting pod in process, cannot admit to delete.\n", payload.pod.Namespace, payload.pod.Name)
		adminresp.Allowed = false
		return adminresp
	}

	//add predelete tidb label.
	pod := payload.pod
	pod.Labels[podPreDelete] = "true"
	newpod, err := pc.kubeCli.CoreV1().Pods(pod.Namespace).Update(pod)
	if err != nil {
		klog.Errorf("[%s/%s]pod update predelete label failed, cannot admit to delete.\n", payload.pod.Namespace, payload.pod.Name)
		adminresp.Allowed = false
		return adminresp
	}

	//url := "http://" + payload.pod.Labels[sldbLabelKey] + "-he3proxy" + "." + payload.pod.Namespace + ".svc:9797/api/v1/clusters/deltidb"
	url := "http://" + pod.Labels[sldbLabelKey] + "-proxy-tidb" + "." + payload.pod.Namespace + ".svc:10080/api/v1/clusters/deltidb"
	addr := payload.pod.Name + "." + payload.tc.Name + "-tidb-peer." + payload.tc.Namespace + ":4000@" + "1"
	body := map[string]interface{}{}
	body["cluster"] = payload.pod.Labels[sldbLabelKey]
	body["addr"] = addr
	body["tidbtype"] = payload.pod.Labels[RoleInstanceLabelKey]
	req := newRequest()
	resp, err := req.Post(url, body)
	if err == nil && resp.StatusCode() == 200 {
		klog.Infof("[%s/%s] delete tidb pod success.\n", payload.pod.Namespace, payload.pod.Name)
	} else {
		klog.Infof("[%s/%s] delete pod failed: %s, but admit to delete.\n", payload.pod.Namespace, payload.pod.Name, err)
	}

	newpod.Labels[podForceDelete] = "true"
	_, err = pc.kubeCli.CoreV1().Pods(newpod.Namespace).Update(newpod)
	if err != nil {
		klog.Errorf("[%s/%s]pod update forcedelete label failed, but admit to delete.\n", newpod.Namespace, newpod.Name)
	}

	//adminresp.Allowed = false
	return adminresp
}


func newRequest() *HttpRequest.Request {
	req := HttpRequest.NewRequest()
	req.SetTimeout(600)
	req.SetHeaders(map[string]string{"Content-Type": "application/json"})
	req.SetTLSClient(&tls.Config{InsecureSkipVerify: true})
	return req
}
