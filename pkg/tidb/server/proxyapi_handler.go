package server

import (
	"bytes"
	"fmt"
	"github.com/pingcap/tidb/proxy/backend"
	"github.com/pingcap/tidb/proxy/core/golog"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"os/exec"
	ctrl "sigs.k8s.io/controller-runtime"
	"strconv"
	"strings"
)

const (
	Offline = iota
	Online
	Unknown
	ComponentLabelKey    string = "app.kubernetes.io/component"
	RoleInstanceLabelKey string = "bcrds.cmss.com/role"
	AllInstanceLabelKey  string = "bcrds.cmss.com/instance"
	InstanceLabelKey     string = "app.kubernetes.io/instance"
	TidbPort             string = "4000"
)

var (
	KubeClient *kubernetes.Clientset
)

type NewTidb struct {
	Cluster  string `json:"cluster"`
	Addr     string `json:"addr"`
	TidbType string `json:"tidbtype"`
}

func (s *Server) GetAllClusters() *backend.Cluster {
	return s.cluster
}

func (s *Server) DeleteTidb(cluster, addr, tidbType string) error {
	addr = strings.Split(addr, backend.WeightSplit)[0]
	if err := s.cluster.DeleteTidb(addr, tidbType); err != nil {
		return err
	}

	return nil
}

func (s *Server) AddNewTidb(addr, tidbType string) error {

	if err := s.cluster.AddTidb(addr, tidbType); err != nil {
		return err
	}
	return nil
}

func GetProxyPod(clustername, namespace string) (*v1.PodList, error) {
	var listOptions metav1.ListOptions
	listOptions = metav1.ListOptions{
		LabelSelector: fmt.Sprintf("%s=%s,%s=%s,%s=%s", ComponentLabelKey, "tidb", RoleInstanceLabelKey, "proxy", AllInstanceLabelKey, clustername),
	}
	podList, err := KubeClient.CoreV1().Pods(namespace).List(listOptions)
	if err != nil {
		golog.Error("server", "GetPod", "get pod fail", 0, "error", err)
		return nil, err
	}
	return podList, nil
}

func GetPod(clustername, namespace, tidbType string) (*v1.PodList, error) {
	var listOptions metav1.ListOptions
	listOptions = metav1.ListOptions{
		LabelSelector: fmt.Sprintf("%s=%s,%s=%s,%s=%s", ComponentLabelKey, "tidb", RoleInstanceLabelKey, tidbType, AllInstanceLabelKey, clustername),
	}

	podList, err := KubeClient.CoreV1().Pods(namespace).List(listOptions)
	if err != nil {
		golog.Error("server", "GetPod", "get pod fail", 0, "error", err)
		return nil, err
	}
	return podList, nil
}

func IsPodReady(pod *v1.Pod) bool {
	condition := getPodReadyCondition(&pod.Status)
	return condition != nil && condition.Status == v1.ConditionTrue
}

func getPodReadyCondition(status *v1.PodStatus) *v1.PodCondition {
	for i := range status.Conditions {
		if status.Conditions[i].Type == v1.PodReady {
			return &status.Conditions[i]
		}
	}
	return nil
}

func (s *Server) dnsCheckOne(pod *v1.Pod) error {
	tcName := pod.Labels[InstanceLabelKey]
	name := pod.Name + "." + tcName + "-tidb-peer" + "." + pod.Namespace
	dnscheck := fmt.Sprintf(`nslookup %s && mysql -h%s -u%s  -p%s -P4000 --connect-timeout=2 -e "select 1;"`, name, name, s.cluster.Cfg.User, s.cluster.Cfg.Password)
	cmd := exec.Command("/bin/sh", "-c", dnscheck)
	var out, outerr bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = &outerr
	err := cmd.Run()
	if err != nil {
		golog.Debug("Server", "dnsCheckOne", "checking dnsCheckOne failed", 0, "name", name, "dns", out.String()+outerr.String(), "err", err)
		return err
	} else {
		golog.Debug("Server", "dnsCheckOne", "checking dnsCheckOne ", 0, "name", name, "dns", out.String())
		return nil
	}
}

func getFloatCpu(cpu string) string {
	var cpustr string
	cpuarr := strings.Split(cpu, "m")
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
	} else {
		cpustr = cpu
	}
	return cpustr
}

func (s *Server) NewOne(podList *v1.PodList, tidbType string) []*NewTidb {
	allNew := make([]*NewTidb, 0)
	for _, pod := range podList.Items {
		if pod.DeletionTimestamp != nil {
			continue
		}
		if IsPodReady(&pod) && s.dnsCheckOne(&pod) == nil {
			flag := false
			for _, mem := range s.cluster.BackendPools[tidbType].Tidbs {
				if strings.Contains(mem.Addr(), pod.Name) {
					flag = true
					break
				}
			}
			if flag == false {
				one := &NewTidb{}
				tcName := pod.Labels[InstanceLabelKey]
				cpuNum := ""
				for _, v1 := range pod.Spec.Containers {
					if v1.Name == "tidb" {
						cpuNum = v1.Resources.Limits.Cpu().String()
					}
				}
				cpuNum = getFloatCpu(cpuNum)
				one.Addr = pod.Name + "." + tcName + "-tidb-peer" + "." + pod.Namespace + ":" + TidbPort + "@" + cpuNum
				one.Cluster = s.cluster.Cfg.ClusterName
				one.TidbType = tidbType
				allNew = append(allNew, one)
				golog.Info("server", "NewOne", "add new tidb", 0,
					"NewOne", one.Cluster, "newone addr", one.Addr)
			}
		} else {
			golog.Info("server", "NewOne", "add new tidb", 0,
				"NewOne", pod.Name, "the pod is not ready, do not add any tidb")
		}
	}
	return allNew
}

func (s *Server) FindNewTidb(clusterName, ns, tidbType string) error {
	Podlist, err := GetPod(clusterName, ns, tidbType)
	if err != nil {
		golog.Error("server", "FindNewTidb", "get pod fail", 0, "error", err)
		return err
	}
	allNewTidb := s.NewOne(Podlist, tidbType)
	if len(allNewTidb) == 0 {
		err = fmt.Errorf("find zero new tidb.")
		golog.Error("server", "AddTidb", "AddTidb fail", 0, "error", err)
		return err
	}
	for _, new := range allNewTidb {
		err = s.AddNewTidb(new.Addr, new.TidbType)
		if err != nil {
			golog.Error("server", "AddTidb", "AddTidb fail", 0, "error", err)
			return err
		}
	}
	return nil
}

func init() {

	// Create the kubernetes clientset
	k8sConfig := ctrl.GetConfigOrDie()
	//k8sConfig, err := clientcmd.BuildConfigFromFlags(viper.GetString("https://10.154.0.150:6443"), viper.GetString("./configs"))
	//if err != nil {
	//	klog.Errorf("Failed to get kubeConfig! Error is %v", err)
	//}

	KubeClient, _ = kubernetes.NewForConfig(k8sConfig)
}
