package serverlessdb

import (
	"bytes"
	"fmt"
	"github.com/tidb-incubator/Serverlessdb-for-HTAP/pkg/sldb-operator/apis/bcrds/v1alpha1"
	"github.com/tidb-incubator/Serverlessdb-for-HTAP/pkg/sldb-operator/controller"
	"github.com/tidb-incubator/Serverlessdb-for-HTAP/pkg/sldb-operator/util"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/selection"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/remotecommand"
	"k8s.io/klog"
	"modernc.org/mathutil"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"time"
)

const (
	instanceKey  = "app.kubernetes.io/instance"
	componentKey = "app.kubernetes.io/component"
	componentVal = "tikv"
	mountPath    = "/var/lib/tikv"
	shellCommand = "df -P " + mountPath + " | tail -n+2 | head -n1"

	// df output:
	// name     total    usage    available
	// lvm-xxx  5029504  2455084  2295892    52%  /var/lib/tikv
	dfOutPattern    = "^.+?\\s+(\\d+)\\s+(\\d+)\\s+(\\d+)\\s+"
	dfOutTotalIndex = 1
	dfOutUsageIndex = 2
	dfOutAvailIndex = 3
	dfOutSubMatches = 4

	tikvStoreReplicas uint64 = 3
)

var (
	command    = []string{"sh", "-c", shellCommand}
	dfOutRegex = regexp.MustCompile(dfOutPattern)
)

type Updater interface {
	Run()
}

type DiskUsageUpdater struct {
	deps *controller.Dependencies
}

func (u *DiskUsageUpdater) Run() {
	klog.Info("Storage Updating Begin.")
	defer klog.Info("Storage Updating End.")

	list, err := (u.deps.ServerlessDBLister).List(labels.Everything())
	if err != nil {
		klog.Errorf("List ServerlessDB Error: %v", err)
		return
	}

	for _, obj := range list {
		if err = u.getAndUpdate(obj); err != nil {
			klog.Warningf("Fetching for [%v.%v] failed: %v",
				obj.Name, obj.Namespace, err)
		}
	}
}

func (u *DiskUsageUpdater) getAndUpdate(obj *v1alpha1.ServerlessDB) error {

	if obj.Status.Phase != v1alpha1.PhaseAvailable {
		return nil
	}

	usage, err := u.getDiskUsage(obj)
	if err != nil {
		return err
	}

	// calculate Avail percentage.
	availPercentage := 100.0
	if capacity, err := resource.ParseQuantity(obj.Spec.MaxValue.Component.StorageSize); err != nil {
		// no error returned, just logging.
		klog.Infof("Get maxStorage failed for db [%s/%s].", obj.Namespace, obj.Name)
	} else {
		totalUsage := resource.MustParse(fmt.Sprintf("%vKi", usage.totalUsage))
		availPercentage -= float64(totalUsage.Value()) * 100 / float64(capacity.Value())
	}

	// handle Avail percentage.
	err = u.handleAvailPercentage(obj, availPercentage)
	if err != nil {
		// no error returned, just emit event and logging.
		u.deps.Controls.ServerlessControl.RecordServerlessDBEvent("SetReadonly", err.Error(), obj, err)
		klog.Infof("Set Readonly failed for db [%s/%s].", obj.Namespace, obj.Name)
	}

	// update object status.
	status := obj.Status.DeepCopy()
	if status.StorageUsage == nil {
		status.StorageUsage = make(map[v1alpha1.StorageUse]resource.Quantity)
	}

	if availPercentage > u.deps.CLIConfig.ReadonlyAvailPercentage {
		status.Conditions = util.FilterOutCondition(status.Conditions, v1alpha1.TiDBReadonly)
	} else {
		updateTime := metav1.Now()
		newCondition := v1alpha1.ServerlessDBCondition{
			Type:               v1alpha1.TiDBReadonly,
			Status:             v1.ConditionTrue,
			LastUpdateTime:     updateTime,
			LastTransitionTime: updateTime,
			Reason:             "ReadonlyOn",
			Message:            "Readonly is turned on due to lack of tikv storage.",
		}
		util.SetServerlessDBCondition(status, newCondition)
	}

	// !not so accurate
	// status.StorageUsage[v1alpha1.TiKVSingleStoreMax] = humanize.IBytes(usage.singleMax * 1024)

	status.StorageUsage[v1alpha1.TiKVSingleStoreMax] = resource.MustParse(fmt.Sprintf("%vKi", usage.singleMax))
	status.StorageUsage[v1alpha1.TiKVTotalStoreUsage] = resource.MustParse(fmt.Sprintf("%vKi", usage.totalUsage))

	_, err = u.deps.ServerlessControl.UpdateServerlessDB(obj, status, &obj.Status)
	return err
}

func (u *DiskUsageUpdater) handleAvailPercentage(obj *v1alpha1.ServerlessDB, availPercentage float64) error {

	// only accessible inside cluster & under cluster dns.
	roRequestUrl := fmt.Sprintf("http://%s-he3proxy.%s:%d/api/v1/proxy/readonly/status",
		obj.Name, obj.Namespace, u.deps.CLIConfig.ProxyComponentHttpPort)

	roRequestBody := "{\"opt\": \"off\"}"
	if availPercentage <= u.deps.CLIConfig.ReadonlyAvailPercentage {
		roRequestBody = "{\"opt\": \"on\"}"
	}

	// set instance as readonly.
	req, err := http.NewRequest("PUT", roRequestUrl, strings.NewReader(roRequestBody))
	if err != nil {
		return fmt.Errorf("create request failed: %v", err)
	}
	req.Header.Add("Content-Type", "application/json")

	client := http.Client{Timeout: time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("request failed: %v", err)
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("response status is not OK: %v", resp.StatusCode)
	}
	return nil
}

func (u *DiskUsageUpdater) getDiskUsage(obj *v1alpha1.ServerlessDB) (*storeUsage, error) {

	selector := labels.NewSelector().Add(
		*eq(instanceKey, obj.Name), *eq(componentKey, componentVal))

	pods, err := u.deps.PodLister.Pods(obj.Namespace).List(selector)
	if err != nil {
		return nil, err
	}

	usage := &storeUsage{}

	for _, pod := range pods {
		podUsage, err := u.getPodDiskUsage(pod)
		if err != nil {
			return nil, err
		}
		usage.singleMax = mathutil.MaxUint64(usage.singleMax, podUsage.usage)
		usage.totalUsage += podUsage.usage / tikvStoreReplicas
	}
	return usage, nil
}

func (u *DiskUsageUpdater) getPodDiskUsage(pod *v1.Pod) (*dfOutput, error) {

	option := &v1.PodExecOptions{Command: command, Stdout: true}

	url := u.deps.KubeClientset.CoreV1().RESTClient().Post().
		Resource("pods").Name(pod.Name).Namespace(pod.Namespace).
		SubResource("exec").VersionedParams(option, scheme.ParameterCodec).URL()

	exec, err := remotecommand.NewSPDYExecutor(u.deps.RestConfig, "POST", url)
	if err != nil {
		return nil, err
	}

	stdout := bytes.Buffer{}
	err = exec.Stream(remotecommand.StreamOptions{Stdout: &stdout})
	if err != nil {
		return nil, err
	}

	output := stdout.String()
	match := dfOutRegex.FindStringSubmatch(output)
	if len(match) < dfOutSubMatches {
		klog.Warningf("df output not matched: [%v]", output)
		return nil, fmt.Errorf("df output not matched")
	}

	total := toInt64(match[dfOutTotalIndex])
	usage := toInt64(match[dfOutUsageIndex])
	avail := toInt64(match[dfOutAvailIndex])
	return &dfOutput{total, usage, avail}, nil
}

func eq(key string, val string) *labels.Requirement {
	r, _ := labels.NewRequirement(key, selection.In, []string{val})
	return r
}

func toInt64(s string) uint64 {
	v, err := strconv.ParseUint(s, 10, 0)
	if err != nil {
		return 0
	}
	return v

}

type storeUsage struct {
	singleMax  uint64
	totalUsage uint64
}

type dfOutput struct {
	total uint64
	usage uint64
	avail uint64
}
