package scheduler

import (
	"context"
	"fmt"
	"github.com/tidb-incubator/Serverlessdb-for-HTAP/pkg/he3local/scheduler/filter"
	"github.com/tidb-incubator/Serverlessdb-for-HTAP/pkg/he3local/scheduler/postfilter"
	"github.com/tidb-incubator/Serverlessdb-for-HTAP/pkg/he3local/scheduler/score"
	//"github.com/BCRDB/LVM-Scheduler/pkg/lvm/sort"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	//corelisters "k8s.io/client-go/listers/core/v1"
	"k8s.io/klog"
	framework "k8s.io/kubernetes/pkg/scheduler/framework/v1alpha1"
	"k8s.io/kubernetes/pkg/scheduler/nodeinfo"
	//"strings"
)

const (
	Name = "lvm"
)

var (
	_ framework.FilterPlugin     = &LVM{}
	_ framework.PostFilterPlugin = &LVM{}
	_ framework.ScorePlugin      = &LVM{}
	_ framework.ScoreExtensions  = &LVM{}
)

type Args struct {
	KubeConfig string `json:"kubeconfig,omitempty"`
	Master     string `json:"master,omitempty"`
}

type LVM struct {
	args   *Args
	handle framework.FrameworkHandle
}

func (y *LVM) Name() string {
	return Name
}

func New(configuration *runtime.Unknown, f framework.FrameworkHandle) (framework.Plugin, error) {
	args := &Args{}
	if err := framework.DecodeInto(configuration, args); err != nil {
		return nil, err
	}
	klog.V(3).Infof("get plugin config args: %+v", args)
	return &LVM{
		args:   args,
		handle: f,
	}, nil
}

func (y *LVM) Filter(ctx context.Context, state *framework.CycleState, pod *v1.Pod, node *nodeinfo.NodeInfo) *framework.Status {
	klog.V(3).Infof("filter pod: %v, node: %v", pod.Name, node.Node().Name)

	isrestat, fit := filter.PodIsRestart(pod, &y.handle, node)
	if isrestat {
		klog.V(3).Infof("pod %s been restart and reuse local pvc ", pod.Name)
		if fit {
			klog.V(3).Infof("reuse local pvc node %s match ", node.Node().Name)
			return framework.NewStatus(framework.Success, "")
		} else {
			klog.V(3).Infof("reuse local pvc node %s not match ", node.Node().Name)
			return framework.NewStatus(framework.Unschedulable, "pod been restart but node is not match")
		}

	}

	vgs, err := filter.GetAllVgs(pod, &y.handle)
	if err != nil || len(vgs) == 0 {
		return framework.NewStatus(framework.Unschedulable, "pod has not all lvm group in pvc in filter")
	}
	klog.V(3).Infof("vgs %s \n", vgs)
	if filter.CheckLVMVGSInNode(node, vgs) {
		ok, err := filter.NodeFitsVgs(node, pod, &y.handle)

		if err != nil || !ok {
			klog.V(3).Infof("filter is not ok for filter node %s,err is %s \n", node.Node().Name, err)
			return framework.NewStatus(framework.Unschedulable, "Node has not fit pod")
		}
		klog.V(3).Infof("node %s is ok for filter \n", node.Node().Name)
		return framework.NewStatus(framework.Success, "")
	} else {
		klog.V(3).Infof("filter is not ok for Node %s has not all vgs in pod \n", node.Node().Name)
		return framework.NewStatus(framework.Unschedulable, "Node has not all vgs in pod")
	}

}

func (y *LVM) PostFilter(ctx context.Context, state *framework.CycleState, pod *v1.Pod, nodes []*v1.Node, filteredNodesStatuses framework.NodeToStatusMap) *framework.Status {
	klog.V(3).Infof("collect info for scheduling  pod: %v", pod.Name)

	klog.V(3).Infof("lens(nodes)=%d \n", len(nodes))
	for _, node := range nodes {
		klog.V(3).Infof("node for postfilter: %v \n", node.Name)
	}

	vgs, err := filter.GetAllVgs(pod, &y.handle)
	if err != nil || len(vgs) == 0 {
		return framework.NewStatus(framework.Unschedulable, "pod has not lvm storageclass in pvc in postfilter")
	}

	return postfilter.ParallelCollection(postfilter.Workers, state, nodes, vgs, filteredNodesStatuses)
}

func (y *LVM) Score(ctx context.Context, state *framework.CycleState, p *v1.Pod, nodeName string) (int64, *framework.Status) {
	nodeInfo, err := y.handle.SnapshotSharedLister().NodeInfos().Get(nodeName)
	if err != nil {
		return 0, framework.NewStatus(framework.Error, fmt.Sprintf("getting node %q from Snapshot: %v", nodeName, err))
	}

	vgs, err := filter.GetAllVgs(p, &y.handle)
	if err != nil || len(vgs) == 0 {
		return 0, framework.NewStatus(framework.Unschedulable, "pod has not lvm storageclass in pvc")
	}

	s, err := score.Score(state, nodeInfo, vgs, &y.handle, p)
	if err != nil {
		return 0, framework.NewStatus(framework.Error, fmt.Sprintf("Score Node Error: %v", err))
	}
	klog.V(3).Infof("node : %v lvm-score: %v", nodeName, s)
	return s, framework.NewStatus(framework.Success, "")
}

func (y *LVM) NormalizeScore(ctx context.Context, state *framework.CycleState, p *v1.Pod, scores framework.NodeScoreList) *framework.Status {
	var (
		highest int64 = 0
		lowest        = scores[0].Score
	)
	for _, nodeScore := range scores {
		fmt.Printf("before node is %s,score id %d \n ", nodeScore.Name, nodeScore.Score)
		if nodeScore.Score < lowest {
			lowest = nodeScore.Score
		}
	}
	fmt.Printf("loweat is %d \n", lowest)

	if lowest < 0 {
		for i := range scores {
			scores[i].Score -= lowest
			fmt.Printf("after node is %s,score id %d \n ", scores[i].Name, scores[i].Score)
		}
	}
	for _, nodeScore := range scores {
		if nodeScore.Score > highest {
			highest = nodeScore.Score
		}
	}

	fmt.Printf("highest is %d \n",  highest)

	if highest == 0 {
		highest = highest + 1
	}
	// Set Range to [0-100]
	for i, nodeScore := range scores {
		scores[i].Score = nodeScore.Score * framework.MaxNodeScore / highest
	}
	return framework.NewStatus(framework.Success, "")
}

func (y *LVM) ScoreExtensions() framework.ScoreExtensions {
	return y
}
