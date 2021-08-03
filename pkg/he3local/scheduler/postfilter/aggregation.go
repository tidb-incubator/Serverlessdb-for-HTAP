package postfilter

import (
	"github.com/tidb-incubator/Serverlessdb-for-HTAP/pkg/he3local/scheduler/filter"
	v1 "k8s.io/api/core/v1"
	"k8s.io/klog"
	framework "k8s.io/kubernetes/pkg/scheduler/framework/v1alpha1"
	"sync"
)

const Workers int = 4

type StorageData struct {
	Size int64
}

func (s *StorageData) Clone() framework.StateData {
	c := &StorageData{
		Size: s.Size,
	}
	return c
}

func CollectMaxValue(vg string, state *framework.CycleState, nodes []*v1.Node, filteredNodesStatuses framework.NodeToStatusMap) *framework.Status {
	Max := StorageData{Size: 0}
	for _, n := range nodes {
		if filteredNodesStatuses[n.GetName()].IsSuccess() {
			vgsize, err := filter.GetVGSize(n.Labels[vg])
			if err != nil {
				framework.NewStatus(framework.Error, "get vg size err")
			}
			if vgsize > Max.Size {
				Max.Size = vgsize
			}
		}
	}
	if Max.Size == 0 {
		return framework.NewStatus(framework.Error, " The max "+vg+" of the nodes is 0")
	}
	
	klog.V(3).Infof("Max value %s is %d ","Max"+vg,Max.Size)
	state.Lock()
	state.Write(framework.StateKey("Max"+vg), &Max)
	defer state.Unlock()
	return framework.NewStatus(framework.Success, "")
}

func ParallelCollection(workers int, state *framework.CycleState, nodes []*v1.Node, vgs []string, filteredNodesStatuses framework.NodeToStatusMap) *framework.Status {
	var (
		stop <-chan struct{}
		mx   sync.RWMutex
		msg  = ""
	)
	pieces := len(vgs)
	toProcess := make(chan string, pieces)
	for _, v := range vgs {
		toProcess <- v
	}
	close(toProcess)
	if pieces < workers {
		workers = pieces
	}
	wg := sync.WaitGroup{}
	wg.Add(workers)
	for i := 0; i < workers; i++ {
		go func() {
			for value := range toProcess {
				select {
				case <-stop:
					return
				default:
					if re := CollectMaxValue(value, state, nodes, filteredNodesStatuses); !re.IsSuccess() {
						klog.V(3).Infof(re.Message())
						mx.Lock()
						msg += re.Message()
						mx.Unlock()
					}
				}
			}
			wg.Done()
		}()
	}
	wg.Wait()
	if msg != "" {
		return framework.NewStatus(framework.Error, msg)
	}
	return framework.NewStatus(framework.Success, "")
}
