package timetask

import (
	"fmt"
	"github.com/robfig/cron"
	"github.com/tidb-incubator/Serverlessdb-for-HTAP/pkg/scale-operator/autoscaler"
	"github.com/tidb-incubator/Serverlessdb-for-HTAP/pkg/scale-operator/sldbcluster"
	"k8s.io/klog"
	"sync"
	"time"

	//"k8s.io/apimachinery/pkg/labels"
	"github.com/tidb-incubator/Serverlessdb-for-HTAP/pkg/scale-operator/rulemanager"
	"github.com/tidb-incubator/Serverlessdb-for-HTAP/pkg/sldb-operator/apis/bcrds/v1alpha1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const threadNum = 20

var globalMutex sync.Mutex

func testFunc(sldb *v1alpha1.ServerlessDB) (bool, error) {
	klog.Infof("testFunc namespace %s, name %s", sldb.Namespace, sldb.Name)
	return true, nil
}

func getSldbList() (*v1alpha1.ServerlessDBList, error) {
	sldbList, err := sldbcluster.SldbClient.Client.BcrdsV1alpha1().ServerlessDBs("").List(metav1.ListOptions{})
	if err != nil {
		klog.Infof("list ServerlessDBs failed")
		return sldbList, err
	}
	return sldbList, nil
}

func getSldbListLimitTime(test func() (*v1alpha1.ServerlessDBList, error), duration time.Duration) (*v1alpha1.ServerlessDBList, error) {
	var ch = make(chan bool)
	var sldbList *v1alpha1.ServerlessDBList
	//var err error
	go func() {
		go func() {
			<-time.NewTimer(duration).C
			ch <- true
		}()
		sldbList, _ = test()
		ch <- false
	}()
	waitV := <-ch
	if waitV == true {
		return nil, fmt.Errorf("getSldbList is timeout")
	}
	return sldbList, nil
}

var taskMap = make(map[string]bool)

func execTask(funcVar func(sldb *v1alpha1.ServerlessDB) (bool, error), name string) error {
	//30s timeout will next handler
	sldbList, err := getSldbListLimitTime(getSldbList, 3e10)
	if err != nil {
		klog.Infof("getSldbListlimitTime is timeout %v", err)
		return err
	}
	clen := len(sldbList.Items)
	hcur := 0
	wg := sync.WaitGroup{}
	wg.Add(threadNum)
	for i := 0; i < threadNum; i++ {
		go func(i int) {
			defer wg.Done()
			for {
				if hcur >= clen {
					break
				}
				curi := -1
				globalMutex.Lock()
				if hcur < clen {
					curi = hcur
					hcur = hcur + 1
				} else {
					globalMutex.Unlock()
					break
				}
				sldbClus := sldbList.Items[curi]
				//avoid task repetitive execution
				if _, ok := taskMap[sldbClus.Namespace+"-"+sldbClus.Name+"-"+name]; !ok {
					taskMap[sldbClus.Namespace+"-"+sldbClus.Name+"-"+name] = true
					globalMutex.Unlock()
				} else {
					globalMutex.Unlock()
					continue
				}
				klog.Infof("[%s/%s]-%s", sldbClus.Namespace, sldbClus.Name, name)
				_, err := funcVar(&sldbClus)
				if err != nil {
					klog.Infof("[%s/%s]-%s funcVar err %v", sldbClus.Namespace, sldbClus.Name, name, err)
				}
				globalMutex.Lock()
				delete(taskMap, sldbClus.Namespace+"-"+sldbClus.Name+"-"+name)
				globalMutex.Unlock()
			}
		}(i)
	}
	wg.Wait()
	return nil
}

func scalerRuleTask() {
	klog.Infof("[scalerRuleTask action start]")
	execTask(rulemanager.AutoscaleByRule, "Rule")
	klog.Infof("[scalerRuleTask action end]")
}

func scalerOutInTask() {
	klog.Infof("[scalerOutTask action start]")
	execTask(autoscaler.PtrScalerManager.SCalerOutInHandler, "Scaler")
	klog.Infof("[scalerOutTask action end]")
}

func ScalerInBaseOnMidWare() {
	klog.Infof("[ScalerInBaseOnMidWare action start]")
	execTask(autoscaler.PtrScalerManager.ScalerBaseOnMidWare, "Scaler")
	klog.Infof("[ScalerInBaseOnMidWare action end]")
}

func expandStorageTask() {
	klog.Infof("[check expandStorageTask action start]")
	execTask(autoscaler.PtrScalerManager.TiKVSCalerHandler, "storageScaler")
	klog.Infof("[check expandStorageTask action end]")
}

func ScalerTimeTask() {
	autoscaler.AutoScalerInit()
	c := cron.New()
	oneSecondSpec := "*/1 * * * * ?"
	c.AddFunc(oneSecondSpec, ScalerInBaseOnMidWare)
	secondSpec := "*/15 * * * * ?"
	c.AddFunc(secondSpec, scalerOutInTask)
	c.AddFunc(secondSpec, expandStorageTask)
	ruleSpec := "30 59 */1 * * ?"
	c.AddFunc(ruleSpec, scalerRuleTask)
	c.Start()
}
