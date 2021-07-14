package sldbcluster

import (
	asCli "github.com/pingcap/advanced-statefulset/client/client/clientset/versioned"
	pingcapCli "github.com/pingcap/tidb-operator/pkg/client/clientset/versioned"
	pingcapinformer "github.com/pingcap/tidb-operator/pkg/client/informers/externalversions"
	pingcapLister "github.com/pingcap/tidb-operator/pkg/client/listers/pingcap/v1alpha1"
	sldbCli "github.com/tidb-incubator/Serverlessdb-for-HTAP/pkg/sldb-operator/client/clientset/versioned"
	sldbinformer "github.com/tidb-incubator/Serverlessdb-for-HTAP/pkg/sldb-operator/client/informers/externalversions"
	slLister "github.com/tidb-incubator/Serverlessdb-for-HTAP/pkg/sldb-operator/client/listers/bcrds/v1alpha1"
	kubeinformers "k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/klog"
	"time"
)

var SldbClient SlabInterface

type SlabInterface struct {
	Client                       *sldbCli.Clientset
	Lister                       slLister.ServerlessDBLister
	PingCapCli                   *pingcapCli.Clientset
	PingCapLister                pingcapLister.TidbClusterLister
	AsCli                        *asCli.Clientset
	KubeCli                      *kubernetes.Clientset
	SldbSharedInformerFactory    sldbinformer.SharedInformerFactory
	PingCapSharedInformerFactory pingcapinformer.SharedInformerFactory
	KubeInformerFactory          kubeinformers.SharedInformerFactory
}

func Sldb_Cluster_Init(cfg *rest.Config) {
	kubecli, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		klog.Fatalf("failed to get kubernetes Clientset: %v", err)
	}
	SldbClient.KubeInformerFactory = kubeinformers.NewSharedInformerFactory(kubecli, 30*time.Second)
	ascli, err := asCli.NewForConfig(cfg)
	if err != nil {
		klog.Fatalf("failed to get advanced-statefulset Clientset: %v", err)
	}
	sldbcli, err := sldbCli.NewForConfig(cfg)
	if err != nil {
		klog.Fatalf("failed to get sldbclientset Clientset: %v", err)
	}
	SldbClient.SldbSharedInformerFactory = sldbinformer.NewSharedInformerFactory(sldbcli, 30*time.Second)
	pingcapcli, err := pingcapCli.NewForConfig(cfg)
	if err != nil {
		klog.Fatalf("failed to get pingcapCli Clientset: %v", err)
	}
	SldbClient.PingCapSharedInformerFactory = pingcapinformer.NewSharedInformerFactory(pingcapcli, 30*time.Second)
	SldbClient.Client = sldbcli
	SldbClient.Lister = SldbClient.SldbSharedInformerFactory.Bcrds().V1alpha1().ServerlessDBs().Lister()
	SldbClient.PingCapCli = pingcapcli
	SldbClient.PingCapLister = SldbClient.PingCapSharedInformerFactory.Pingcap().V1alpha1().TidbClusters().Lister()
	SldbClient.KubeCli = kubecli
	SldbClient.AsCli = ascli
}

//func UpdateSldbSpec(sldb *v1alpha1.ServerlessDB) error {
//	ns := sldb.Namespace
//	sldbName := sldb.Name
//	oldSldb := sldb.DeepCopy()
//
//	// don't wait due to limited number of clients, but backoff after the default number of steps
//	err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
//		var updateErr error
//		_, updateErr = SldbClient.Client.BcrdsV1alpha1().ServerlessDBs(ns).Update(sldb)
//		if updateErr == nil {
//			klog.Infof("sldb update: [%s/%s] updated successfully", ns, sldbName)
//			return nil
//		}
//		klog.V(4).Infof("failed to update sldb cluster: [%s/%s], error: %v", ns, sldbName, updateErr)
//		if updated, err := SldbClient.Lister.ServerlessDBs(ns).Get(sldbName); err == nil {
//			// make a copy so we don't mutate the shared cache
//			sldb = updated.DeepCopy()
//			//
//			sldb.Spec = *oldSldb.Spec.DeepCopy()
//		} else {
//			utilruntime.HandleError(fmt.Errorf("error getting updated cluster %s/%s from lister: %v", ns, sldbName, err))
//		}
//		return updateErr
//	})
//	if err != nil {
//		klog.Errorf("failed to update TidbClusterAutoScaler: [%s/%s], error: %v", ns, sldbName, err)
//	}
//	return err
//}
