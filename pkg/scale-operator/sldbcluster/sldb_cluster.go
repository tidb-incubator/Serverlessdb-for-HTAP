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
