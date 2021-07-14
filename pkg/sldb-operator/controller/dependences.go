// Copyright 2018 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package controller

import (
	"flag"
	"io/ioutil"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/util/wait"
	"os"
	"time"

	pcversioned "github.com/pingcap/tidb-operator/pkg/client/clientset/versioned"
	pcfake "github.com/pingcap/tidb-operator/pkg/client/clientset/versioned/fake"
	pcinformers "github.com/pingcap/tidb-operator/pkg/client/informers/externalversions"
	pclisters "github.com/pingcap/tidb-operator/pkg/client/listers/pingcap/v1alpha1"
	"github.com/tidb-incubator/Serverlessdb-for-HTAP/pkg/sldb-operator/apis/bcrds/v1alpha1"
	"github.com/tidb-incubator/Serverlessdb-for-HTAP/pkg/sldb-operator/client/clientset/versioned"
	"github.com/tidb-incubator/Serverlessdb-for-HTAP/pkg/sldb-operator/client/clientset/versioned/fake"
	informers "github.com/tidb-incubator/Serverlessdb-for-HTAP/pkg/sldb-operator/client/informers/externalversions"
	listers "github.com/tidb-incubator/Serverlessdb-for-HTAP/pkg/sldb-operator/client/listers/bcrds/v1alpha1"
	"github.com/tidb-incubator/Serverlessdb-for-HTAP/pkg/sldb-operator/scheme"
	"gopkg.in/yaml.v2"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	kubeinformers "k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	kubefake "k8s.io/client-go/kubernetes/fake"
	eventv1 "k8s.io/client-go/kubernetes/typed/core/v1"
	appslisters "k8s.io/client-go/listers/apps/v1"
	batchlisters "k8s.io/client-go/listers/batch/v1"
	corelisterv1 "k8s.io/client-go/listers/core/v1"
	"k8s.io/client-go/rest"
	restclient "k8s.io/client-go/rest"
	"k8s.io/client-go/tools/record"
	"k8s.io/klog"
	"sigs.k8s.io/controller-runtime/pkg/client"
	controllerfake "sigs.k8s.io/controller-runtime/pkg/client/fake"
)

// CLIConfig is used save all configuration read from command line parameters
type CLIConfig struct {
	PrintVersion bool
	// The number of workers that are allowed to sync concurrently.
	// Larger number = more responsive management, but more CPU
	// (and network) load
	Workers int
	// Controls whether operator should manage kubernetes cluster
	LeaseDuration time.Duration
	RenewDuration time.Duration
	RetryPeriod   time.Duration
	WaitDuration  time.Duration
	// ResyncDuration is the resync time of informer
	ResyncDuration time.Duration
	// Defines whether tidb operator run in test mode, test mode is
	// only open when test
	TestMode bool
	// Selector is used to filter CR labels to decide
	// what resources should be watched and synced by controller
	Selector string
	// Default config path for config file
	configPath string
	// Default config read from config file
	Config Config

	InstanceFetchInterval   time.Duration
	ReadonlyAvailPercentage float64
	ProxyComponentHttpPort  int
}

type Storage struct {
	StorageClassName string `yaml:"storageClassName"`
}

type TiDBCluster struct {
	Version string        `yaml:"version"`
	PD      Specification `yaml:"pd"`
	TiDB    Specification `yaml:"tidb"`
	TiKV    Specification `yaml:"tikv"`
}

type Monitor struct {
	Specification         `yaml:",inline"`
	PrometheusReserveDays int `yaml:"prometheusReserveDays"`
}

type Proxy struct {
	Specification `yaml:",inline"`
}

type ResourceList struct {
	Cpu     string `yaml:"cpu"`
	Memory  string `yaml:"memory"`
	Storage string `yaml:"storage,omitempty"`
}

// ResourceRequirements describes the compute resource requirements.
type ResourceRequirements struct {
	Limits   ResourceList `yaml:"limits"`
	Requests ResourceList `yaml:"requests"`
}

func (r *ResourceRequirements) ParseResource() corev1.ResourceRequirements {
	resourceRequirements := corev1.ResourceRequirements{
		Limits: corev1.ResourceList{
			corev1.ResourceCPU:    resource.MustParse(r.Limits.Cpu),
			corev1.ResourceMemory: resource.MustParse(r.Limits.Memory),
		},
		Requests: corev1.ResourceList{
			corev1.ResourceCPU:    resource.MustParse(r.Requests.Cpu),
			corev1.ResourceMemory: resource.MustParse(r.Requests.Memory),
		},
	}
	if res, err := resource.ParseQuantity(r.Limits.Storage); err == nil {
		resourceRequirements.Limits[corev1.ResourceStorage] = res
	}
	if res, err := resource.ParseQuantity(r.Requests.Storage); err == nil {
		resourceRequirements.Requests[corev1.ResourceStorage] = res
	}
	return resourceRequirements
}

type Specification struct {
	Enable               bool   `yaml:"enable"`
	Image                string `yaml:"image"`
	Replicas             int32  `yaml:"replicas"`
	ResourceRequirements `yaml:",inline"`
}

type Duration struct {
	CreateTimeout string `yaml:"createTimeout"`
	RestartCheck  string `yaml:"restartCheck"`
	BackupTimeout string `yaml:"backupTimeout"`
}

type Config struct {
	Duration    Duration    `yaml:"duration"`
	Storage     Storage     `yaml:"storage"`
	TiDBCluster TiDBCluster `yaml:"tidbCluster"`
	Monitor     Monitor     `yaml:"monitor"`
	Proxy       Proxy       `yaml:"proxy"`
}

// DefaultCLIConfig returns the default command line configuration
func DefaultCLIConfig() *CLIConfig {
	return &CLIConfig{
		Workers:                 5,
		LeaseDuration:           15 * time.Second,
		RenewDuration:           5 * time.Second,
		RetryPeriod:             3 * time.Second,
		WaitDuration:            5 * time.Second,
		ResyncDuration:          30 * time.Second,
		Selector:                "",
		InstanceFetchInterval:   5 * time.Minute,
		ReadonlyAvailPercentage: 15.0,
		ProxyComponentHttpPort:  9797,
	}
}

// AddFlag adds a flag for setting global feature gates to the specified FlagSet.
func (c *CLIConfig) AddFlag(_ *flag.FlagSet) {
	flag.BoolVar(&c.PrintVersion, "V", false, "Show version and quit")
	flag.BoolVar(&c.PrintVersion, "version", false, "Show version and quit")
	flag.IntVar(&c.Workers, "workers", c.Workers, "The number of workers that are allowed to sync concurrently. Larger number = more responsive management, but more CPU (and network) load")
	flag.DurationVar(&c.ResyncDuration, "resync-duration", c.ResyncDuration, "Resync time of informer")
	flag.BoolVar(&c.TestMode, "test-mode", false, "whether tidb-operator run in test mode")
	flag.StringVar(&c.Selector, "selector", c.Selector, "Selector (label query) to filter on, supports '=', '==', and '!='")
	flag.StringVar(&c.configPath, "config", "operator_config.yaml", "config file. ")
	flag.DurationVar(&c.InstanceFetchInterval, "instance-update-interval", c.InstanceFetchInterval, "Interval for instance status fetching.")
	flag.Float64Var(&c.ReadonlyAvailPercentage, "readonly-avail-percentage", c.ReadonlyAvailPercentage, "Set instance as readonly when available storage percentage LE this value.")
	flag.IntVar(&c.ProxyComponentHttpPort, "proxy-component-http-port", c.ProxyComponentHttpPort, "Http port number of he3proxy component.")

	flag.Parse()

	c.unmarshalConfig()
}

func (c *CLIConfig) unmarshalConfig() {
	f, err := os.Open(c.configPath)
	if err != nil {
		klog.Fatalf("read config file %s failed %v. ", c.configPath, err)
	}

	msg, err := ioutil.ReadAll(f)
	if err != nil {
		klog.Fatalf("read config file body %s failed %v. ", c.configPath, err)
	}

	err = yaml.Unmarshal(msg, &c.Config)
}

type Controls struct {
	ServerlessControl ServerlessDBControlInterface
}

// Dependencies is used to store all shared dependent resources to avoid
// pass parameters everywhere.
type Dependencies struct {
	// CLIConfig represents all parameters read from command line
	CLIConfig *CLIConfig

	RestConfig *restclient.Config
	// Operator client interface
	Clientset versioned.Interface
	// pingcap client interface
	PingcapClientset pcversioned.Interface
	// Kubernetes client interface
	KubeClientset                  kubernetes.Interface
	GenericClient                  client.Client
	InformerFactory                informers.SharedInformerFactory
	PingcapInformerFactory         pcinformers.SharedInformerFactory
	KubeInformerFactory            kubeinformers.SharedInformerFactory
	LabelFilterKubeInformerFactory kubeinformers.SharedInformerFactory
	Recorder                       record.EventRecorder

	// Listers
	PodLister              corelisterv1.PodLister
	JobLister              batchlisters.JobLister
	ServiceLister          corelisterv1.ServiceLister
	SecretLister           corelisterv1.SecretLister
	PVCLister              corelisterv1.PersistentVolumeClaimLister
	DeploymentLister       appslisters.DeploymentLister
	StatefulSetLister      appslisters.StatefulSetLister
	ServerlessDBLister     listers.ServerlessDBLister
	TiDBClusterLister      pclisters.TidbClusterLister
	TiDBMonitorLister      pclisters.TidbMonitorLister
	TiDBInitializerLister  pclisters.TidbInitializerLister
	PcBackupLister         pclisters.BackupLister
	PcBackupScheduleLister pclisters.BackupScheduleLister
	PcRestoreLister        pclisters.RestoreLister

	// Controls
	Controls
}

func newRealControls(
	clientset versioned.Interface,
	kubeClientset kubernetes.Interface,
	genericCli client.Client,
	informerFactory informers.SharedInformerFactory,
	kubeInformerFactory kubeinformers.SharedInformerFactory,
	recorder record.EventRecorder) Controls {
	// Shared variables to construct `Dependencies` and some of its fields
	var (
		serverlessDBLister = informerFactory.Bcrds().V1alpha1().ServerlessDBs().Lister()
		//statefulSetLister  = kubeInformerFactory.Apps().V1().StatefulSets().Lister()
	)
	return Controls{
		// real controller init
		ServerlessControl: NewRealServerlessDBControl(clientset, serverlessDBLister, recorder),
	}
}

func newDependencies(cliCfg *CLIConfig, restConfig *restclient.Config, clientset versioned.Interface,
	pcClientset pcversioned.Interface, kubeClientset kubernetes.Interface, genericCli client.Client,
	informerFactory informers.SharedInformerFactory, pcInformerFactory pcinformers.SharedInformerFactory,
	kubeInformerFactory kubeinformers.SharedInformerFactory, labelFilterKubeInformerFactory kubeinformers.SharedInformerFactory,
	recorder record.EventRecorder) *Dependencies {

	return &Dependencies{
		CLIConfig:                      cliCfg,
		RestConfig:                     restConfig,
		InformerFactory:                informerFactory,
		PingcapInformerFactory:         pcInformerFactory,
		Clientset:                      clientset,
		PingcapClientset:               pcClientset,
		KubeClientset:                  kubeClientset,
		GenericClient:                  genericCli,
		KubeInformerFactory:            kubeInformerFactory,
		LabelFilterKubeInformerFactory: labelFilterKubeInformerFactory,
		Recorder:                       recorder,

		// Listers
		PodLister:              kubeInformerFactory.Core().V1().Pods().Lister(),
		JobLister:              kubeInformerFactory.Batch().V1().Jobs().Lister(),
		ServiceLister:          kubeInformerFactory.Core().V1().Services().Lister(),
		SecretLister:           kubeInformerFactory.Core().V1().Secrets().Lister(),
		PVCLister:              kubeInformerFactory.Core().V1().PersistentVolumeClaims().Lister(),
		DeploymentLister:       kubeInformerFactory.Apps().V1().Deployments().Lister(),
		StatefulSetLister:      kubeInformerFactory.Apps().V1().StatefulSets().Lister(),
		ServerlessDBLister:     informerFactory.Bcrds().V1alpha1().ServerlessDBs().Lister(),
		TiDBClusterLister:      pcInformerFactory.Pingcap().V1alpha1().TidbClusters().Lister(),
		TiDBMonitorLister:      pcInformerFactory.Pingcap().V1alpha1().TidbMonitors().Lister(),
		TiDBInitializerLister:  pcInformerFactory.Pingcap().V1alpha1().TidbInitializers().Lister(),
		PcBackupLister:         pcInformerFactory.Pingcap().V1alpha1().Backups().Lister(),
		PcBackupScheduleLister: pcInformerFactory.Pingcap().V1alpha1().BackupSchedules().Lister(),
		PcRestoreLister:        pcInformerFactory.Pingcap().V1alpha1().Restores().Lister(),
	}
}

// NewDependencies is used to construct the dependencies
func NewDependencies(ns string, cliCfg *CLIConfig, restConfig *restclient.Config, clientset versioned.Interface,
	pcCliset pcversioned.Interface, kubeClientset kubernetes.Interface, genericCli client.Client) *Dependencies {

	var (
		options     []informers.SharedInformerOption
		pcoptions   []pcinformers.SharedInformerOption
		kubeoptions []kubeinformers.SharedInformerOption
	)
	//if !cliCfg.ClusterScoped {
	//	options = append(options, informers.WithNamespace(ns))
	//	kubeoptions = append(kubeoptions, kubeinformers.WithNamespace(ns))
	//}
	// fixme: modify labels
	tweakListOptionsFunc := func(options *metav1.ListOptions) {
		if len(options.LabelSelector) > 0 {
			options.LabelSelector += ",app.kubernetes.io/managed-by=tidb-operator"
		} else {
			options.LabelSelector = "app.kubernetes.io/managed-by=tidb-operator"
		}
	}
	labelKubeOptions := append(kubeoptions, kubeinformers.WithTweakListOptions(tweakListOptionsFunc))
	tweakListOptionsFunc = func(options *metav1.ListOptions) {
		if len(cliCfg.Selector) > 0 {
			options.LabelSelector = cliCfg.Selector
		}
	}
	options = append(options, informers.WithTweakListOptions(tweakListOptionsFunc))
	pcoptions = append(pcoptions, pcinformers.WithTweakListOptions(tweakListOptionsFunc))

	// Initialize the informer factories
	informerFactory := informers.NewSharedInformerFactoryWithOptions(clientset, cliCfg.ResyncDuration, options...)
	pingcapInformerFactory := pcinformers.NewSharedInformerFactoryWithOptions(pcCliset, cliCfg.ResyncDuration, pcoptions...)
	kubeInformerFactory := kubeinformers.NewSharedInformerFactoryWithOptions(kubeClientset, cliCfg.ResyncDuration, kubeoptions...)
	labelFilterKubeInformerFactory := kubeinformers.NewSharedInformerFactoryWithOptions(kubeClientset, cliCfg.ResyncDuration, labelKubeOptions...)

	// Initialize the event recorder
	eventBroadcaster := record.NewBroadcasterWithCorrelatorOptions(record.CorrelatorOptions{QPS: 1})
	eventBroadcaster.StartLogging(klog.V(2).Infof)
	eventBroadcaster.StartRecordingToSink(&eventv1.EventSinkImpl{
		Interface: eventv1.New(kubeClientset.CoreV1().RESTClient()).Events("")})
	recorder := eventBroadcaster.NewRecorder(v1alpha1.Scheme, corev1.EventSource{Component: "sldb-controller-manager"})
	deps := newDependencies(cliCfg, restConfig, clientset, pcCliset, kubeClientset, genericCli, informerFactory,
		pingcapInformerFactory, kubeInformerFactory, labelFilterKubeInformerFactory, recorder)
	deps.Controls = newRealControls(clientset, kubeClientset, genericCli, informerFactory, kubeInformerFactory, recorder)
	return deps
}

// NewFakeDependencies returns a fake dependencies for testing
func NewFakeDependencies() *Dependencies {
	cli := fake.NewSimpleClientset()
	pccli := pcfake.NewSimpleClientset()
	kubeCli := kubefake.NewSimpleClientset()
	genCli := controllerfake.NewFakeClientWithScheme(scheme.Scheme)
	cliCfg := DefaultCLIConfig()
	informerFactory := informers.NewSharedInformerFactory(cli, 0)
	pcinformerFactory := pcinformers.NewSharedInformerFactory(pccli, 0)
	kubeInformerFactory := kubeinformers.NewSharedInformerFactory(kubeCli, 0)
	labelFilterKubeInformerFactory := kubeinformers.NewSharedInformerFactory(kubeCli, 0)
	recorder := record.NewFakeRecorder(100)

	cfg, _ := rest.InClusterConfig()
	deps := newDependencies(cliCfg, cfg, cli, pccli, kubeCli, genCli, informerFactory, pcinformerFactory, kubeInformerFactory, labelFilterKubeInformerFactory, recorder)
	newFakeCLIConfig(deps)
	deps.Controls = newFakeControl(kubeCli, informerFactory, kubeInformerFactory)
	initFakeInformer(deps)
	return deps
}

func initFakeInformer(deps *Dependencies) {
	klog.Info("Init fake informers")
	//stop := make(chan struct{})
	//defer close(stop)
	deps.InformerFactory.Start(wait.NeverStop)
	deps.InformerFactory.WaitForCacheSync(wait.NeverStop)
	deps.PingcapInformerFactory.Start(wait.NeverStop)
	deps.PingcapInformerFactory.WaitForCacheSync(wait.NeverStop)
	deps.KubeInformerFactory.Start(wait.NeverStop)
	deps.KubeInformerFactory.WaitForCacheSync(wait.NeverStop)
}

func newFakeCLIConfig(deps *Dependencies) {
	cliCfg := DefaultCLIConfig()
	specification := Specification{
		Replicas: 1,
		ResourceRequirements: ResourceRequirements{
			Limits: ResourceList{
				Cpu:     "1",
				Memory:  "1Mi",
				Storage: "1Mi",
			},
			Requests: ResourceList{
				Cpu:     "1",
				Memory:  "1Mi",
				Storage: "1Mi",
			},
		},
	}
	cliCfg.Config.TiDBCluster.PD = specification
	cliCfg.Config.TiDBCluster.TiDB = specification
	cliCfg.Config.TiDBCluster.TiKV = specification
	cliCfg.Config.Monitor.Specification = specification
	cliCfg.Config.Proxy.Specification = specification

	cliCfg.Config.Monitor.Enable = true
	deps.CLIConfig = cliCfg
}

func newFakeControl(kubeClientset kubernetes.Interface, informerFactory informers.SharedInformerFactory, kubeInformerFactory kubeinformers.SharedInformerFactory) Controls {
	// Shared variables to construct `Dependencies` and some of its fields
	return Controls{
		ServerlessControl: NewFakeServerlessDBControl(informerFactory.Bcrds().V1alpha1().ServerlessDBs()),
	}
}
