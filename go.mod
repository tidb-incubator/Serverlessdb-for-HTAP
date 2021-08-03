module github.com/tidb-incubator/Serverlessdb-for-HTAP

go 1.16

require (
	github.com/container-storage-interface/spec v1.3.0
	github.com/dustin/go-humanize v1.0.0
	github.com/emicklei/go-restful v2.9.6+incompatible // indirect
	github.com/golang/glog v0.0.0-20160126235308-23def4e6c14b
	github.com/golang/protobuf v1.4.3
	github.com/kubernetes-csi/csi-lib-utils v0.7.0
	github.com/kubernetes-csi/external-snapshotter/v2 v2.1.1
	github.com/opencontainers/go-digest v1.0.0 // indirect
	github.com/pborman/uuid v1.2.0
	github.com/pingcap/advanced-statefulset/client v1.16.0
	github.com/pingcap/errors v0.11.4
	github.com/pingcap/tidb-operator v1.1.13
	github.com/prometheus/client_golang v1.0.0
	github.com/robfig/cron v1.1.0
	github.com/sirupsen/logrus v1.6.0
	github.com/spf13/cobra v1.1.3
	golang.org/x/net v0.0.0-20201021035429-f5854403a974
	golang.org/x/sys v0.0.0-20210415045647-66c3f260301c // indirect
	golang.org/x/xerrors v0.0.0-20200804184101-5ec99f83aff1 // indirect
	google.golang.org/grpc v1.27.0
	google.golang.org/protobuf v1.25.0 // indirect
	gopkg.in/square/go-jose.v2 v2.5.1 // indirect
	gopkg.in/yaml.v2 v2.4.0
	k8s.io/api v0.17.1
	k8s.io/apiextensions-apiserver v0.20.4 // indirect
	k8s.io/apimachinery v0.20.4
	k8s.io/client-go v0.20.4
	k8s.io/cloud-provider v0.20.4 // indirect
	k8s.io/component-base v0.20.4
	k8s.io/csi-translation-lib v0.20.4 // indirect
	k8s.io/klog v1.0.0
	k8s.io/kubernetes v1.17.1
	k8s.io/utils v0.0.0-20201110183641-67b214c5f920
	modernc.org/mathutil v1.0.0
	sigs.k8s.io/controller-runtime v0.4.0
)

// Pinned to kubernetes-1.16.2
replace (
	github.com/prometheus/client_golang => github.com/prometheus/client_golang v0.9.2
	github.com/uber-go/atomic => go.uber.org/atomic v1.5.0
	google.golang.org/grpc => google.golang.org/grpc v1.23.0
	k8s.io/api => k8s.io/api v0.0.0-20191122220107-b5267f2975e0
	k8s.io/apiextensions-apiserver => k8s.io/apiextensions-apiserver v0.0.0-20191122222427-64482ea217ff
	k8s.io/apimachinery => k8s.io/apimachinery v0.0.0-20191121175448-79c2a76c473a
	k8s.io/apiserver => k8s.io/apiserver v0.0.0-20191121180716-5a28f8b2ad8e
	k8s.io/cli-runtime => k8s.io/cli-runtime v0.0.0-20191122222818-9150eb3ded31
	k8s.io/client-go => k8s.io/client-go v0.0.0-20191121175918-3a262fe58afa
	k8s.io/cloud-provider => k8s.io/cloud-provider v0.0.0-20191121022508-6371aabbd7a7
	k8s.io/cluster-bootstrap => k8s.io/cluster-bootstrap v0.0.0-20191122223827-289de4a64c1c
	k8s.io/code-generator => k8s.io/code-generator v0.0.0-20191121015212-c4c8f8345c7e
	k8s.io/component-base => k8s.io/component-base v0.0.0-20191122163614-46ba8a4433be
	k8s.io/cri-api => k8s.io/cri-api v0.0.0-20191121183020-775aa3c1cf73
	k8s.io/csi-translation-lib => k8s.io/csi-translation-lib v0.0.0-20191121022617-4b18d293964d
	k8s.io/gengo => k8s.io/gengo v0.0.0-20191120174120-e74f70b9b27e
	k8s.io/kube-aggregator => k8s.io/kube-aggregator v0.0.0-20191122221605-1e8d331e4dcc
	k8s.io/kube-controller-manager => k8s.io/kube-controller-manager v0.0.0-20191122223648-5cfd5067047c
	k8s.io/kube-openapi => k8s.io/kube-openapi v0.0.0-20191107075043-30be4d16710a
	k8s.io/kube-proxy => k8s.io/kube-proxy v0.0.0-20191122223145-16f2c0c680a0
	k8s.io/kube-scheduler => k8s.io/kube-scheduler v0.0.0-20191121022142-fe73241eced9
	k8s.io/kubectl => k8s.io/kubectl v0.0.0-20191122225023-1e3c8b70f494
	k8s.io/kubelet => k8s.io/kubelet v0.0.0-20191122223325-9316382755ad
	k8s.io/legacy-cloud-providers => k8s.io/legacy-cloud-providers v0.0.0-20191122224431-860df69ff5cc
	k8s.io/metrics => k8s.io/metrics v0.0.0-20191122222628-19ed227de2b6
	k8s.io/sample-apiserver => k8s.io/sample-apiserver v0.0.0-20191122221846-294c70c3d5d4
	k8s.io/utils => k8s.io/utils v0.0.0-20191114200735-6ca3b61696b6

)
