module github.com/tidb-incubator/Serverlessdb-for-HTAP/pkg/webhook

go 1.13

require (
	github.com/BurntSushi/toml v0.3.1
	github.com/kirinlabs/HttpRequest v1.1.1
	github.com/onsi/gomega v1.5.0
	github.com/openshift/generic-admission-server v1.14.0
	github.com/pingcap/advanced-statefulset/client v1.16.0
	github.com/pingcap/kvproto v0.0.0-20200927054727-1290113160f0
	github.com/pingcap/tidb-operator v1.1.11
	gomodules.xyz/jsonpatch/v2 v2.0.1
	k8s.io/api v0.0.0
	k8s.io/apimachinery v0.0.0
	k8s.io/client-go v0.0.0
	k8s.io/component-base v0.0.0
	k8s.io/klog v1.0.0
	k8s.io/utils v0.0.0-20190801114015-581e00157fb1
)

replace github.com/renstrom/dedent => github.com/lithammer/dedent v1.1.0

replace k8s.io/api => k8s.io/api v0.0.0-20190918155943-95b840bb6a1f

replace k8s.io/apiextensions-apiserver => k8s.io/apiextensions-apiserver v0.0.0-20190918161926-8f644eb6e783

replace k8s.io/apimachinery => k8s.io/apimachinery v0.0.0-20190913080033-27d36303b655

replace k8s.io/apiserver => k8s.io/apiserver v0.0.0-20190918160949-bfa5e2e684ad

replace k8s.io/cli-runtime => k8s.io/cli-runtime v0.0.0-20190918162238-f783a3654da8

replace k8s.io/client-go => k8s.io/client-go v0.0.0-20190918160344-1fbdaa4c8d90

replace k8s.io/code-generator => k8s.io/code-generator v0.0.0-20190912054826-cd179ad6a269

replace k8s.io/csi-api => k8s.io/csi-api v0.0.0-20190118125032-c4557c74373f

replace k8s.io/kube-aggregator => k8s.io/kube-aggregator v0.0.0-20190918161219-8c8f079fddc3

replace k8s.io/kube-controller-manager => k8s.io/kube-controller-manager v0.0.0-20190918162944-7a93a0ddadd8

replace k8s.io/kube-proxy => k8s.io/kube-proxy v0.0.0-20190918162534-de037b596c1e

replace k8s.io/kube-scheduler => k8s.io/kube-scheduler v0.0.0-20190918162820-3b5c1246eb18

replace k8s.io/kubelet => k8s.io/kubelet v0.0.0-20190918162654-250a1838aa2c

replace k8s.io/metrics => k8s.io/metrics v0.0.0-20190918162108-227c654b2546

replace k8s.io/sample-apiserver => k8s.io/sample-apiserver v0.0.0-20190918161442-d4c9c65c82af

replace k8s.io/sample-cli-plugin => k8s.io/sample-cli-plugin v0.0.0-20190918162410-e45c26d066f2

replace k8s.io/sample-controller => k8s.io/sample-controller v0.0.0-20190918161628-92eb3cb7496c

replace k8s.io/cloud-provider => k8s.io/cloud-provider v0.0.0-20190918163234-a9c1f33e9fb9

replace k8s.io/cluster-bootstrap => k8s.io/cluster-bootstrap v0.0.0-20190918163108-da9fdfce26bb

replace k8s.io/component-base => k8s.io/component-base v0.0.0-20190918160511-547f6c5d7090

replace k8s.io/cri-api => k8s.io/cri-api v0.0.0-20190828162817-608eb1dad4ac

replace k8s.io/csi-translation-lib => k8s.io/csi-translation-lib v0.0.0-20190918163402-db86a8c7bb21

replace k8s.io/kubectl => k8s.io/kubectl v0.0.0-20190918164019-21692a0861df

replace k8s.io/legacy-cloud-providers => k8s.io/legacy-cloud-providers v0.0.0-20190918163543-cfa506e53441

replace k8s.io/node-api => k8s.io/node-api v0.0.0-20190918163711-2299658ad911

replace github.com/uber-go/atomic => go.uber.org/atomic v1.5.0

replace github.com/Azure/go-autorest => github.com/Azure/go-autorest v12.2.0+incompatible

replace github.com/prometheus/client_golang => github.com/prometheus/client_golang v0.9.4
