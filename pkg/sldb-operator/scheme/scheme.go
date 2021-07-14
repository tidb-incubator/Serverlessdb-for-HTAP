// Copyright 2019 PingCAP, Inc.
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

package scheme

import (
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	kubescheme "k8s.io/client-go/kubernetes/scheme"

	sldbscheme "github.com/tidb-incubator/Serverlessdb-for-HTAP/pkg/sldb-operator/client/clientset/versioned/scheme"
)

// Scheme gathers the schemes of native resources and custom resources used by sldb-operator
// in favor of the generic controller-runtime/client
var Scheme = runtime.NewScheme()

func init() {
	v1.AddToGroupVersion(Scheme, schema.GroupVersion{Version: "v1"})
	utilruntime.Must(sldbscheme.AddToScheme(Scheme))
	utilruntime.Must(kubescheme.AddToScheme(Scheme))
}
