#!/usr/bin/env bash

# Copyright 2017 PingCAP, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# See the License for the specific language governing permissions and
# limitations under the License.

set -o errexit
set -o nounset
set -o pipefail

SCRIPT_ROOT=$(dirname "${BASH_SOURCE[0]}")/..
cd $SCRIPT_ROOT

export GO111MODULE=on

go mod vendor

CODEGEN_PKG=${CODEGEN_PKG:-$(ls -d -1 ./vendor/k8s.io/code-generator 2>/dev/null || echo ../code-generator)}

# `--output-base $SCRIPT_ROOT` will output generated code to current dir
bash "${CODEGEN_PKG}"/generate-groups.sh "deepcopy,client,informer,lister" \
    github.com/tidb-incubator/Serverlessdb-for-HTAP/pkg/sldb-operator/client \
    github.com/tidb-incubator/Serverlessdb-for-HTAP/pkg/sldb-operator/apis \
    bcrds:v1alpha1 \
    --output-base $SCRIPT_ROOT \
    --go-header-file ./hack/boilerplate/boilerplate.generatego.txt
