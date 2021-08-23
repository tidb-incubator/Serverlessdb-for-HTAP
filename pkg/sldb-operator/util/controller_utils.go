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

package util

import (
	"fmt"
	"github.com/tidb-incubator/Serverlessdb-for-HTAP/pkg/sldb-operator/apis/bcrds/v1alpha1"
)

var (
	// controllerKind contains the schema.GroupVersionKind for ServerlessDB controller type.
	ServerlessDBControllerKind = v1alpha1.SchemeGroupVersion.WithKind("ServerlessDB")
)

const (
	// AnnPingcapToolOptions describes optional command line args for a tool.
	AnnPingcapToolOptions = "tidb.pingcap.com/tool-options"
	DefaultRootUser       = "root"
	DefaultRootPassword   = "Nz_2@sMw6R"
)

// RequeueError is used to requeue the item, this error type should't be considered as a real error
type RequeueError struct {
	s string
}

func (re *RequeueError) Error() string {
	return re.s
}

// RequeueErrorf returns a RequeueError
func RequeueErrorf(format string, a ...interface{}) error {
	return &RequeueError{fmt.Sprintf(format, a...)}
}

// IsRequeueError returns whether err is a RequeueError
func IsRequeueError(err error) bool {
	_, ok := err.(*RequeueError)
	return ok
}

// IgnoreError is used to ignore this item, this error type should't be considered as a real error, no need to requeue
type IgnoreError struct {
	s string
}

func (re *IgnoreError) Error() string {
	return re.s
}

// IgnoreErrorf returns a IgnoreError
func IgnoreErrorf(format string, a ...interface{}) error {
	return &IgnoreError{fmt.Sprintf(format, a...)}
}

// IsIgnoreError returns whether err is a IgnoreError
func IsIgnoreError(err error) bool {
	_, ok := err.(*IgnoreError)
	return ok
}


func GetProxyResourceName(instanceName string) string {
	return fmt.Sprintf("%s-proxy", instanceName)
}
