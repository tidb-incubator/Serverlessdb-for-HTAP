/*
Copyright 2017 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package main

import (
	"flag"
	"fmt"
	"github.com/tidb-incubator/Serverlessdb-for-HTAP/pkg/he3local/lvm"
	pb "github.com/tidb-incubator/Serverlessdb-for-HTAP/pkg/he3local/lvmpb"
	"github.com/tidb-incubator/Serverlessdb-for-HTAP/pkg/he3local/lvmserver"
	"github.com/tidb-incubator/Serverlessdb-for-HTAP/pkg/he3local/util"
	"google.golang.org/grpc"
	"net"
	"os"
	"path"
	"time"
)

const (
	PORT = ":50001"
)

func init() {
	flag.Set("logtostderr", "true")
}

var (
	endpoint          = flag.String("endpoint", "unix://tmp/csi.sock", "CSI endpoint")
	driverName        = flag.String("drivername", "hostpath.csi.k8s.io", "name of the driver")
	nodeID            = flag.String("nodeid", "", "node id")
	maxVolumesPerNode = flag.Int64("maxvolumespernode", 0, "limit of volumes per node")
	showVersion       = flag.Bool("version", false, "Show version.")
	// Set by the build process
	version = "dev"
)

//var NodeId string

func main() {
	flag.Parse()
	//	NodeId = nodeID

	if *showVersion {
		baseName := path.Base(os.Args[0])
		fmt.Println(baseName, version)
		return
	}
	go RPCForSnap()
	handle()
	os.Exit(0)
}

func handle() {
	driver, err := lvm.NewHostPathDriver(*driverName, *nodeID, *endpoint, *maxVolumesPerNode, version)
	if err != nil {
		fmt.Printf("Failed to initialize driver: %s", err.Error())
		os.Exit(1)
	}

	util.SetNodeId(*nodeID)
	util.Updatenodeslabs()
	util.UpdateThinForNode()
	go loopupdatevg()
	go loopupthing()
	driver.Run()
}

func loopupdatevg() {

	for {
		fmt.Println("begin run Updatenodeslabs")
		util.Updatenodeslabs()
		time.Sleep(100 * time.Second)
	}

	fmt.Println("exit for update vg")

}

func loopupthing() {

	for {
		fmt.Println("begin run UpdateThinForNode")
		util.UpdateThinForNode()
		
		time.Sleep(100 * time.Second)
	}

	fmt.Println("exit for update vg")

}

func RPCForSnap() {
	lis, err := net.Listen("tcp", PORT)
	if err != nil {
		fmt.Printf("failed to listen: %v", err)
		os.Exit(1)
	}
	s := grpc.NewServer()
	pb.RegisterSnapshotServer(s, &lvmserver.SnapSerice{})
	pb.RegisterLVMVolumeServer(s, &lvmserver.LVMSerice{})
	fmt.Println("rpc服务已经开启")
	s.Serve(lis)
}
