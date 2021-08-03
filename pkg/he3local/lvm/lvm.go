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

package lvm

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	//"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/golang/glog"
	//"golang.org/x/net/context"
	"github.com/tidb-incubator/Serverlessdb-for-HTAP/pkg/he3local/util"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"k8s.io/kubernetes/pkg/volume/util/volumepathhandler"

	utilexec "k8s.io/utils/exec"

	timestamp "github.com/golang/protobuf/ptypes/timestamp"
)

const (
	kib    int64 = 1024
	mib    int64 = kib * 1024
	gib    int64 = mib * 1024
	gib100 int64 = gib * 100
	tib    int64 = gib * 1024
	tib100 int64 = tib * 100
)

type hostPath struct {
	name              string
	nodeID            string
	version           string
	endpoint          string
	ephemeral         bool
	maxVolumesPerNode int64

	ids *identityServer
	ns  *nodeServer
	cs  *controllerServer
}

type hostPathVolume struct {
	VolName string `json:"volName"`
	VolID   string `json:"volID"`
	VolSize int64  `json:"volSize"`
	//	VolPath       string     `json:"volPath"`
	//	MountPath     string     `json:"mountPath"`
	VolAccessType accessType `json:"volAccessType"`
	ParentVolID   string     `json:"parentVolID,omitempty"`
	ParentSnapID  string     `json:"parentSnapID,omitempty"`
	//	VolContentsource     *csi.VolumeContentSource `json:"volumeContentSource,omitempty"`

}

type hostPathSnapshot struct {
	Name         string              `json:"name"`
	Id           string              `json:"id"`
	VolID        string              `json:"volID"`
	Path         string              `json:"path"`
	CreationTime timestamp.Timestamp `json:"creationTime"`
	SizeBytes    int64               `json:"sizeBytes"`
	ReadyToUse   bool                `json:"readyToUse"`
}

var (
	vendorVersion = "dev"

	hostPathVolumes         map[string]hostPathVolume
	hostPathVolumeSnapshots map[string]hostPathSnapshot
	//cluster                 map[string]int64
)

const (
	// Directory where data for volumes and snapshots are persisted.
	// This can be ephemeral within the container or persisted if
	// backed by a Pod volume.
	dataRoot = "/csi-data-dir"

	// Extension with which snapshot files will be saved.
	snapshotExt = ".snap"
)

func init() {
	hostPathVolumes = map[string]hostPathVolume{}
	hostPathVolumeSnapshots = map[string]hostPathSnapshot{}

}

func NewHostPathDriver(driverName, nodeID, endpoint string, maxVolumesPerNode int64, version string) (*hostPath, error) {
	if driverName == "" {
		return nil, errors.New("no driver name provided")
	}

	if nodeID == "" {
		return nil, errors.New("no node id provided")
	}

	if endpoint == "" {
		return nil, errors.New("no driver endpoint provided")
	}
	if version != "" {
		vendorVersion = version
	}

	if err := os.MkdirAll(dataRoot, 0750); err != nil {
		return nil, fmt.Errorf("failed to create dataRoot: %v", err)
	}

	glog.Infof("Driver: %v ", driverName)
	glog.Infof("Version: %s", vendorVersion)

	return &hostPath{
		name:              driverName,
		version:           vendorVersion,
		nodeID:            nodeID,
		endpoint:          endpoint,
		maxVolumesPerNode: maxVolumesPerNode,
	}, nil
}

func getSnapshotID(file string) (bool, string) {
	glog.V(4).Infof("file: %s", file)
	// Files with .snap extension are volumesnapshot files.
	// e.g. foo.snap, foo.bar.snap
	if filepath.Ext(file) == snapshotExt {
		return true, strings.TrimSuffix(file, snapshotExt)
	}
	return false, ""
}

func discoverExistingSnapshots() {
	glog.V(4).Infof("discovering existing snapshots in %s", dataRoot)
	files, err := ioutil.ReadDir(dataRoot)
	if err != nil {
		glog.Errorf("failed to discover snapshots under %s: %v", dataRoot, err)
	}
	for _, file := range files {
		isSnapshot, snapshotID := getSnapshotID(file.Name())
		if isSnapshot {
			glog.V(4).Infof("adding snapshot %s from file %s", snapshotID, getSnapshotPath(snapshotID))
			hostPathVolumeSnapshots[snapshotID] = hostPathSnapshot{
				Id:         snapshotID,
				Path:       getSnapshotPath(snapshotID),
				ReadyToUse: true,
			}
		}
	}
}

func (hp *hostPath) Run() {
	// Create GRPC servers
	hp.ids = NewIdentityServer(hp.name, hp.version)
	hp.ns = NewNodeServer(hp.nodeID, hp.maxVolumesPerNode)
	hp.cs = NewControllerServer(hp.nodeID)

	discoverExistingSnapshots()
	s := NewNonBlockingGRPCServer()
	s.Start(hp.endpoint, hp.ids, hp.cs, hp.ns)
	s.Wait()
}

func getVolumeByID(volumeID string) (hostPathVolume, error) {
	if hostPathVol, ok := hostPathVolumes[volumeID]; ok {
		return hostPathVol, nil
	}
	return hostPathVolume{}, fmt.Errorf("volume id %s does not exist in the volumes list", volumeID)
}

func getVolumeByName(volName string) (hostPathVolume, error) {
	for _, hostPathVol := range hostPathVolumes {
		if hostPathVol.VolName == volName {
			return hostPathVol, nil
		}
	}
	return hostPathVolume{}, fmt.Errorf("volume name %s does not exist in the volumes list", volName)
}

func getSnapshotByName(name string) (hostPathSnapshot, error) {
	for _, snapshot := range hostPathVolumeSnapshots {
		if snapshot.Name == name {
			return snapshot, nil
		}
	}
	return hostPathSnapshot{}, fmt.Errorf("snapshot name %s does not exist in the snapshots list", name)
}

// getVolumePath returns the canonical path for hostpath volume
func getVolumePath(volID string) string {
	return filepath.Join(dataRoot, volID)
}

// createVolume create the directory for the hostpath volume.
// It returns the volume path or err if one occurs.
func createHostpathVolume(volID, name string, cap int64, volAccessType accessType, ephemeral bool) (*hostPathVolume, error) {
	path := getVolumePath(volID)

	switch volAccessType {
	case mountAccess:
		err := os.MkdirAll(path, 0777)
		if err != nil {
			return nil, err
		}
	case blockAccess:
		executor := utilexec.New()
		size := fmt.Sprintf("%dM", cap/mib)
		// Create a block file.

		out, err := executor.Command("fallocate", "-l", size, path).CombinedOutput()
		if err != nil {
			return nil, fmt.Errorf("failed to create block device: %v, %v", err, string(out))
		}

		// Associate block file with the loop device.
		volPathHandler := volumepathhandler.VolumePathHandler{}
		_, err = volPathHandler.AttachFileDevice(path)
		if err != nil {
			// Remove the block file because it'll no longer be used again.
			if err2 := os.Remove(path); err2 != nil {
				glog.Errorf("failed to cleanup block file %s: %v", path, err2)
			}
			return nil, fmt.Errorf("failed to attach device %v: %v", path, err)
		}
	default:
		return nil, fmt.Errorf("unsupported access type %v", volAccessType)
	}

	hostpathVol := hostPathVolume{
		VolID:         volID,
		VolName:       name,
		VolSize:       cap,
		VolAccessType: volAccessType,
	}
	hostPathVolumes[volID] = hostpathVol
	return &hostpathVol, nil
}

// updateVolume updates the existing hostpath volume.
func updateVolume(volID string, capacity int64) error {

	glog.V(4).Infof("updating hostpath volume: %s", volID)

	devicePath, vgname, poolname,_, err := util.GetDevicePathForVolumue(context.Background(), volID)

	sizeCmd := fmt.Sprintf("%s lvdisplay %s | grep 'LV Size' | awk '{print $3,$4}'", NsenterCmd, devicePath)
	sizeStr, err := util.Runcmd(sizeCmd)
	if err != nil {
		return err
	}

	if sizeStr == "" {
		return status.Error(codes.Internal, "Get lvm size error")
	}

	lsize := strings.Split(sizeStr, " ")

	if len(lsize) != 2 {
		return status.Error(codes.Internal, "Get  size error: not unit")
	}

	size := strings.TrimSpace(lsize[0])
	unit := strings.TrimSpace(lsize[1])

	sizef, err := strconv.ParseFloat(size, 64)
	if err != nil {
		return err
	}
	var bsize int64
	if unit == "GiB" {
		bsize = int64(sizef * 1024 * 1024 * 1024)
	} else if unit == "MiB" {
		bsize = int64(sizef * 1024 * 1024)
	} else {
		return status.Error(codes.Internal, "unit error for volume size")
	}

	// if lvmsize equal/bigger than pv size, no do expand.
	if bsize >= capacity {
		return nil
	}

	//newplus:=capacity-bsize

	exterdsize := fmt.Sprintf("%dM", capacity/mib)

	//log.Infof("NodeExpandVolume:: volumeId: %s, devicePath: %s, from size: %d, to Size: %d%s", volumeID, devicePath, sizeInt, pvSize, unit)

	// resize lvm volume
	// lvextend -L3G /dev/vgtest/lvm-5db74864-ea6b-11e9-a442-00163e07fb69
	resizeCmd := fmt.Sprintf("%s lvextend -L%s %s", NsenterCmd, exterdsize, devicePath)
	output, err := util.Runcmd(resizeCmd)
	if err != nil {
		return err
	}
	if strings.Contains(strings.ToLower(output),"insufficient"){
	return fmt.Errorf("not emough free storage for expand" )
	}

	if poolname == "" {
		util.UpdateNode(util.GetNodeName(), vgname, false, capacity-bsize)
	}
	return nil
}

// hostPathIsEmpty is a simple check to determine if the specified hostpath directory
// is empty or not.
func hostPathIsEmpty(p string) (bool, error) {
	f, err := os.Open(p)
	if err != nil {
		return true, fmt.Errorf("unable to open hostpath volume, error: %v", err)
	}
	defer f.Close()

	_, err = f.Readdir(1)
	if err == io.EOF {
		return true, nil
	}
	return false, err
}

func loadFromFilesystemVolume(hostPathVolume hostPathVolume, destPath string) error {
	/*	srcPath := hostPathVolume.VolPath
		isEmpty, err := hostPathIsEmpty(srcPath)
		if err != nil {
			return status.Errorf(codes.Internal, "failed verification check of source hostpath volume %v: %v", hostPathVolume.VolID, err)
		}

		// If the source hostpath volume is empty it's a noop and we just move along, otherwise the cp call will fail with a a file stat error DNE
		if !isEmpty {
			args := []string{"-a", srcPath + "/.", destPath + "/"}
			executor := utilexec.New()
			out, err := executor.Command("cp", args...).CombinedOutput()
			if err != nil {
				return status.Errorf(codes.Internal, "failed pre-populate data from volume %v: %v: %s", hostPathVolume.VolID, err, out)
			}
		}
	*/
	return nil
}

func loadFromBlockVolume(hostPathVolume hostPathVolume, destPath string) error {
	/*	srcPath := hostPathVolume.VolPath
		args := []string{"if=" + srcPath, "of=" + destPath}
		executor := utilexec.New()
		out, err := executor.Command("dd", args...).CombinedOutput()
		if err != nil {
			return status.Errorf(codes.Internal, "failed pre-populate data from volume %v: %v: %s", hostPathVolume.VolID, err, out)
		}
	*/
	return nil
}
