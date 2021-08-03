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
	"fmt"
	"k8s.io/utils/exec"
	"os"
	"strings"
	//"strconv"
	"github.com/golang/glog"
	"golang.org/x/net/context"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/tidb-incubator/Serverlessdb-for-HTAP/pkg/he3local/util"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	//"k8s.io/client-go/kubernetes"
	//"k8s.io/client-go/tools/clientcmd"
	"k8s.io/utils/mount"

	"k8s.io/kubernetes/pkg/util/resizefs"
	//k8smount "k8s.io/kubernetes/pkg/util/mount"
	//"k8s.io/kubernetes/pkg/volume/util/volumepathhandler"
)

const TopologyKeyNode = "topology.hostpath.csi/node"

type nodeServer struct {
	nodeID            string
	maxVolumesPerNode int64

	mounter mount.Interface
	//client  kubernetes.Interface
	//k8smounter k8smount.Interface
}

const (
	FsTypeTag = "fsType"
	DefaultFs = "ext4"
)

func NewNodeServer(nodeId string, maxVolumesPerNode int64) *nodeServer {
	return &nodeServer{
		nodeID:            nodeId,
		maxVolumesPerNode: maxVolumesPerNode,
		mounter:           mount.New(""),
		//k8smounter:        k8smount.New(""),
	}
}

func (ns *nodeServer) NodePublishVolume(ctx context.Context, req *csi.NodePublishVolumeRequest) (*csi.NodePublishVolumeResponse, error) {

	fmt.Println("begin publish volume")
	// Check arguments
	if req.GetVolumeCapability() == nil {
		return nil, status.Error(codes.InvalidArgument, "Volume capability missing in request")
	}
	if len(req.GetVolumeId()) == 0 {
		return nil, status.Error(codes.InvalidArgument, "Volume ID missing in request")
	}
	if len(req.GetTargetPath()) == 0 {
		return nil, status.Error(codes.InvalidArgument, "Target path missing in request")
	}

	devicepath, err := createVloumeinNode(ctx, req)

	if err != nil {
		return nil, status.Error(codes.InvalidArgument, "create volume err")
	}

	targetPath := req.GetTargetPath()

	notMnt, err := mount.IsNotMountPoint(ns.mounter, targetPath)
	if err != nil {
		if _, err := os.Stat(targetPath); os.IsNotExist(err) {
			if err := os.MkdirAll(targetPath, 0750); err != nil {
				return nil, status.Error(codes.Internal, err.Error())
			}
			notMnt = true
		} else {
			fmt.Println(err)
			return nil, status.Error(codes.Internal, err.Error())
		}
	}

	if !notMnt {
		return &csi.NodePublishVolumeResponse{}, nil
	}

	fsType := DefaultFs
	if _, ok := req.VolumeContext[FsTypeTag]; ok {
		fsType = req.VolumeContext[FsTypeTag]
	}

	/*devicepath, _,_, err := util.GetDevicePathForVolumue(ctx, req.GetVolumeId())

	if err != nil {

		return nil, status.Error(codes.InvalidArgument, err.Error())
	}*/

	fsexist, err := util.FsCheck(devicepath)
	if err != nil {
		fmt.Println(err)
		return nil, status.Errorf(codes.Internal, "fscheck err: %v", err)
	}
	if fsexist == "" {
		if err := util.FormatFSForDevice(devicepath, fsType); err != nil {
			return nil, status.Errorf(codes.Internal, "format fs failed: err=%v", err)
		}
	}

	if notMnt {

		var options []string
		if req.GetReadonly() {
			options = append(options, "ro")
		} else {
			options = append(options, "rw")
		}
		mountFlags := req.GetVolumeCapability().GetMount().GetMountFlags()
		options = append(options, mountFlags...)

		err = ns.mounter.Mount(devicepath, targetPath, fsType, options)
		if err != nil {
			fmt.Println(err)
			return nil, status.Error(codes.Internal, err.Error())
		}
		fmt.Printf("NodePublishVolume:: mount successful devicePath: %s, targetPath: %s, options: %v", devicepath, targetPath, options)
	}
	//try update address for pv until success
	err = util.SetNodeAddressForPV(ctx, ns.nodeID, req.GetVolumeId())
	if err != nil {
		fmt.Printf("err is %s \n", err)
	}
	return &csi.NodePublishVolumeResponse{}, nil
}

func createVloumeinNode(ctx context.Context, req *csi.NodePublishVolumeRequest) (string, error) {

	volumeID := req.GetVolumeId()
	parms := req.VolumeContext
	vgName := ""
	if _, ok := parms[VgNameTag]; ok {
		vgName = parms[VgNameTag]
	}
	if vgName == "" {
		return "", status.Error(codes.Internal, "error with input vgName is empty")
	}

	lvmType := LinearType
	if _, ok := parms[LvmTypeTag]; ok {
		lvmType = parms[LvmTypeTag]
	}

	var poolname string

	if _, ok := parms["thinpool"]; ok {
		poolname = parms["thinpool"]
	}

	//for example devicepath /dev/mapper/vg1-f7e751b2--bc0c--11ea--b594--384c4fcc6ec6

	volindevice := strings.ReplaceAll(volumeID, "-", "--")
	devicePath := fmt.Sprintf("/dev/mapper/%s-%s", vgName, volindevice)

	fmt.Printf("device Path is %s \n", devicePath)

	if _, err := os.Stat(devicePath); os.IsNotExist(err) {

		/*	err := util.UpdatePVForNodeAffinity(ctx, volumeID, vgName)
			if err != nil {
				return status.Error(codes.Internal, err.Error())
			}
		*/
		err = util.CreateVolume(ctx, volumeID, vgName, lvmType, poolname)
		if err != nil {
			return "", status.Error(codes.Internal, err.Error())
		}

		//************************************
		snap, enss3, err := util.GetVolContentsource(ctx, volumeID)

		if err != nil {
			return "", status.Error(codes.Internal, err.Error())
		}

		if snap != "" {
			mountspappath := fmt.Sprintf("/csi-data-dir/%s", snap)
			if _, err := os.Stat(mountspappath); os.IsNotExist(err) {
				if err := os.MkdirAll(mountspappath, 0750); err != nil {
					return "", status.Error(codes.Internal, err.Error())
				}
			}
			err = util.LoadFromSnapshot(ctx, snap, devicePath, mountspappath, enss3)

			if err != nil {
				return "", status.Error(codes.Internal, err.Error())
			}

			err = mount.New("").Unmount(mountspappath)
			if err != nil {
				return "", status.Error(codes.Internal, err.Error())
			}

			// Delete the mount point.
			// Does not return error for non-existent path, repeated calls OK for idempotency.
			if err = os.RemoveAll(mountspappath); err != nil {
				return "", status.Error(codes.Internal, err.Error())
			}

		}
	}
	return devicePath, nil
}

func (ns *nodeServer) NodeUnpublishVolume(ctx context.Context, req *csi.NodeUnpublishVolumeRequest) (*csi.NodeUnpublishVolumeResponse, error) {

	// Check arguments
	if len(req.GetVolumeId()) == 0 {
		return nil, status.Error(codes.InvalidArgument, "Volume ID missing in request")
	}
	if len(req.GetTargetPath()) == 0 {
		return nil, status.Error(codes.InvalidArgument, "Target path missing in request")
	}
	targetPath := req.GetTargetPath()
	//volumeID := req.GetVolumeId()

	// Unmount only if the target path is really a mount point.
	if notMnt, err := mount.IsNotMountPoint(mount.New(""), targetPath); err != nil {
		if !os.IsNotExist(err) {
			return nil, status.Error(codes.Internal, err.Error())
		}
	} else if !notMnt {
		// Unmounting the image or filesystem.
		err = mount.New("").Unmount(targetPath)
		if err != nil {
			return nil, status.Error(codes.Internal, err.Error())
		}
	}
	// Delete the mount point.
	// Does not return error for non-existent path, repeated calls OK for idempotency.
	if err := os.RemoveAll(targetPath); err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}

	glog.V(4).Infof("hostpath: volume %s has been unpublished.", targetPath)

	return &csi.NodeUnpublishVolumeResponse{}, nil
}

func (ns *nodeServer) NodeStageVolume(ctx context.Context, req *csi.NodeStageVolumeRequest) (*csi.NodeStageVolumeResponse, error) {

	// Check arguments
	if len(req.GetVolumeId()) == 0 {
		return nil, status.Error(codes.InvalidArgument, "Volume ID missing in request")
	}
	if len(req.GetStagingTargetPath()) == 0 {
		return nil, status.Error(codes.InvalidArgument, "Target path missing in request")
	}
	if req.GetVolumeCapability() == nil {
		return nil, status.Error(codes.InvalidArgument, "Volume Capability missing in request")
	}

	return &csi.NodeStageVolumeResponse{}, nil
}

func (ns *nodeServer) NodeUnstageVolume(ctx context.Context, req *csi.NodeUnstageVolumeRequest) (*csi.NodeUnstageVolumeResponse, error) {

	// Check arguments
	if len(req.GetVolumeId()) == 0 {
		return nil, status.Error(codes.InvalidArgument, "Volume ID missing in request")
	}
	if len(req.GetStagingTargetPath()) == 0 {
		return nil, status.Error(codes.InvalidArgument, "Target path missing in request")
	}

	return &csi.NodeUnstageVolumeResponse{}, nil
}

func (ns *nodeServer) NodeGetInfo(ctx context.Context, req *csi.NodeGetInfoRequest) (*csi.NodeGetInfoResponse, error) {

	topology := &csi.Topology{
		Segments: map[string]string{TopologyKeyNode: ns.nodeID},
	}

	return &csi.NodeGetInfoResponse{
		NodeId:             ns.nodeID,
		MaxVolumesPerNode:  ns.maxVolumesPerNode,
		AccessibleTopology: topology,
	}, nil
}

func (ns *nodeServer) NodeGetCapabilities(ctx context.Context, req *csi.NodeGetCapabilitiesRequest) (*csi.NodeGetCapabilitiesResponse, error) {

	return &csi.NodeGetCapabilitiesResponse{
		Capabilities: []*csi.NodeServiceCapability{
			{
				Type: &csi.NodeServiceCapability_Rpc{
					Rpc: &csi.NodeServiceCapability_RPC{
						Type: csi.NodeServiceCapability_RPC_STAGE_UNSTAGE_VOLUME,
					},
				},
			},
			{
				Type: &csi.NodeServiceCapability_Rpc{
					Rpc: &csi.NodeServiceCapability_RPC{
						Type: csi.NodeServiceCapability_RPC_EXPAND_VOLUME,
					},
				},
			},
		},
	}, nil
}

func (ns *nodeServer) NodeGetVolumeStats(ctx context.Context, in *csi.NodeGetVolumeStatsRequest) (*csi.NodeGetVolumeStatsResponse, error) {
	return nil, status.Error(codes.Unimplemented, "")
}

// NodeExpandVolume is only implemented so the driver can be used for e2e testing.
func (ns *nodeServer) NodeExpandVolume(ctx context.Context, req *csi.NodeExpandVolumeRequest) (*csi.NodeExpandVolumeResponse, error) {

	volID := req.GetVolumeId()
	if len(volID) == 0 {
		return nil, status.Error(codes.InvalidArgument, "Volume ID not provided")
	}

	volPath := req.GetVolumePath()

	devicePath, _, _, fstype, err := util.GetDevicePathForVolumue(ctx, volID)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())

	}

	if len(volPath) == 0 {
		return nil, status.Error(codes.InvalidArgument, "Volume path not provided")
	}

	_, err = os.Stat(volPath)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "Could not get file information from %s: %v", volPath, err)
	}

	err = updateVolume(volID, req.GetCapacityRange().RequiredBytes)

	if fstype == "ext4" {
		if err := ns.resizeVolume(ctx, volID, volPath, devicePath); err != nil {
			return nil, status.Error(codes.Internal, err.Error())
		}
	} else if fstype == "xfs" {
		eterCmd := fmt.Sprintf("%s xfs_growfs %s", NsenterCmd, devicePath)
		_, err := util.Runcmd(eterCmd)
		if err != nil {
			return nil, status.Error(codes.Internal, err.Error())
		}
	}
	return &csi.NodeExpandVolumeResponse{
		CapacityBytes: req.CapacityRange.RequiredBytes,
	}, nil
}

func (ns *nodeServer) resizeVolume(ctx context.Context, volumeID, targetPath string, devicePath string) error {

	//expand lvm volume

	// use resizer to expand volume filesystem
	//realExec := mount.NewOsExec()
	realExec := exec.New()
	resizer := resizefs.NewResizeFs(&mount.SafeFormatAndMount{Interface: ns.mounter, Exec: realExec})
	ok, err := resizer.Resize(devicePath, targetPath)
	if err != nil {
		fmt.Printf("NodeExpandVolume:: Resize Error, volumeId: %s, devicePath: %s, volumePath: %s, err: %s", volumeID, devicePath, targetPath, err.Error())
		return err
	}
	if !ok {
		fmt.Printf("NodeExpandVolume:: Resize failed, volumeId: %s, devicePath: %s, volumePath: %s", volumeID, devicePath, targetPath)
		return status.Error(codes.Internal, "Fail to resize volume fs")
	}
	fmt.Printf("NodeExpandVolume:: resizefs successful volumeId: %s, devicePath: %s, volumePath: %s", volumeID, devicePath, targetPath)
	return nil
}
