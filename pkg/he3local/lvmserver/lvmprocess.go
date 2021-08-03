package lvmserver

import (
	"context"
	pb "github.com/tidb-incubator/Serverlessdb-for-HTAP/pkg/he3local/lvmpb"
	"github.com/tidb-incubator/Serverlessdb-for-HTAP/pkg/he3local/util"
)

type SnapSerice struct {
}

func (s *SnapSerice) SnapshotForVolume(ctx context.Context, in *pb.SnapshotForVolumeRequest) (*pb.SnapshotForVolumeReply, error) {

	res := &pb.SnapshotForVolumeReply{
		Result: &pb.Result{},
		Error:  &pb.ErrorReason{},
	}

	err := CreateSnap(ctx, in.SnapshotID, in.VolumeID, in.VgName, in.Size, in.S3Env,in.Fstype)
	if err == nil {
		return res, nil
	} else {
		return nil, err
	}
}



type LVMSerice struct {
}

func (s *LVMSerice) DeleteVolume(ctx context.Context, in *pb.DeleteVolumeRequest) (*pb.DeleteVolumeVolumeReply, error) {

	res := &pb.DeleteVolumeVolumeReply{
		Result: &pb.Result{},
		Error:  &pb.ErrorReason{},
	}

	err := util.DeleteHostpathVolume(ctx, in.VolumeID)
	if err == nil {
		return res, nil
	} else {
		return nil, err
	}
}
