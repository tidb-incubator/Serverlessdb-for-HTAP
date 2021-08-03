package lvmclient

import (
	"fmt"
	pb "github.com/tidb-incubator/Serverlessdb-for-HTAP/pkg/he3local/lvmpb"
	"github.com/tidb-incubator/Serverlessdb-for-HTAP/pkg/he3local/util"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"log"
	"time"
)

func StrartSnap(ctx context.Context, volumeID string, vgname string, size string, snapshotID string, s3env string, fstype string, timeout string) error {

	address, err := util.GetNodeAddress(ctx, volumeID)
	if err != nil || address == "" {
		return fmt.Errorf("address is empty")
	}

	conn, err := grpc.Dial(address+":50001", grpc.WithInsecure())

	if err != nil {
		fmt.Printf("did not connect: %v", err)
		return err
	}

	defer conn.Close()

	c := pb.NewSnapshotClient(conn)

	reques := pb.SnapshotForVolumeRequest{VgName: vgname,
		SnapshotID: snapshotID,
		S3Env:      s3env,
		Size:       size,
		VolumeID:   volumeID,
		Fstype:     fstype,
	}

	fmt.Printf("time out for snap is %s \n", timeout)

	dur, err := time.ParseDuration(timeout)
	if err != nil {
		return fmt.Errorf("timeout for snap is %s", err)
	}
	ctxtime, cancelFn := context.WithTimeout(context.Background(), dur)

	r, err := c.SnapshotForVolume(ctxtime, &reques)
	if err != nil {
		fmt.Printf("could not full backup: %v", err)
		return err
	}
	cancelFn()
	log.Println(r)
	return nil

}
