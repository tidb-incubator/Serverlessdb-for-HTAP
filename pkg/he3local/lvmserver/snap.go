package lvmserver

import (
	"fmt"
	"github.com/tidb-incubator/Serverlessdb-for-HTAP/pkg/he3local/util"
	"os"
	//"errors"
	"context"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"k8s.io/utils/mount"
	"strings"
)

const (
	// NsenterCmd is the nsenter command
	NsenterCmd = "nsenter --mount=/proc/1/ns/mnt"
)

func CreateSnap(ctx context.Context, snapshotID string, volumeID string, vgName string, size string, s3env string,fstype string) error {

	err := createSnapVolume(ctx, snapshotID, volumeID, vgName, size)

	if err != nil {
		fmt.Printf("create snapvoluem is %s \n", err)
		return err
	}

	transnapid := strings.ReplaceAll(snapshotID, "-", "--")

	snapdevicepath := fmt.Sprintf("/dev/mapper/%s-%s", vgName, transnapid)

	mountspappath := fmt.Sprintf("/csi-data-dir/%s-snap", snapshotID)

	if _, err := os.Stat(mountspappath); os.IsNotExist(err) {
		if err := os.MkdirAll(mountspappath, 0750); err != nil {
			return status.Error(codes.Internal, err.Error())
		}
	}
	fmt.Printf("mount vol %s,%s \n", snapdevicepath, mountspappath)
	err = util.MountVolforDeviceForSnap(ctx, snapdevicepath, mountspappath, fstype)

	if err != nil {
		fmt.Printf("mountVolforDevice is %s \n", err)
		mount.New("").Unmount(mountspappath)
		deletesnapvolume(snapdevicepath)
		//add code to delet s3 resource 
		return err
	}

	fmt.Printf("mirror to s3 %s,%s,%s \n", mountspappath, snapshotID, s3env)
	err = util.MirrorDevicetoS3(ctx, mountspappath, snapshotID, s3env)

	if err != nil {
		fmt.Printf("mirrorfiletoS3 is %s \n", err)
		//add code to delet s3 resource 
		mount.New("").Unmount(mountspappath)
		deletesnapvolume(snapdevicepath)
		return err
	}

	fmt.Printf("unmout %s \n", mountspappath)
	err = mount.New("").Unmount(mountspappath)
	if err != nil {
		fmt.Printf("err is %s \n", err)
		deletesnapvolume(snapdevicepath)
		return status.Error(codes.Internal, err.Error())
		//add code to delet s3 resource 
	}

	fmt.Printf("delete snap vilume %s \n", snapdevicepath)
	err = deletesnapvolume(snapdevicepath)

	if err != nil {
		return status.Error(codes.Internal, err.Error())
		//add code to delet s3 resource 
	}

	fmt.Printf("remove %s \n", mountspappath)
	// Delete the mount point.
	// Does not return error for non-existent path, repeated calls OK for idempotency.
	if err = os.RemoveAll(mountspappath); err != nil {
		return status.Error(codes.Internal, err.Error())
		//add code to delet s3 resource 
	}
	return nil
}
func deletesnapvolume(snapdevice string) error {

	cmd := fmt.Sprintf("%s lvremove  %s -y", NsenterCmd, snapdevice)
	_, err := util.Runcmd(cmd)
	if err != nil {
		return err
	}
	return nil
}

// create lvm volume
func createSnapVolume(ctx context.Context, snapID, volumeID, vgName, size string) error {

	var err error

	// check vg exist
	ckCmd := fmt.Sprintf("%s vgck %s", NsenterCmd, vgName)
	_, err = util.Runcmd(ckCmd)
	if err != nil {
		fmt.Printf("createVolume:: VG is not exist: %s \n", vgName)
		return err
	}

	cmd := fmt.Sprintf("%s lvcreate -L %s --snapshot --name %s %s/%s", NsenterCmd, size, snapID, vgName, volumeID)
	_, err = util.Runcmd(cmd)
	if err != nil {
		return err
	}

	fmt.Printf("Successful Create Linear LVM volume: %s, Size: %s, vgName: %s", volumeID, size, vgName)

	return nil
}
