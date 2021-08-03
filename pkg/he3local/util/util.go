package util

import (
	"context"
	"fmt"
	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"k8s.io/utils/mount"
	"os/exec"
	"strings"
	//"github.com/golang/glog"
	"errors"
	"os"
	"strconv"
)

const (
	// NsenterCmd is the nsenter command
	NsenterCmd = "nsenter --mount=/proc/1/ns/mnt"

	// LinearType linear type
	LinearType = "linear"
	// StripingType striping type
	StripingType = "striping"
)

var nodeid string

func SetNodeId(id string) {
	nodeid = id
}

func GetNodeName() string {
	return nodeid
}

// Run run shell command
func Runcmd(cmd string) (string, error) {
	fmt.Printf("cmd is %s \n", cmd)
	out, err := exec.Command("sh", "-c", cmd).CombinedOutput()
	if err != nil {
		return "", fmt.Errorf("Failed to run cmd: " + cmd + ", with out: " + string(out) + ", with error: " + err.Error())
	}
	return string(out), nil
}

func FsCheck(devicePath string) (string, error) {
	// We use `file -bsL` to determine whether any filesystem type is detected.
	// If a filesystem is detected (ie., the output is not "data", we use
	// `blkid` to determine what the filesystem is. We use `blkid` as `file`
	// has inconvenient output.
	// We do *not* use `lsblk` as that requires udev to be up-to-date which
	// is often not the case when a device is erased using `dd`.
	output, err := exec.Command("file", "-bsL", devicePath).CombinedOutput()
	if err != nil {
		return "", err
	}
	if strings.TrimSpace(string(output)) == "data" {
		return "", nil
	}
	output, err = exec.Command("blkid", "-c", "/dev/null", "-o", "export", devicePath).CombinedOutput()
	if err != nil {
		return "", err
	}

	lines := strings.Split(string(output), "\n")
	for _, line := range lines {
		fields := strings.Split(strings.TrimSpace(line), "=")
		if len(fields) != 2 {
			return "", fmt.Errorf("fscheck faild %s ", devicePath)
		}
		if fields[0] == "TYPE" {
			return fields[1], nil
		}
	}
	return "", fmt.Errorf("fscheck faild %s ", devicePath)
}

func FormatFSForDevice(devicePath, fstype string) error {
	output, err := exec.Command("mkfs", "-t", fstype, devicePath).CombinedOutput()
	if err != nil {
		return errors.New("formatFSForDevice error: " + string(output))
	}
	return nil
}

func MountVolforDevice(ctx context.Context, devicepath, targetPath string, fs string) error {

	if fs != "" {
		FormatFSForDevice(devicepath, fs)
	}

	var options []string
	options = append(options, "rw")


	err := mount.New("").Mount(devicepath, targetPath, fs, options)
	return err
}

func MountVolforDeviceForSnap(ctx context.Context, devicepath, targetPath string, fs string) error {

	var options []string
	options = append(options, "rw")

	if fs == "xfs" {
		options = append(options, "nouuid")
	}

	err := mount.New("").Mount(devicepath, targetPath, "", options)
	return err
}

func MirrorDevicetoS3(ctx context.Context, targetPath string, snapid string, s3env string) error {
	mirrircmd := fmt.Sprintf("%s mc mirror %s  minio/snapbucket/%s/", s3env, targetPath, snapid)
	_, err := Runcmd(mirrircmd)
	return err
}

func MirrorS3toDevice(ctx context.Context, targetPath string, snapid string, s3env string) error {
	mirrircmd := fmt.Sprintf("%s mc mirror minio/snapbucket/%s/  %s", s3env, snapid, targetPath)
	_, err := Runcmd(mirrircmd)
	return err
}

func DeleteSnap(ctx context.Context, snapid string) error {

	s3ens, err := GetS3ENSForSnapID(ctx, snapid)
	if err != nil {
		return err
	}

	mirrircmd := fmt.Sprintf("%s mc rm minio/snapbucket/%s  --recursive  --force", s3ens, snapid)
	_, err = Runcmd(mirrircmd)
	return err
}

func GetpvnumberForVG(vgName string) (int, error) {
	pvNum := 0
	// step1: check vg is created or not
	vgCmd := fmt.Sprintf("%s vgdisplay %s | grep 'VG Name' | grep %s | grep -v grep | wc -l", NsenterCmd, vgName, vgName)
	fmt.Println(vgCmd)
	vgline, err := Runcmd(vgCmd)
	fmt.Printf("output is %s \n", strings.TrimSpace(vgline))
	if err != nil {
		return 0, err
	}

	if strings.TrimSpace(vgline) == "1" {
		pvNumCmd := fmt.Sprintf("%s vgdisplay %s | grep 'Cur PV' | grep -v grep | awk '{print $3}'", NsenterCmd, vgName)
		fmt.Println(pvNumCmd)
		if pvNumStr, err := Runcmd(pvNumCmd); err != nil {
			return 0, err
		} else if pvNum, err = strconv.Atoi(strings.TrimSpace(pvNumStr)); err != nil {
			return 0, err
		}
		return pvNum, nil
	}
	return 0, fmt.Errorf("not pv for %s", vgName)
}

func CheckVolumeInNode(vgName string, volumeid string) bool {

	vgCmd := fmt.Sprintf("%s lvs | grep %s | grep %s | wc -l", NsenterCmd, volumeid, vgName)
	count, err := Runcmd(vgCmd)
	if err != nil {
		return false
	}

	if strings.TrimSpace(count) == "1" {
		return true
	} else {
		return false
	}

}

// create lvm volume
func CreateVolume(ctx context.Context, volumeID, vgName, lvmType string, poolname string) error {
	pvSize, unit := GetPvSize(ctx, volumeID)
	pvNumber := 0
	var err error

	if pvNumber, err = GetpvnumberForVG(vgName); err != nil {
		return err
	}

	// check vg exist
	ckCmd := fmt.Sprintf("%s vgck %s", NsenterCmd, vgName)
	_, err = Runcmd(ckCmd)
	if err != nil {
		log.Errorf("createVolume:: VG is not exist: %s", vgName)
		return err
	}
	var output string
	// Create lvm volume
	if lvmType == StripingType {
		cmd := fmt.Sprintf("%s lvcreate -i %d -n %s -L %d%s %s", NsenterCmd, pvNumber, volumeID, pvSize, unit, vgName)
		output, err = Runcmd(cmd)

		fmt.Printf("Successful Create Striping LVM volume: %s, Size: %s, vgName: %s, striped number: %d", volumeID, pvSize, unit, vgName, pvNumber)
	} else if lvmType == LinearType {

		if poolname == "" {
			cmd := fmt.Sprintf("%s lvcreate -n %s -L %d%s %s", NsenterCmd, volumeID, pvSize, unit, vgName)
			output, err = Runcmd(cmd)

		} else {

			cmd := fmt.Sprintf("%s lvcreate -V %d%s -T %s/%s -n %s", NsenterCmd, pvSize, unit, vgName, poolname, volumeID)
			output, err = Runcmd(cmd)

		}

	}

	if err != nil {
		return err
	}

	if strings.Contains(strings.ToLower(output), "insufficient") {
		return fmt.Errorf("not emough free storage for expand")
	}
	fmt.Printf("Successful Create Linear LVM volume: %s, Size: $d%s, vgName: %s", volumeID, pvSize, unit, vgName)

	var size int64
	if unit == "g" {
		size = pvSize * 1024 * 1024 * 1024
	} else if unit == "m" {
		size = pvSize * 1024 * 1024
	}

	if poolname == "" {
		UpdateNode(nodeid, vgName, false, size)
	}
	//func UpdateNode(nodename string, lablename string, add bool, updatesize int64) error {

	return nil
}

// loadFromSnapshot populates the given destPath with data from the snapshotID
func LoadFromSnapshot(ctx context.Context, snapshotId, devicePath, mountpath string, s3env string) error {

	if _, err := os.Stat(mountpath); os.IsNotExist(err) {
		if err := os.MkdirAll(mountpath, 0750); err != nil {
			return status.Error(codes.Internal, err.Error())
		}
	}

	err := MountVolforDevice(ctx, devicePath, mountpath, "ext4")

	if err != nil {
		fmt.Printf("mountVolforDevice is %s \n", err)
	}

	err = MirrorS3toDevice(ctx, mountpath, snapshotId, s3env)

	if err != nil {
		fmt.Printf("mirrorfiletoS3 is %s \n", err)
	}

	return nil
}

// deleteVolume deletes the directory for the hostpath volume.
func DeleteHostpathVolume(ctx context.Context, volID string) error {
	fmt.Printf("deleting hostpath volume: %s \n", volID)

	devicepath, vgname, poolname,_, err := GetDevicePathForVolumue(ctx, volID)

	if err != nil {
		fmt.Printf("dele voume %s \n", err)
		return err
	}

	if devicepath == "" {
		return nil
	} else {

		cmd := fmt.Sprintf("%s lvremove  %s -y", NsenterCmd, devicepath)
		_, err = Runcmd(cmd)
		if err != nil {
			return err
		}

	}
	//delete(hostPathVolumes, volID)

	pv := GetPV(ctx, volID)

	if pv == nil {
		return nil
	}

	pvQuantity := pv.Spec.Capacity["storage"]
	pvSize := pvQuantity.Value()
	if poolname == "" {
		UpdateNode(nodeid, vgname, true, pvSize)
	}
	return nil
}
