package util

import (
	"fmt"
	"k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"regexp"
	"strconv"
	"strings"
)

func GetFreeForLVM(vgName string) (string, error) {
	//vgdisplay lvmvg1 | grep 'Free'  | awk '{print $5}'
	//vgdisplay lvmvg1 | grep 'PE Size' |  awk '{print $3}'

	// step1: get pe size
	pesizecmd := fmt.Sprintf("%s vgdisplay %s | grep 'PE Size' |  awk '{print $3}'", NsenterCmd, vgName)
	fmt.Println(pesizecmd)
	pvsizestr, err := Runcmd(pesizecmd)
	fmt.Printf("output is %s \n", strings.TrimSpace(pvsizestr))
	if err != nil {
		return "", err
	}
	ps, err := strconv.ParseFloat(strings.TrimSpace(pvsizestr), 64)
	if err != nil {
		return "", err
	}

	pesize := int64(ps)
	penumcmd := fmt.Sprintf("%s vgdisplay %s | grep 'Free'  | awk '{print $5}'", NsenterCmd, vgName)
	fmt.Println(penumcmd)
	penumstr, err := Runcmd(penumcmd)
	if err != nil {
		return "", err
	}
	penum, err := strconv.ParseInt(strings.TrimSpace(penumstr), 10, 64)
	if err != nil {
		return "", err
	}
	freesize := penum * pesize
	var res string
	if freesize > 1024 {
		res = fmt.Sprintf("%dGi", freesize/(1024))
	} else {
		res = fmt.Sprintf("%dMi", penum*pesize)
	}

	return res, nil
}

//**********

func GetTotalForLVM(vgName string) (string, error) {
	//vgdisplay lvmvg1 | grep 'Free'  | awk '{print $5}'
	//vgdisplay lvmvg1 | grep 'PE Size' |  awk '{print $3}'

	// step1: get pe size
	pesizecmd := fmt.Sprintf("%s vgdisplay %s | grep 'PE Size' |  awk '{print $3}'", NsenterCmd, vgName)
	fmt.Println(pesizecmd)
	pvsizestr, err := Runcmd(pesizecmd)
	fmt.Printf("output is %s \n", strings.TrimSpace(pvsizestr))
	if err != nil {
		return "", err
	}
	ps, err := strconv.ParseFloat(strings.TrimSpace(pvsizestr), 64)
	if err != nil {
		return "", err
	}

	pesize := int64(ps)
	
	


	//****************
	totalnumcmd := fmt.Sprintf("%s vgdisplay %s | grep 'Total'  | awk '{print $3}'", NsenterCmd, vgName)
	fmt.Println(totalnumcmd)
	totalnumstr, err := Runcmd(totalnumcmd)
	if err != nil {
		return "", err
	}
	totalnum, err := strconv.ParseInt(strings.TrimSpace(totalnumstr), 10, 64)
	if err != nil {
		return "", err
	}

	//********************
	totalsize := totalnum * pesize
	var res string
	if totalsize > 1024 {
		res = fmt.Sprintf("%dGi", totalsize/(1024))
	} else {
		res = fmt.Sprintf("%dMi", totalnum*pesize)
	}

	return res, nil
}

//************************
/*func UpdateNodeLable(nodename string, lablename string, lablevalue string) error {

	nodeobj, err := kubeClient.CoreV1().Nodes().Get(nodename, metav1.GetOptions{})
	if err != nil {
		return err
	}

	nodeobj.Labels[lablename] = lablevalue

	_, err = kubeClient.CoreV1().Nodes().Update(nodeobj)
	if err != nil {
		fmt.Printf("UpdateNode: fail to update node lable, err: %v", err)
		return err
	}

	return nil
}*/

func UpdateNodeLab(nodeobj *v1.Node, lablename string, lablevalue string) bool {
	if nodeobj.Labels == nil {
		nodeobj.Labels = make(map[string]string, 0)
	}
	if nodeobj.Labels[lablename] == lablevalue {
		return false
	} else {
		nodeobj.Labels[lablename] = lablevalue
		return true
	}
}

func GetAllLvms() []string {
	vgscmd := fmt.Sprintf("%s vgs", NsenterCmd)
	fmt.Println(vgscmd)
	vgsstr, err := Runcmd(vgscmd)
	if err != nil {
		return nil
	}
	vgs := make([]string, 0)
	allvgs := strings.Split(vgsstr, "\n")
	fmt.Printf("lens %d \n", len(allvgs))
	for _, item := range allvgs {
		if strings.Contains(strings.TrimSpace(item), "lvm") {
			fmt.Println(item)
			tem := strings.Split(strings.TrimSpace(item), " ")
			vgs = append(vgs, tem[0])
		}

	}
	return vgs
}

func Updatenodeslabs() error {
	vgs := GetAllLvms()
	/*	if len(vgs) == 0 {
		return fmt.Errorf("no lvm vg in node")
	}*/
	nodeobj, err := kubeClient.CoreV1().Nodes().Get(nodeid, metav1.GetOptions{})
	if err != nil {
		return err
	}
	temlab := make(map[string]string)

	for _, vg := range vgs {
		temlab[vg] = ""
	}
	var changelab bool
	//delete lvm in node lable but not exist in host
	for labkey, _ := range nodeobj.Labels {
		if len(labkey) >= 3 && labkey[0:3] == "lvm" {
			vgname := strings.Split(labkey, "/")[0]
			if _, ok := temlab[vgname]; !ok {
				fmt.Printf("delete not exit vgname %s \n", labkey)
				delete(nodeobj.Labels, labkey)
				changelab = true
			}
		}
	}

	for _, vg := range vgs {
		labvalue, err := GetFreeForLVM(vg)
		if err != nil {
			fmt.Println(err)
			continue
		}

		if UpdateNodeLab(nodeobj, vg, labvalue) {
			changelab = true
		}

		//*************
		totalvg := fmt.Sprintf("total/%s", vg)
		labtotalvalue, err := GetTotalForLVM(vg)
		if err != nil {
			fmt.Println(err)
			continue
		}

		if UpdateNodeLab(nodeobj, totalvg, labtotalvalue) {
			changelab = true
		}

		//****************

	}

	if changelab {
		_, err = kubeClient.CoreV1().Nodes().Update(nodeobj)
		if err != nil {
			fmt.Printf("UpdateNode: fail to update node lable, err: %v", err)
			return err
		}
		fmt.Printf("lable has been changed in nodid %s \n", nodeid)
	} else {
		fmt.Printf("not lable been changed in nodid %s \n", nodeid)
	}
	return nil

}

func UpdateThinForNode() error {
	thinmap := GetAllLThinsVGS()

	nodeobj, err := kubeClient.CoreV1().Nodes().Get(nodeid, metav1.GetOptions{})
	if err != nil {
		return err
	}

	var changelab bool
	//delete lvm in node lable but not exist in host
	for labkey, _ := range nodeobj.Labels {
		if len(labkey) >= 3 && labkey[0:3] == "lvm" && strings.Contains(labkey, "/") {
			if _, ok := thinmap[labkey]; !ok {
				fmt.Printf("delete not exit thing vgname %s \n", labkey)
				delete(nodeobj.Labels, labkey)
				changelab = true
			}
		}
	}

	for thinkey, thinvalue := range thinmap {
		if UpdateNodeLab(nodeobj, thinkey, thinvalue) {
			changelab = true
		}

	}

	if changelab {
		_, err = kubeClient.CoreV1().Nodes().Update(nodeobj)
		if err != nil {
			fmt.Printf("UpdateNode: fail to update node lable, err: %v", err)
			return err
		}
		fmt.Printf("thin lable has been changed in nodid %s \n", nodeid)
	} else {
		fmt.Printf("thin not lable been changed in nodid %s \n", nodeid)
	}
	return nil

}

//***********************

func GetAllLThinsVGS() map[string]string {
	lvscmd := fmt.Sprintf("%s lvs", NsenterCmd)
	fmt.Println(lvscmd)
	alllvsstr, err := Runcmd(lvscmd)
	if err != nil {
		return nil
	}
	thispools := make(map[string]string, 0)
	alllvs := strings.Split(alllvsstr, "\n")

	for _, item := range alllvs {
		//lv := strings.Split(strings.TrimSpace(item), " ")

		lv := regexp.MustCompile(" +").Split(strings.TrimSpace(item), -1)

		if len(lv) >= 2 && strings.Contains(lv[2], "twi") {
			pool := fmt.Sprintf("%s/%s", lv[1], lv[0])
			total := fmt.Sprintf("%s/%s-total", lv[1], lv[0])
			totalvalue := GetSizeForPool(lv[3])

			pol, err := strconv.ParseFloat(strings.TrimSpace(lv[4]), 64)
			if err != nil {
				fmt.Println(err)
				continue
			}

			poolstorage := int64((100 - pol) * float64(totalvalue) / 100)

			thispools[pool] = fmt.Sprintf("%dGi", poolstorage)
			thispools[total] = fmt.Sprintf("%dGi", totalvalue)
		}
	}
	return thispools
}

func GetSizeForPool(size string) int64 {
	//res int64
	//unit is mb
	var pesize int64
	if strings.Contains(size, "t") {
		ps, err := strconv.ParseFloat(strings.TrimSuffix(size, "t"), 64)
		if err != nil {
			return 0
		}
		pesize = int64(ps * 1024)

	} else if strings.Contains(size, "g") {
		ps, err := strconv.ParseFloat(strings.TrimSuffix(size, "g"), 64)
		if err != nil {
			return 0
		}
		pesize = int64(ps)
	} else {
		pesize = 0
	}
	return pesize
}
