package web

import (
	"fmt"
	"io/ioutil"
	"k8s.io/klog"
	"net/http"
	"strings"
	"time"
)

type HttpApiClient interface {
	PostAddTidb(url string, name string, namesp string) error
	DeleteTidb(url string, name string, namesp string, instances []string, weight string) ([]string, error)
	GetAllTidb(url string) (string, error)
	DeleteErrorTidb(url string, name string, namesp string, addr string) error
}

type AutoScalerClientApi struct{}

func tryPostAddTidb(url string, name string, namesp string) error {
	contentType := "application/json"
	bobyPattern := `{"cluster":"%s","namespace":"%s"}`
	boby := fmt.Sprintf(bobyPattern, name, namesp)
	postTidb := strings.NewReader(boby)
	resp, err := http.Post(url, contentType, postTidb)
	if resp != nil && resp.StatusCode == http.StatusOK {
		klog.Infof("[%s/%s] PostAddTidb url %s boby %s success", namesp, name, url, boby)
		return nil
	}
	var code int
	if resp != nil {
		code = resp.StatusCode
	}
	klog.Infof("[%s/%s] PostAddTidb url %s boby %s failed resp.StatusCode %v", namesp, name, url, boby, code)
	return fmt.Errorf("[%s/%s] PostAddTidb url %s boby %s failed %v", namesp, name, url, boby, err)
}

func (*AutoScalerClientApi) GetAllTidb(url string) (string, error) {
	resp, err := http.Get(url)
	if err != nil {
		return "", err
	}
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("midware is down")
	}
	bytes, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}
	return string(bytes), nil
}

func (*AutoScalerClientApi) PostAddTidb(url string, name string, namesp string) error {
	count := 0
	for {
		err := tryPostAddTidb(url, name, namesp)
		if err == nil {
			break
		}
		if count > 30 {
			return err
		}
		count++
		time.Sleep(time.Second)
	}
	return nil
}

func (*AutoScalerClientApi) DeleteTidb(url string, name string, namesp string, instances []string, weight string) ([]string, error) {
	bobyPattern := `{"cluster":"%s","addr":"%s"}`
	var successInstances []string
	for _, instance := range instances {
		addr := instance + "." + name + "-tidb-peer." + namesp + ".svc:4000@" + weight
		boby := fmt.Sprintf(bobyPattern, name, addr)
		deleteTidb := strings.NewReader(boby)
		req, _ := http.NewRequest("DELETE", url, deleteTidb)
		req.Header.Add("Content-Type", "application/json")
		_, err := http.DefaultClient.Do(req)
		if err != nil {
			klog.Infof("[%s/%s] DeleteTidb instance %s failed", name, namesp, instance)
		}
		successInstances = append(successInstances, instance)
	}
	if len(successInstances) != len(instances) {
		return successInstances, fmt.Errorf("delete some tidb failed")
	}
	return successInstances, nil
}

func (*AutoScalerClientApi) DeleteErrorTidb(url string, name string, namesp string, addr string) error {
	bobyPattern := `{"cluster":"%s","addr":"%s"}`
	boby := fmt.Sprintf(bobyPattern, name, addr)
	deleteTidb := strings.NewReader(boby)
	req, _ := http.NewRequest("DELETE", url, deleteTidb)
	req.Header.Add("Content-Type", "application/json")
	_, err := http.DefaultClient.Do(req)
	if err != nil {
		klog.Infof("[%s/%s] DeleteTidb addr %s failed", name, namesp, addr)
		return err
	}
	return nil
}

func NewAutoScalerClientApi() HttpApiClient {
	return &AutoScalerClientApi{}
}
