package server

import (
	"fmt"
	"github.com/pingcap/tidb/proxy/config"
	"github.com/pingcap/tidb/proxy/core/golog"
	"math"
	"time"
)

type Serverless struct {
	//for scale out
	lastSend          int64
	lastchange        float64
	resendForScaleOut time.Duration

	//for servereless
	proxy          *Server
	serverlessaddr string
	counter        *Counter

	//for scale in
	//allscaleinum    []float64
	scalueincout    int
	minscalinnum    float64
	scaleInInterval int

	//for 0 core
	silentPeriod int
}

func (sl *Serverless) RestServerless() {
	sl.lastSend=0
	sl.lastchange=0
	sl.resetscalein()
}

var tpsOneCore float64 = 1000

func NewServerless(cfg *config.Config, srv *Server, count *Counter) (*Serverless, error) {
	s := new(Serverless)
	//s.lastSend = time.Now().Unix()
	s.proxy = srv
	s.counter = count

	//s.allscaleinum = make([]float64, 12)
	if cfg.Cluster.ScaleInInterval != 0 {
		s.scaleInInterval = cfg.Cluster.ScaleInInterval
	} else {
		s.scaleInInterval = 5
	}

	s.silentPeriod = cfg.Cluster.SilentPeriod
	s.serverlessaddr = cfg.Cluster.ServerlessAddr

	s.resendForScaleOut = time.Duration(cfg.Cluster.ResendForScaleOUT) * time.Second

	golog.Info("serverless", "NewServerless", "Serverless Server running", 0,
		"address",
		s.serverlessaddr)
	return s, nil
}

func (sl *Serverless) CheckServerless() {
	needcore := sl.GetNeedCores()
	currentcore := sl.GetCurrentCores()
	if needcore == currentcore {
		return
	}
	if needcore > currentcore {
		sl.scaleout(currentcore, needcore)
	} else {
		sl.scalein(currentcore, needcore)
	}
}

func (sl *Serverless) GetlastSend() int64 {
	return sl.lastSend
}

func (sl *Serverless) SetLastChange(diff float64) {
	sl.lastSend = time.Now().Unix()
	sl.lastchange = diff
}

func (sl *Serverless) SetScalein(diffcores float64) {
	sl.scalueincout++

	if diffcores < sl.minscalinnum {
		sl.minscalinnum = diffcores
	}

	if sl.scalueincout==sl.scaleInInterval*60{
		fmt.Printf("send scale in ")
		sl.resetscalein()
	}

	/*if sl.scalueincout == 60 {
		sl.allscaleinum = append(sl.allscaleinum, sl.minscalinnum)
		sl.scalueincout = 0
		sl.minscalinnum = 0
	} else {
		return
	}*/

/*
	if len(sl.allscaleinum) == sl.scaleInInterval {
		fmt.Printf("send scale in ")
		sl.resetscalein()
	}
*/
}

func (sl *Serverless) resetscalein() {
	//sl.allscaleinum = make([]float64, 12)
	sl.scalueincout = 0
	sl.minscalinnum = 0

}

func (sl *Serverless) scalein(currentcore, needcore float64) {
	if sl.silentPeriod > 0 {
		if needcore == 0 && sl.counter.QuiescentTotalTime > int64(sl.silentPeriod)*60 {
			fmt.Printf("quiescent time %d > 30s post serverless scale down to 0 \n", sl.counter.QuiescentTotalTime)
			return
		}
	}
	sl.SetScalein(currentcore - needcore)
}

func (sl *Serverless) scaleout(currentcore, needcore float64) {
	sl.resetscalein()

	difference := needcore - currentcore

	if difference == sl.lastchange {
		if time.Now().Unix()-sl.GetlastSend() > int64(sl.resendForScaleOut) {
			fmt.Printf("scal out current %d,needcore is %d \n", currentcore, needcore)
			sl.SetLastChange(difference)
		}
	} else {
		fmt.Printf("scal out current %d,needcore is %d \n", currentcore, needcore)
		sl.SetLastChange(difference)
	}

}

func (sl *Serverless) GetCurrentCores() float64 {
	tws := sl.proxy.cluster.TidbsWeights
	var currentcores float64
	for _, tw := range tws {
		currentcores = currentcores + float64(tw)
	}
	return currentcores
}

func (sl *Serverless) GetNeedCores() float64 {

	temp := sl.counter.OldClientQPS

	if temp > int64(tpsOneCore) {
		return math.Ceil(float64(sl.counter.OldClientQPS) / float64(tpsOneCore))
	}

	if temp > int64(tpsOneCore/2) && temp <= int64(tpsOneCore) {
		return 1
	} else if temp > int64(tpsOneCore/4) && temp <= int64(tpsOneCore/2) {
		return 0.5
	} else if temp > 0 && temp <= int64(tpsOneCore/4) {
		return 0.25
	} else {
		return 0
	}

}
