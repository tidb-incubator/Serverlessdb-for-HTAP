package server

import (
	"fmt"
	"github.com/pingcap/tidb/proxy/backend"
	"github.com/pingcap/tidb/proxy/config"
	"github.com/pingcap/tidb/proxy/core/golog"
	"math"
	"time"
)

type Serverless struct {
	multiScales map[string]*Scale

	//for servereless
	proxy          *Server
	serverlessaddr string
	counter        *Counter

	//for 0 core
	silentPeriod int
}

type Scale struct {
	//for scale out
	lastSend          int64
	lastchange        float64
	resendForScaleOut time.Duration

	//for scale in
	//allscaleinum    []float64
	scalueincout    int
	minscalinnum    float64
	scaleInInterval int
}

func (sl *Serverless) RestServerless(tidbType string) {
	sl.multiScales[tidbType].lastSend=0
	sl.multiScales[tidbType].lastchange=0
	sl.multiScales[tidbType].resetscalein()
}

var CostOneCore float64 = 1000

func NewServerless(cfg *config.Config, srv *Server, count *Counter) (*Serverless, error) {
	s := new(Serverless)
	//s.lastSend = time.Now().Unix()
	s.proxy = srv
	s.counter = count
	s.multiScales = make(map[string]*Scale)
	s.multiScales[backend.TiDBForTP] = &Scale{}
	s.multiScales[backend.TiDBForAP] = &Scale{}

	//s.allscaleinum = make([]float64, 12)
	if cfg.Cluster.ScaleInInterval != 0 {
		s.multiScales[backend.TiDBForTP].scaleInInterval = cfg.Cluster.ScaleInInterval
		s.multiScales[backend.TiDBForAP].scaleInInterval = cfg.Cluster.ScaleInInterval
	} else {
		s.multiScales[backend.TiDBForTP].scaleInInterval = 5
		s.multiScales[backend.TiDBForAP].scaleInInterval = 5
	}

	s.silentPeriod = cfg.Cluster.SilentPeriod
	s.serverlessaddr = cfg.Cluster.ServerlessAddr

	s.multiScales[backend.TiDBForTP].resendForScaleOut = time.Duration(cfg.Cluster.ResendForScaleOUT) * time.Second
	s.multiScales[backend.TiDBForAP].resendForScaleOut = time.Duration(cfg.Cluster.ResendForScaleOUT) * time.Second

	golog.Info("serverless", "NewServerless", "Serverless Server running", 0,
		"address",
		s.serverlessaddr)
	return s, nil
}

func (sl *Serverless) CheckServerless() {
	for tidbtype, pool := range sl.proxy.cluster.BackendPools {
		needcore := sl.multiScales[tidbtype].GetNeedCores(pool.Costs)
		currentcore := sl.GetCurrentCores(tidbtype)
		if needcore == currentcore {
			return
		}
		if needcore > currentcore {
			sl.multiScales[tidbtype].scaleout(currentcore, needcore)
		} else {
			sl.scalein(currentcore, needcore, tidbtype)
		}
	}

}

func (sl *Scale) GetlastSend() int64 {
	return sl.lastSend
}

func (sl *Scale) SetLastChange(diff float64) {
	sl.lastSend = time.Now().Unix()
	sl.lastchange = diff
}

func (sl *Scale) SetScalein(diffcores float64) {
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

func (sl *Scale) resetscalein() {
	//sl.allscaleinum = make([]float64, 12)
	sl.scalueincout = 0
	sl.minscalinnum = 0

}

func (sl *Serverless) scalein(currentcore, needcore float64, tidbType string) {
	if sl.silentPeriod > 0 {
		if needcore == 0 && sl.counter.QuiescentTotalTime > int64(sl.silentPeriod)*60 {
			fmt.Printf("quiescent time %d > 30s post serverless scale down to 0 \n", sl.counter.QuiescentTotalTime)
			return
		}
	}
	sl.multiScales[tidbType].SetScalein(currentcore - needcore)
}

func (sl *Scale) scaleout(currentcore, needcore float64) {
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

func (sl *Serverless) GetCurrentCores(tidbType string) float64 {
	tws := sl.proxy.cluster.BackendPools[tidbType].TidbsWeights
	var currentcores float64
	for _, tw := range tws {
		currentcores = currentcores + float64(tw)
	}
	return currentcores
}

func (sl *Scale) GetNeedCores(costs int64) float64 {



	if costs > int64(CostOneCore) {
		return math.Ceil(float64(costs) / float64(CostOneCore))
	}

	if costs > int64(CostOneCore/2) && costs <= int64(CostOneCore) {
		return 1
	} else if costs > int64(CostOneCore/4) && costs <= int64(CostOneCore/2) {
		return 0.5
	} else if costs > 0 && costs <= int64(CostOneCore/4) {
		return 0.25
	} else {
		return 0
	}

}
