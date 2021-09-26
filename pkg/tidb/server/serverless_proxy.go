package server

import (
	"context"
	"fmt"
	"github.com/pingcap/tidb/proxy/backend"
	"github.com/pingcap/tidb/proxy/config"
	"github.com/pingcap/tidb/proxy/core/golog"
	"github.com/pingcap/tidb/proxy/scalepb"
	"google.golang.org/grpc"
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

const (
	CostOneTpCore float64 = 100000
	CostOneApCore float64 = 8000000000
)
var ScalerClient scalepb.ScaleClient
var ClusterName string
var NameSpace string

func GprcClientToCluster() error {
	serviceName := "scale-operator.sldb-admin.svc:8028"
	conn, err := grpc.Dial(serviceName, grpc.WithInsecure())
	if err != nil {
		golog.Fatal("serverless","GprcClientToCluster","gprc to scaler failed",0,"address",serviceName)
		return err
	}
	ScalerClient = scalepb.NewScaleClient(conn)
	return nil
}

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

	ClusterName = cfg.Cluster.ClusterName
	NameSpace = cfg.Cluster.NameSpace

	s.silentPeriod = cfg.Cluster.SilentPeriod
	s.serverlessaddr = cfg.Cluster.ServerlessAddr

	s.multiScales[backend.TiDBForTP].resendForScaleOut = time.Duration(cfg.Cluster.ResendForScaleOUT) * time.Second
	s.multiScales[backend.TiDBForAP].resendForScaleOut = time.Duration(cfg.Cluster.ResendForScaleOUT) * time.Second

	golog.Info("serverless", "NewServerless", "Serverless Server running", 0,
		"address",
		s.serverlessaddr)

	GprcClientToCluster()

	return s, nil
}

func (sl *Serverless) CheckServerless() {
	for tidbtype, pool := range sl.proxy.cluster.BackendPools {
		needcore := sl.multiScales[tidbtype].GetNeedCores(pool.Costs, tidbtype)
		currentcore := sl.GetCurrentCores(tidbtype)
		if needcore == currentcore {
			continue
		}
		if needcore > currentcore {
			sl.multiScales[tidbtype].scaleout(currentcore, needcore, tidbtype)
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

func (sl *Scale) SetScalein(diffcores, needcore float64, tidbtype string) {
	sl.scalueincout++

	if diffcores < sl.minscalinnum {
		sl.minscalinnum = diffcores
	}

	if sl.scalueincout==sl.scaleInInterval*60{
		fmt.Printf("send scale in ")
		req2 := &scalepb.AutoScaleRequest{
			Clustername: ClusterName,
			Namespace: NameSpace,
			Curtime: time.Now().Unix(),
			Hashrate: float32(needcore),
			Autoscaler: 2,
			Scaletype: tidbtype,
		}
		ScalerClient.AutoScalerCluster(context.Background(),req2)
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
	sl.multiScales[tidbType].SetScalein(currentcore - needcore, needcore, tidbType)
}

func (sl *Scale) scaleout(currentcore, needcore float64, tidbtype string) {
	sl.resetscalein()

	difference := needcore - currentcore
	req := &scalepb.AutoScaleRequest{
		Clustername: ClusterName,
		Namespace: NameSpace,
		Curtime: time.Now().Unix(),
		Hashrate: float32(needcore),
		Autoscaler: 1,
		Scaletype: tidbtype,
	}

	if (difference == sl.lastchange && time.Now().Unix()-sl.GetlastSend() > int64(sl.resendForScaleOut)) || difference != sl.lastchange {
		fmt.Printf("scal out current %d,needcore is %d \n", currentcore, needcore)
		ScalerClient.AutoScalerCluster(context.Background(),req)
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

func (sl *Scale) GetNeedCores(costs int64, tidbtype string) float64 {
	var CostOneCore float64
	switch tidbtype {
	case backend.TiDBForAP:
		CostOneCore = CostOneApCore
	case backend.TiDBForTP:
		CostOneCore = CostOneTpCore
	}

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
