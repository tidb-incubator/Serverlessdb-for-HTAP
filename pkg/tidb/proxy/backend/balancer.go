// Copyright 2016 The he3proxy Authors. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"): you may
// not use this file except in compliance with the License. You may obtain
// a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// License for the specific language governing permissions and limitations
// under the License.

package backend

import (
	"context"
	"fmt"
	"github.com/pingcap/tidb/proxy/core/errors"
	"github.com/pingcap/tidb/proxy/scalepb"
	"google.golang.org/grpc"
	"sync/atomic"
)

const DefaultBigSize = 16.0

func Gcd(ary []int) int {
	var i int
	min := ary[0]
	length := len(ary)
	for i = 0; i < length; i++ {
		if ary[i] < min {
			min = ary[i]
		}
	}

	for {
		isCommon := true
		for i = 0; i < length; i++ {
			if ary[i]%min != 0 {
				isCommon = false
				break
			}
		}
		if isCommon {
			break
		}
		min--
		if min < 1 {
			break
		}
	}
	return min
}

func ngcd(costs []int, n int) int {
	if n == 1 {
		return costs[n-1]
	}
	return gcd(costs[n-1], ngcd(costs, n-1))
}

func gcd(x, y int) int {
	if x == 0 {
		return y
	}
	complment := y % x
	if complment != 0 {
		return gcd(complment, x)
	} else {
		return x
	}
}

func (cluster *Pool) InitBalancer() {
	var sum int
	cluster.LastTidbIndex = 0

	sws := make([]int, 0, len(cluster.TidbsWeights))

	for i := 0; i < len(cluster.TidbsWeights); i++ {
		sws = append(sws, int(cluster.TidbsWeights[i]*10))
	}

	//gcd := Gcd(sws)
	gcd := ngcd(sws, len(sws))

	for i, v := range sws {
		weight := v / gcd
		sum += weight
		sws[i] = weight
	}
	cluster.RoundRobinQ = make([]int, 0, sum)
	//order by SWRR algorithm.
	if 1 < len(cluster.TidbsWeights) {
		cluster.RoundRobinQ = order(sws)
	}
	fmt.Println("cluster RoundRobinQ is ", cluster.RoundRobinQ, cluster.TidbsWeights)
	for i:=0;i<len(cluster.Tidbs);i++ {
		fmt.Println("db weight db self",cluster.Tidbs[i].addr,cluster.Tidbs[i].Self)
	}
}

type peer struct {
	index   int
	current int
	execpt  int
	weight  int
}

func order(tidbsWeights []int) []int {
	var total int
	var peers = make(map[int]*peer)
	for key, value := range tidbsWeights {
		peers[key] = &peer{
			index:  key,
			weight: value,
			execpt: value,
		}
		total += value
	}
	result := make([]int, total, total)
	var best *peer
	for i := 0; i < total; i++ {
		for _, v := range peers {
			v.current += v.execpt
			if best == nil || v.current > best.current {
				best = v
			}
		}
		if best == nil {
			return result
		}
		best.current -= total
		result[i] = best.index
	}
	return result
}

func (cluster *Cluster) GetNextTidb(lbIndicator string, cost int64,bindFlag bool) (*DB, error) {
	//Distinguish SQL types based on costs
	var db *DB
	var err error
	switch {
	case cost <= 10000:
		//Predicate SQL is belong to TP type
		pool := cluster.BackendPools[TiDBForTP]
		var i int
		for ;i<30;i++ {
			pool.Lock()
			//if cluster.ProxyNode.IsPureCompute && len(pool.Tidbs) == 1 {
			if len(pool.Tidbs) == 1 {
				db = pool.Tidbs[0]
			} else {
				db, err = pool.GetNextDB(lbIndicator)
				if err != nil {
					pool.Unlock()
					return nil, err
				}
			}
			pool.Unlock()
			if db.Self {
				atomic.AddInt64(&cluster.ProxyNode.ProxyCost, cost)
			}

			if db.Self {
				atomic.AddInt64(&cluster.ProxyNode.ProxyCost, cost)

			} else {
				atomic.AddInt64(&pool.Costs, cost)
			}
			return db, err
		}

	case cost > 1000000000:
		//Predicate SQL is belong to Big AP type
		//invoke grpc api of starting a new pod to handle this request.
		var tempSize float32
		switch {
		case cost < 10000000000:
			tempSize = 16.0
		case cost > 10000000000 && cost < 100000000000:
			tempSize = 32.0
		case cost > 100000000000:
			tempSize = 64.0
		default:
			tempSize = DefaultBigSize
		}
		resp, err := ScaleTempTidb(cluster.Cfg.NameSpace, cluster.Cfg.ClusterName, tempSize, true, "")
		if err != nil {
			return nil, err
		}
		return GetBigCostDB(resp.GetStartAddr(), cluster.Cfg.User, cluster.Cfg.Password, "")

	default:
		//choose AP tidb pools
		pool := cluster.BackendPools[TiDBForAP]
		pool.Lock()
		if len(pool.Tidbs) == 1 {
			db = pool.Tidbs[0]
		} else {
			db, err = pool.GetNextDB(lbIndicator)
			if err != nil {
				pool.Unlock()
				return nil, err
			}
		}
		pool.Unlock()
		atomic.AddInt64(&pool.Costs, cost)
		return db, err
	}
	return db, err
}

func (cluster *Pool) GetNextDB(indicator string) (*DB, error) {
	switch indicator {
	case "qps":
		var index int
		queueLen := len(cluster.RoundRobinQ)
		if queueLen == 0 {
			fmt.Println("queueLen is 0, cluster tidb is ", cluster.Tidbs, cluster.RoundRobinQ, cluster.TidbsWeights)
			return nil, errors.ErrNoDatabase
		}
		if queueLen == 1 {
			index = cluster.RoundRobinQ[0]
			return cluster.Tidbs[index], nil
		}

		cluster.LastTidbIndex = cluster.LastTidbIndex % queueLen

		var db *DB
		for i := 0; i < len(cluster.RoundRobinQ); i++ {
			index = cluster.RoundRobinQ[cluster.LastTidbIndex]
			if len(cluster.Tidbs) <= index {
				fmt.Println("========index is====", index)
				return nil, errors.ErrNoDatabase
			}
			db = cluster.Tidbs[index]
			cluster.LastTidbIndex++
			cluster.LastTidbIndex = cluster.LastTidbIndex % queueLen
			if db.state == Up {
				return db, nil
			}
		}
	case "cost":
		//Check whether the number of tidb nodes exceeds 8.
		//when less then 8, get tidb node of least costs.
		//if len(cluster.Tidbs) < 8 {
		//	var bestTidb *DB
		//	for _, tidb := range cluster.Tidbs {
		//		if bestTidb == nil || bestTidb.costs > tidb.costs {
		//			bestTidb = tidb
		//		}
		//	}
		//	return bestTidb, nil
		//}

		//todo: add get tidb node when the number of tidb nodes exceeds 8.
	default:
		return nil, errors.ErrAllDatabaseDown
	}
	return nil, errors.ErrInternalServer
}

func GetBigCostDB(addr string, user string, password string, dbName string) (*DB, error) {
	db := new(DB)
	db.addr = addr
	db.user = user
	db.password = password
	db.db = dbName
	db.dbType = BigCost

	return db, nil
}

func ScaleTempTidb(ns, clus string, hashrate float32, needStart bool, needStopAddr string) (*scalepb.TempClusterReply, error) {
	serviceName := "scale-operator.sldb-admin.svc:8028"

	conn, err := grpc.Dial(serviceName, grpc.WithInsecure())
	if err != nil {
		fmt.Errorf("scale big tidb failed:%s", err)
		return nil, err
	}
	defer conn.Close()

	t := scalepb.NewScaleClient(conn)

	// 调用gRPC接口
	tr, err := t.ScaleTempCluster(context.Background(), &scalepb.TempClusterRequest{
		Clustername: clus,
		Namespace:   ns,
		Start:       needStart,
		Hashrate:    hashrate,
		StopAddr:    needStopAddr,
	})
	if err != nil {
		return nil, err
	}
	return tr, nil
}
