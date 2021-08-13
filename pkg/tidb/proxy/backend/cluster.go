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
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pingcap/tidb/proxy/config"
	"github.com/pingcap/tidb/proxy/core/errors"
	"github.com/pingcap/tidb/proxy/core/golog"
	"github.com/pingcap/tidb/proxy/util"
)

const (
	Master      = "master"
	Tidb        = "Tidb"
	TidbSplit   = ","
	WeightSplit = "@"
)

type Cluster struct {
	Cfg config.ClusterConfig

	sync.RWMutex

	Tidbs         []*DB
	LastTidbIndex int
	RoundRobinQ   []int
	TidbsWeights  []float64

	DownAfterNoAlive time.Duration

	Online bool
}

func (cluster *Cluster) CheckCluster() {
	//to do
	//1 check connection alive

	for cluster.Online {
		cluster.checkTidbs()
		time.Sleep(16 * time.Second)
	}
}



func (cluster *Cluster) GetTidbConn() (*BackendConn, error) {
	cluster.Lock()
	db, err := cluster.GetNextTidb()
	cluster.Unlock()
	if err != nil {
		return nil, err
	}
   db.Self=false
	if db == nil {
		return nil, errors.ErrNoTidbDB
	}
	if atomic.LoadInt32(&(db.state)) == Down {
		return nil, errors.ErrTidbDown
	}

	return db.GetConn()
}

func (cluster *Cluster) checkTidbs() {
	cluster.RLock()
	if cluster.Tidbs == nil {
		cluster.RUnlock()
		return
	}
	Tidbs := make([]*DB, len(cluster.Tidbs))
	copy(Tidbs, cluster.Tidbs)
	cluster.RUnlock()

	for i := 0; i < len(Tidbs); i++ {
		if err := Tidbs[i].Ping(); err != nil {
			golog.Error("Node", "checkTidb", "Ping", 0, "db.Addr", Tidbs[i].Addr(), "error", err.Error())
		} else {
			if atomic.LoadInt32(&(Tidbs[i].state)) == Down {
				golog.Info("Node", "checkTidb", "Tidb up", 0, "db.Addr", Tidbs[i].Addr())
				cluster.UpTidb(Tidbs[i].addr)
			}
			Tidbs[i].SetLastPing()
			if atomic.LoadInt32(&(Tidbs[i].state)) != ManualDown {
				atomic.StoreInt32(&(Tidbs[i].state), Up)
			}
			continue
		}

		if int64(cluster.DownAfterNoAlive) > 0 && time.Now().Unix()-Tidbs[i].GetLastPing() > int64(cluster.DownAfterNoAlive/time.Second) {
			golog.Info("Node", "checkTidb", "Tidb down", 0,
				"db.Addr", Tidbs[i].Addr(),
				"Tidb_down_time", int64(cluster.DownAfterNoAlive/time.Second))
			//If can't ping Tidb after DownAfterNoAlive, set Tidb Down
			cluster.DownTidb(Tidbs[i].addr, Down)
		}
	}

}

func (cluster *Cluster) AddTidb(addr string) error {
	var db *DB
	var weight float64
	var err error
	if len(addr) == 0 {
		return errors.ErrAddressNull
	}
	cluster.Lock()
	defer cluster.Unlock()
	for _, v := range cluster.Tidbs {
		if strings.Split(v.addr, WeightSplit)[0] == strings.Split(addr, WeightSplit)[0] {
			return errors.ErrTidbExist
		}
	}
	addrAndWeight := strings.Split(addr, WeightSplit)
	if len(addrAndWeight) == 2 {
		weight, err = strconv.ParseFloat(addrAndWeight[1], 64)
		if err != nil {
			return err
		}
	} else {
		weight = 1
	}
	cluster.TidbsWeights = append(cluster.TidbsWeights, weight)
	if db, err = cluster.OpenDB(addrAndWeight[0],weight); err != nil {
		return err
	} else {
		cluster.Tidbs = append(cluster.Tidbs, db)
		cluster.InitBalancer()
		return nil
	}
}

func (cluster *Cluster) DeleteTidb(addr string) error {
	var i int
	cluster.Lock()
	defer cluster.Unlock()
	TidbCount := len(cluster.Tidbs)
	if TidbCount == 0 {
		return errors.ErrNoTidbDB
	}

	var he3db *DB

	for i = 0; i < TidbCount; i++ {
		if cluster.Tidbs[i].addr == addr {
			he3db = cluster.Tidbs[i]
			break
		}
	}

	if i == TidbCount {
		return errors.ErrTidbNotExist
	}
	if TidbCount == 1 {
		cluster.Tidbs = nil
		cluster.TidbsWeights = nil
		cluster.RoundRobinQ = nil
		return nil
	}

	s := make([]*DB, 0, TidbCount-1)
	sw := make([]float64, 0, TidbCount-1)
	for i = 0; i < TidbCount; i++ {
		if cluster.Tidbs[i].addr != addr {
			s = append(s, cluster.Tidbs[i])
			sw = append(sw, cluster.TidbsWeights[i])
		}
	}

	cluster.Tidbs = s
	cluster.TidbsWeights = sw
	cluster.InitBalancer()

	CanDelete := func() (bool, error) {
		golog.Info("Cluster", "DeleteTidb", "checking using conn num ", 0)

		if he3db.usingConnsCount == 0 {
			return true, nil
		}
		return false, nil
	}

	if err := util.Retry( 1 * time.Second, 30, CanDelete); err != nil {

		golog.Warn("Cluster", "DeleteTidb", "usingconn been killed", 0, "current conn num",he3db.usingConnsCount)
	}

	return nil
}

func (cluster *Cluster) OpenDB(addr string,weight float64) (*DB, error) {
	db, err := Open(addr, cluster.Cfg.User, cluster.Cfg.Password, "", weight)
	return db, err
}

func (cluster *Cluster) UpDB(addr string) (*DB, error) {
	weight:=1.0
	for i,db:= range cluster.Tidbs{
		if db.addr==addr{

	weight=cluster.TidbsWeights[i]
		}
	}

	db, err := cluster.OpenDB(addr,weight)


	if err != nil {
		return nil, err
	}

	if err := db.Ping(); err != nil {
		db.Close()
		atomic.StoreInt32(&(db.state), Down)
		return nil, err
	}
	atomic.StoreInt32(&(db.state), Up)
	return db, nil
}

func (cluster *Cluster) UpTidb(addr string) error {
	db, err := cluster.UpDB(addr)
	if err != nil {
		golog.Error("Node", "UpTidb", err.Error(), 0)
	}

	cluster.Lock()
	for k, Tidb := range cluster.Tidbs {
		if Tidb.addr == addr {
			cluster.Tidbs[k] = db
			cluster.Unlock()
			return nil
		}
	}
	cluster.Tidbs = append(cluster.Tidbs, db)
	cluster.Unlock()

	return err
}

func (cluster *Cluster) DownTidb(addr string, state int32) error {
	cluster.RLock()
	if cluster.Tidbs == nil {
		cluster.RUnlock()
		return errors.ErrNoTidbDB
	}
	Tidbs := make([]*DB, len(cluster.Tidbs))
	copy(Tidbs, cluster.Tidbs)
	cluster.RUnlock()

	//Tidb is *DB
	for _, Tidb := range Tidbs {
		if Tidb.addr == addr {
			Tidb.Close()
			atomic.StoreInt32(&(Tidb.state), state)
			break
		}
	}
	return nil
}

//TidbStr(127.0.0.1:3306@2,192.168.0.12:3306@3)
func (cluster *Cluster) ParseTidbs(Tidbs string) error {
	var db *DB
	var weight float64
	var err error

	if len(Tidbs) == 0 {
		return nil
	}
	Tidbs = strings.Trim(Tidbs, TidbSplit)
	TidbArray := strings.Split(Tidbs, TidbSplit)
	count := len(TidbArray)
	cluster.Tidbs = make([]*DB, 0, count)
	cluster.TidbsWeights = make([]float64, 0, count)

	//parse addr and weight
	for i := 0; i < count; i++ {
		addrAndWeight := strings.Split(TidbArray[i], WeightSplit)
		if len(addrAndWeight) == 2 {
			weight, err = strconv.ParseFloat(addrAndWeight[1], 64)
			if err != nil {
				return err
			}
		} else {
			weight = 1
		}
		cluster.TidbsWeights = append(cluster.TidbsWeights, weight)
		if db, err = cluster.OpenDB(addrAndWeight[0],weight); err != nil {
			continue
		}
		cluster.Tidbs = append(cluster.Tidbs, db)
	}
	cluster.InitBalancer()
	return nil
}
