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
	"math/rand"
	"time"

	"github.com/pingcap/tidb/proxy/core/errors"
)

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

func (cluster *Cluster) InitBalancer() {
	var sum int
	cluster.LastTidbIndex = 0


	sws := make([]int, 0, len(cluster.TidbsWeights))

	for i := 0; i < len(cluster.TidbsWeights); i++ {
			sws = append(sws, int(cluster.TidbsWeights[i]*10))
	}



	gcd := Gcd(sws)

	for _, weight := range sws {
		sum += weight / gcd
	}

	cluster.RoundRobinQ = make([]int, 0, sum)
	for index, weight := range sws {
		for j := 0; j < weight/gcd; j++ {
			cluster.RoundRobinQ = append(cluster.RoundRobinQ, index)
		}
	}

	//random order
	if 1 < len(cluster.TidbsWeights) {
		r := rand.New(rand.NewSource(time.Now().UnixNano()))
		for i := 0; i < sum; i++ {
			x := r.Intn(sum)
			temp := cluster.RoundRobinQ[x]
			other := sum % (x + 1)
			cluster.RoundRobinQ[x] = cluster.RoundRobinQ[other]
			cluster.RoundRobinQ[other] = temp
		}
	}
}

func (cluster *Cluster) GetNextTidb() (*DB, error) {
	var index int
	queueLen := len(cluster.RoundRobinQ)
	if queueLen == 0 {
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
			return nil, errors.ErrNoDatabase
		}
		db = cluster.Tidbs[index]
		cluster.LastTidbIndex++
		cluster.LastTidbIndex = cluster.LastTidbIndex % queueLen
		if db.state == Up {
			return db, nil
		}
	}

	return nil, errors.ErrAllDatabaseDown


}
