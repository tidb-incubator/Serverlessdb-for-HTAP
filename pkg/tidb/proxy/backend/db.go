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
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pingcap/tidb/proxy/core/errors"
	"github.com/pingcap/tidb/proxy/mysql"
)

const (
	Up = iota
	Down
	ManualDown
	Unknown

	InitConnCount           = 16
	DefaultMaxConnNum       = 1024
	PingPeroid        int64 = 4
)

const (
	//BigCost indicates whether the current node is a BigCost AP type node.
	BigCost = "bigcost"
)

type DB struct {
	sync.RWMutex

	addr     string
	user     string
	password string
	db       string
	state    int32

	maxConnNum  int
	InitConnNum int
	idleConns   chan *Conn
	cacheConns  chan *Conn
	checkConn   *Conn
	lastPing    int64

	pushConnCount int64
	popConnCount  int64
	usingConnsCount int64
	//Self indicates whether the current node is a proxy node.
	Self bool
	dbType string
}

func Open(addr string, user string, password string, dbName string,weight float64, tidbtype string) (*DB, error) {
	var err error
	db := new(DB)
	db.addr = addr
	db.user = user
	db.password = password
	db.db = dbName

	var conum int
	if tidbtype == TiDBForAP {
		db.maxConnNum = 256
		db.InitConnNum = 64
	} else {
		//if weight < 1.0 {
		//	conum = 512
		//} else {
		//	conum = int(128)
		//}
		conum = 128
		if conum> DefaultMaxConnNum{
			db.maxConnNum = DefaultMaxConnNum*2
			db.InitConnNum = DefaultMaxConnNum
		}else{
			max := conum * 10
			if max > (DefaultMaxConnNum*2) {
				max = DefaultMaxConnNum * 2
			}
			db.maxConnNum = max
			if conum > DefaultMaxConnNum {
				conum = DefaultMaxConnNum
			}
			db.InitConnNum = conum
		}
	}

	//check connection
	db.checkConn, err = db.newConn()
	if err != nil {
		db.Close()
		//fmt.Println("check conn err is ", err)
		return nil, err
	}

	db.idleConns = make(chan *Conn, db.maxConnNum)
	db.cacheConns = make(chan *Conn, db.maxConnNum)
	atomic.StoreInt32(&(db.state), Unknown)
	wg := &sync.WaitGroup{}
	wg.Add(db.InitConnNum)
	var cErr error
	for i := 0; i < db.maxConnNum; i++ {
		if i < db.InitConnNum {
			go func() {
				conn, err := db.newConn()
				defer wg.Done()
				if err != nil {
					cErr = err
					//fmt.Println("make conn err is ", err)
					return
				}
				db.cacheConns <- conn
				atomic.AddInt64(&db.pushConnCount, 1)
				//atomic.AddInt64(&db.usingConnsCount, -1)
			}()
		} else {
			conn := new(Conn)
			db.idleConns <- conn
			atomic.AddInt64(&db.pushConnCount, 1)
			//atomic.AddInt64(&db.usingConnsCount, -1)
		}
	}
	wg.Wait()
	if cErr != nil {
		db.Close()
		return nil,cErr
	}
	db.SetLastPing()
	atomic.StoreInt32(&(db.state), Up)
	return db, nil
}

func (db *DB) Addr() string {
	return db.addr
}

func (db *DB) DbType() string {
	return db.dbType
}

func (db *DB) State() string {
	var state string
	switch db.state {
	case Up:
		state = "up"
	case Down, ManualDown:
		state = "down"
	case Unknown:
		state = "unknow"
	}
	return state
}

func (db *DB) ConnCount() (int, int, int64, int64, int64, int) {
	db.RLock()
	defer db.RUnlock()
	return len(db.idleConns), len(db.cacheConns), db.pushConnCount, db.popConnCount,db.usingConnsCount, db.maxConnNum
}

func (db *DB) Close() error {
	db.Lock()
	idleChannel := db.idleConns
	cacheChannel := db.cacheConns
	db.cacheConns = nil
	db.idleConns = nil
	db.Unlock()
	if cacheChannel == nil || idleChannel == nil {
		return nil
	}

	close(cacheChannel)
	for conn := range cacheChannel {
		db.closeConn(conn)
	}
	close(idleChannel)

	return nil
}

func (db *DB) getConns() (chan *Conn, chan *Conn) {
	db.RLock()
	cacheConns := db.cacheConns
	idleConns := db.idleConns
	db.RUnlock()
	return cacheConns, idleConns
}

func (db *DB) getCacheConns() chan *Conn {
	db.RLock()
	conns := db.cacheConns
	db.RUnlock()
	return conns
}

func (db *DB) getIdleConns() chan *Conn {
	db.RLock()
	conns := db.idleConns
	db.RUnlock()
	return conns
}

func (db *DB) Ping() error {
	var err error
	if db.checkConn == nil {
		db.checkConn, err = db.newConn()
		if err != nil {
			if db.checkConn != nil {
				db.checkConn.Close()
				db.checkConn = nil
			}
			return err
		}
	}
	err = db.checkConn.Ping()
	if err != nil {
		if db.checkConn != nil {
			db.checkConn.Close()
			db.checkConn = nil
		}
		return err
	}
	return nil
}

func (db *DB) newConn() (*Conn, error) {
	co := new(Conn)

	if err := co.Connect(db.addr, db.user, db.password, db.db); err != nil {
		return nil, err
	}

	co.pushTimestamp = time.Now().Unix()

	return co, nil
}

func (db *DB) addIdleConn() {
	conn := new(Conn)
	select {
	case db.idleConns <- conn:
	default:
		break
	}
}

func (db *DB) closeConn(co *Conn) error {
	atomic.AddInt64(&db.pushConnCount, 1)

	if co != nil {
		co.Close()
		conns := db.getIdleConns()
		if conns != nil {
			select {
			case conns <- co:
				return nil
			default:
				return nil
			}
		}
	} else {
		db.addIdleConn()
	}
	return nil
}

func (db *DB) closeConnNotAdd(co *Conn) error {
	if co != nil {
		co.Close()
		conns := db.getIdleConns()
		if conns != nil {
			select {
			case conns <- co:
				return nil
			default:
				return nil
			}
		}
	} else {
		db.addIdleConn()
	}
	return nil
}

func (db *DB) tryReuse(co *Conn) error {
	var err error
	//reuse Connection
	if co.IsInTransaction() {
		//we can not reuse a connection in transaction status
		err = co.Rollback()
		if err != nil {
			return err
		}
	}

	if !co.IsAutoCommit() {
		//we can not  reuse a connection not in autocomit
		_, err = co.exec("set autocommit = 1")
		if err != nil {
			return err
		}
	}

	//connection may be set names early
	//we must use default utf8
	if co.GetCharset() != mysql.DEFAULT_CHARSET {
		err = co.SetCharset(mysql.DEFAULT_CHARSET, mysql.DEFAULT_COLLATION_ID)
		if err != nil {
			return err
		}
	}

	return nil
}

func (db *DB) PopConn() (*Conn, error) {
	var co *Conn
	var err error

	cacheConns, idleConns := db.getConns()
	if cacheConns == nil || idleConns == nil {
		return nil, errors.ErrDatabaseClose
	}
	co = db.GetConnFromCache(cacheConns)
	if co == nil {
		co, err = db.GetConnFromIdle(cacheConns, idleConns)
		if err != nil {
			return nil, err
		}
	}

	err = db.tryReuse(co)
	if err != nil {
		db.closeConn(co)
		return nil, err
	}

	return co, nil
}

func (db *DB) GetConnFromCache(cacheConns chan *Conn) *Conn {
	var co *Conn
	var err error
	for 0 < len(cacheConns) {
		co = <-cacheConns
		atomic.AddInt64(&db.popConnCount, 1)
		//atomic.AddInt64(&db.usingConnsCount, 1)
		if co != nil && PingPeroid < time.Now().Unix()-co.pushTimestamp {
			err = co.Ping()
			if err != nil {
				db.closeConn(co)
				co = nil
			}
		}
		if co != nil {
			break
		}
	}
	return co
}

func (db *DB) GetConnFromIdle(cacheConns, idleConns chan *Conn) (*Conn, error) {
	var co *Conn
	var err error
	select {
	case co = <-idleConns:
		atomic.AddInt64(&db.popConnCount, 1)
	//	atomic.AddInt64(&db.usingConnsCount, 1)
		co, err := db.newConn()
		if err != nil {
			db.closeConn(co)
			return nil, err
		}
		err = co.Ping()
		if err != nil {
			db.closeConn(co)
			return nil, errors.ErrBadConn
		}
		return co, nil
	case co = <-cacheConns:
		atomic.AddInt64(&db.popConnCount, 1)
		//atomic.AddInt64(&db.usingConnsCount, 1)
		if co == nil {
			return nil, errors.ErrConnIsNil
		}
		if co != nil && PingPeroid < time.Now().Unix()-co.pushTimestamp {
			err = co.Ping()
			if err != nil {
				db.closeConn(co)
				return nil, errors.ErrBadConn
			}
		}
	case <-time.After(time.Second):
		return nil,errors.ErrGetConnTimeout
	}
	return co, nil
}

func (db *DB) PushConn(co *Conn, err error) {
	atomic.AddInt64(&db.pushConnCount, 1)
	//atomic.AddInt64(&db.usingConnsCount, -1)
	if co == nil {
		db.addIdleConn()
		return
	}
	conns := db.getCacheConns()
	if conns == nil {
		co.Close()
		return
	}
	if err != nil {
		db.closeConnNotAdd(co)
		return
	}
	co.pushTimestamp = time.Now().Unix()
	select {
	case conns <- co:
		return
	default:
		db.closeConnNotAdd(co)
		return
	}
}

type BackendConn struct {
	*Conn
	db *DB
	bindConn bool
}

func (p *BackendConn) GetBindConn() bool{
	return p.bindConn
}

func (p *BackendConn) IsProxySelf() bool {
	return p.db.Self
}

func (p *BackendConn) GetDbType() string {
	return p.db.dbType
}

func (p *BackendConn) GetDbAddr() string {
	return p.db.addr
}

func (p *BackendConn) SetNoDelayTrue() {
	tcptemp := p.Conn.conn.(*net.TCPConn)
	tcptemp.SetNoDelay(true)
}
func (p *BackendConn) SetNoDelayFlase() {
	tcptemp := p.Conn.conn.(*net.TCPConn)
	tcptemp.SetNoDelay(false)
}

func (p *BackendConn) Close() {
	atomic.AddInt64(&p.db.usingConnsCount,-1)
	//fmt.Printf("using conn is %d \n",p.db.usingConnsCount)
	fmt.Printf("Close using conn is %d initnum %d,maxConn %d\n",p.db.usingConnsCount,p.db.InitConnNum,p.db.maxConnNum)

	if p != nil && p.Conn != nil {
		if p.Conn.pkgErr != nil {
			p.db.closeConn(p.Conn)
		} else {
			p.db.PushConn(p.Conn, nil)
		}
		p.Conn = nil
	}
}

func (db *DB) GetConn(bindFlag bool) (*BackendConn, error) {
	c, err := db.PopConn()
	if err != nil {
		return nil, err
	}
	atomic.AddInt64(&db.usingConnsCount,1)
	//80% connections pool
	poolConnNum := int64(db.maxConnNum * 4/5)
	if db.usingConnsCount > poolConnNum {
		if bindFlag == true {
			bindFlag = false
		}
	}
	return &BackendConn{c, db,bindFlag}, nil
}

func (db *DB) SetLastPing() {
	db.lastPing = time.Now().Unix()
}

func (db *DB) GetLastPing() int64 {
	return db.lastPing
}

func (p *BackendConn) SetPacketErr(err error) {
	if p != nil && p.Conn != nil {
		p.Conn.pkgErr = err
	}
}
