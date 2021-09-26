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

package server

import (
	"fmt"

	"github.com/pingcap/tidb/metrics"
	"github.com/pingcap/tidb/proxy/backend"

	"github.com/pingcap/tidb/proxy/mysql"
)

func (c *clientConn) isInTransaction() bool {

	return c.ctx.GetSessionVars().InTxn()

}


func (c *clientConn) isPrepare() bool {
	return c.ctx.GetSessionVars().GetStatusFlag(mysql.SERVER_STATUS_PREPARE)
}

func (c *clientConn) cleanPrePare(id uint32) error {
	if c.prepareConn == nil {
		return nil
	}
	if c.prepareConn.IsProxySelf() {
		return nil
	}
	c.prepareConn.ClosePrepare(id)
	stmts := c.ctx.GetMapStatement()
	if len(stmts) > 1 {
		return nil
	}
	//c.s.Close()
	c.ctx.GetSessionVars().SetStatusFlag(mysql.SERVER_STATUS_PREPARE, false)
	if c.ctx.GetSessionVars().InTxn() == false {
		c.prepareConn.SetNoDelayFlase()
		c.closeConn(c.prepareConn,false)
	}
	c.prepareConn = nil
	return nil
}

func (c *clientConn) setPrepare() {
	c.ctx.GetSessionVars().SetStatusFlag(mysql.SERVER_STATUS_PREPARE,true)
}

func (c *clientConn) isAutoCommit() bool {
	return c.ctx.GetSessionVars().IsAutocommit()

}

func (c *clientConn) handleBegin() error {

	//fmt.Printf("begin %+v \n",c.txConn)
	if co := c.txConn; co != nil {
		dbtype := co.GetDbType()
		if dbtype == backend.TiDBForTP || dbtype == backend.TiDBForAP {
			metrics.QueriesCounter.WithLabelValues(dbtype).Inc()
		}
		if !co.IsProxySelf() {
			if err := co.Begin(); err != nil {
				return err
			}
		}
	}

	//c.status |= mysql.SERVER_STATUS_IN_TRANS
	c.ctx.GetSessionVars().SetInTxn(true)
	return c.writeOK(nil)
}

func (c *clientConn) handleCommit() (err error) {
	if err := c.commit(); err != nil {
		return err
	} else {
		return c.writeOK(nil)
	}
}

func (c *clientConn) handleRollback() (err error) {
	if err := c.rollback(); err != nil {
		return err
	} else {
		return c.writeOK(nil)
	}
}

func (c *clientConn) commit() (err error) {
//	c.status &= ^mysql.SERVER_STATUS_IN_TRANS
  c.ctx.GetSessionVars().SetInTxn(false)
	if co := c.txConn; co != nil {
		dbtype := co.GetDbType()
		if dbtype == backend.TiDBForTP || dbtype == backend.TiDBForAP {
			metrics.QueriesCounter.WithLabelValues(dbtype).Inc()
		}
		if !co.IsProxySelf() {
			if e := co.Commit(); e != nil {
				err = e
			}
			co.SetNoDelayFlase()
		}
		if c.isPrepare() == false && !co.IsProxySelf() {
			co.Close()
		}
	}
	c.txConn = nil
	return
}

func (c *clientConn) commitInProxy() (err error) {
	if co := c.txConn; co != nil {
		if co.IsProxySelf() {
			c.ctx.GetSessionVars().SetInTxn(false)
		} else {
			fmt.Println("commitInProxy failed")
			return fmt.Errorf("commitInProxy failed")
		}
	}
	c.txConn = nil
	return
}

func (c *clientConn) rollback() (err error) {
	//c.status &= ^mysql.SERVER_STATUS_IN_TRANS
	c.ctx.GetSessionVars().SetInTxn(false)
   //fmt.Printf("rollback is %+v",c.txConn)
	if co := c.txConn; co != nil {
		dbtype := co.GetDbType()
		if dbtype == backend.TiDBForTP || dbtype == backend.TiDBForAP {
			metrics.QueriesCounter.WithLabelValues(dbtype).Inc()
		}
		if !co.IsProxySelf() {
			if e := co.Rollback(); e != nil {
				err = e
			}
			co.SetNoDelayFlase()
		}
		if c.isPrepare() == false  && !co.IsProxySelf() {
			co.Close()
		}
	}
	c.txConn = nil
	return
}

func (c *clientConn) rollbackInProxy() (err error) {
	//fmt.Printf("rollback is %+v",c.txConn)
	if co := c.txConn; co != nil {
		if co.IsProxySelf() {
			c.ctx.GetSessionVars().SetInTxn(false)
		} else {
			fmt.Println("rollbackInProxy failed")
			return fmt.Errorf("rollbackInProxy failed")
		}
	}
	c.txConn = nil
	return
}

