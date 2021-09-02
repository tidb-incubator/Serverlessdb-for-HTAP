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

func (c *clientConn) isInTransaction() bool {

	return c.ctx.GetSessionVars().InTxn()

}

func (c *clientConn) isAutoCommit() bool {
	return c.ctx.GetSessionVars().IsAutocommit()

}

func (c *clientConn) handleBegin() error {

	//fmt.Printf("begin %+v \n",c.txConn)
	if co := c.txConn; co != nil {
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
		if !co.IsProxySelf() {
			if e := co.Commit(); e != nil {
				err = e
			}
			co.SetNoDelayFlase()
		}

		co.Close()
	}
		c.txConn = nil
	return
}

func (c *clientConn) commitInProxy() (err error) {
	if co := c.txConn; co != nil {
		co.Close()
	}

	c.txConn = nil
	return
}






func (c *clientConn) rollback() (err error) {
	//c.status &= ^mysql.SERVER_STATUS_IN_TRANS
	c.ctx.GetSessionVars().SetInTxn(false)
   //fmt.Printf("rollback is %+v",c.txConn)
	if co := c.txConn; co != nil {
		if !co.IsProxySelf() {
			if e := co.Rollback(); e != nil {
				err = e
			}
			co.SetNoDelayFlase()
		}

		co.Close()

	}

	c.txConn = nil
	return
}

func (c *clientConn) rollbackInProxy() (err error) {
	//fmt.Printf("rollback is %+v",c.txConn)
	if co := c.txConn; co != nil {
		co.Close()
	}

	c.txConn = nil
	return
}

