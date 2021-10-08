package server

import (
	"context"
	"fmt"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/tidb/metrics"
	"github.com/pingcap/tidb/proxy/backend"
	"github.com/pingcap/tidb/proxy/mysql"
	"github.com/pingcap/tidb/sessionctx/variable"
	"sync/atomic"
)

/*处理query语句*/
func (c *clientConn) handleDMLForProxy(ctx context.Context,conn *backend.BackendConn,stmt ast.StmtNode) ( error) {
	sessionVars := c.ctx.GetSessionVars()
	var rs *mysql.Result
	s := &TiDBStatement{
		sql: stmt.Text(),
	}
	rs, err := c.executeInNode(conn, s, nil)
	if err != nil {
		return  err
	}

	if rs == nil {
		msg := fmt.Sprintf("result is empty")
		return  mysql.NewError(mysql.ER_UNKNOWN_ERROR, msg)
	}
	sessionVars.StmtCtx.AddAffectedRows(rs.AffectedRows)
	sessionVars.StmtCtx.LastInsertID = rs.InsertId

	if rs.Resultset != nil {
		err = c.writeResultsetForProxy(ctx,rs.Resultset)
	} else {
		err = c.writeOK(ctx)
	}

	if err != nil {
		return  err
	}

	return  nil
}

func (c *clientConn) scaleClosePrepare(cluster *backend.Cluster) uint64 {
	if pool,ok:= cluster.BackendPools[backend.TiDBForTP];ok {
		curVersion := pool.CurVersion
		if c.GetCurVersion() != curVersion {
			if c.txConn == nil {
				if c.isPrepare() == true {
					// all connection use new tidb prepare
					if c.prepareConn != nil && !c.prepareConn.IsProxySelf() {
						if  c.prepareConn.GetBindConn() {
							for _, v := range c.ctx.GetMapStatement() {
								c.prepareConn.ClosePrepare(v.tidbId)
							}
							c.prepareConn.SetNoDelayFlase()
							c.prepareConn.Close()
						}
					}
					c.prepareConn = nil
				}
			}
		}
		return curVersion
	}
	return 0
}

func (c *clientConn) connSet(co *backend.BackendConn) (err error) {
	if !co.IsProxySelf() {
		if err = co.UseDB(c.dbname); err != nil {
			//reset the database to null
			c.dbname = ""
			return
		}
		charset,_ := variable.GetSessionOrGlobalSystemVar(c.ctx.GetSessionVars(), variable.CharacterSetConnection)
		collation,_ := variable.GetSessionOrGlobalSystemVar(c.ctx.GetSessionVars(), variable.CollationConnection)

		fmt.Printf("c.charset is %s,c.collation is %d \n",charset,mysql.CharsetIds[collation])
		if err = co.SetCharset(charset, mysql.CharsetIds[collation]); err != nil {
			return
		}
	}
	return
}

func (c *clientConn) mountPrepareConn(co *backend.BackendConn,curVersion uint64)(err error) {
	if co.GetBindConn() == true {
		if c.prepareConn == nil && c.isPrepare() == true {
			c.prepareConn = co
			if !co.IsProxySelf() {
				err = c.connSet(co)
				if err != nil {
					fmt.Println("connSet Failed", err)
					return
				}
				for _, v := range c.ctx.GetMapStatement() {
					var tidbS *backend.Stmt
					tidbS, err = co.Prepare(v.sql)
					if err != nil {
						fmt.Println("co.Prepare ", err)
						return
					}
					v.tidbId = tidbS.GetId()
				}
			}
			c.SetCurVersion(curVersion)
		}
	}
	return
}

func (c *clientConn) getBackendConn(cluster *backend.Cluster,bindFlag bool) (co *backend.BackendConn, err error) {
	sessionVars := c.ctx.GetSessionVars()
	cost := int64(sessionVars.Proxy.Cost)
	var Flag bool
	var curVersion uint64
	if cost > cluster.MaxCostPerSql {
		atomic.StoreInt64(&cluster.MaxCostPerSql, cost)
	}

	//fmt.Println("current cost is ", cost, " max cost is ", cluster.MaxCostPerSql)
	if !sessionVars.InTxn() && sessionVars.IsAutocommit() ||
		sessionVars.GetStatusFlag(mysql.SERVER_STATUS_PREPARE) == false {
		//fmt.Println("no tran")
		co, err = cluster.GetTidbConn(cost,false)
		if err != nil {
			return
		}
	} else {
		curVersion = c.scaleClosePrepare(cluster)
		if sessionVars.InTxn() || !sessionVars.IsAutocommit() {
			//set tx transaction
			var txStart bool
			if c.txConn == nil {
				c.txConn = c.prepareConn
				if c.prepareConn != nil {
					txStart = true
				}
			}
			co = c.txConn
			if co == nil {
				if co, err = cluster.GetTidbConn(cost, bindFlag); err != nil {
					return
				}
				if !co.IsProxySelf() {
					if !sessionVars.IsAutocommit() {
						if err = co.SetAutoCommit(0); err != nil {
							return
						}
					} else {
						if err = co.Begin(); err != nil {
							return
						}
					}
					co.SetNoDelayTrue()
				}
				c.txConn = co
			} else {
				if co.IsProxySelf() {
					atomic.AddInt64(&cluster.ProxyNode.ProxyCost, cost)
					metrics.QueriesCounter.WithLabelValues(backend.TiDBForTP).Inc()
				} else {
					if txStart == true {
						if !sessionVars.IsAutocommit() {
							if err = co.SetAutoCommit(0); err != nil {
								return
							}
						} else {
							if err = co.Begin(); err != nil {
								return
							}
						}
					}
					dbtype := co.GetDbType()
					if dbtype == backend.TiDBForTP || dbtype == backend.TiDBForAP {
						atomic.AddInt64(&cluster.BackendPools[dbtype].Costs, cost)
						metrics.QueriesCounter.WithLabelValues(dbtype).Inc()
					}
				}
			}
		} else {
			//no transation, scale out or scale in,prepare umount connection
			co = c.prepareConn
			if co == nil {
				if co, err = cluster.GetTidbConn(cost,bindFlag); err != nil {
					return
				}
				if !co.IsProxySelf() {
					co.SetNoDelayTrue()
				}
			} else {
				if co.IsProxySelf() {
					atomic.AddInt64(&cluster.ProxyNode.ProxyCost, cost)
					metrics.QueriesCounter.WithLabelValues(backend.TiDBForTP).Inc()
				} else {
					dbtype := co.GetDbType()
					if dbtype == backend.TiDBForTP || dbtype == backend.TiDBForAP {
						atomic.AddInt64(&cluster.BackendPools[dbtype].Costs, cost)
						metrics.QueriesCounter.WithLabelValues(dbtype).Inc()
					}
				}
			}
			//prepare mount connection
		}
		Flag = true
	}
	//prepare bind
	if Flag == true {
		err = c.mountPrepareConn(co,curVersion)
		if err != nil {
			if !co.IsProxySelf() {
				co.SetNoDelayFlase()
				co.Close()
			}
			c.txConn = nil
			c.prepareConn = nil
		}
	}
	if co.GetBindConn() == false {
		err = c.connSet(co)
	}

	return
}

func initTidbStmt(tidbStmt *backend.Stmt,conn *backend.Conn,s *TiDBStatement,bindFlag bool) {
	//init tidb stmt
	tidbStmt.SetColums(s.columns)
	tidbStmt.SetParamNum(s.numParams)
	tidbStmt.SetId(s.tidbId)
	tidbStmt.SetConn(conn)
	tidbStmt.SetQuery(s.sql)
	tidbStmt.SetBindConn(bindFlag)
}

func (c *clientConn) executeInNode(conn *backend.BackendConn, s *TiDBStatement,args []interface{}) (*mysql.Result, error) {
	tidbStmt := &backend.Stmt{}
	initTidbStmt(tidbStmt,conn.Conn,s,conn.GetBindConn())
	r, err := conn.Execute(tidbStmt,s.paramsType,args...)
	if err != nil {
		return nil, err
	}

	return r, err
}

func (c *clientConn) closeConn(conn *backend.BackendConn, rollback bool) {
	sessionVars := c.ctx.GetSessionVars()
	if conn == nil {
		return
	}
	dbtype := conn.GetDbType()
	cost := int64(sessionVars.Proxy.Cost)
	if !conn.IsProxySelf() && (dbtype == backend.TiDBForTP || dbtype == backend.TiDBForAP) {
		atomic.AddInt64(&c.server.cluster.BackendPools[dbtype].Costs, -cost)
	}
	if conn.IsProxySelf() {
		atomic.AddInt64(&c.server.cluster.ProxyNode.ProxyCost, -cost)
	}
	if sessionVars.InTxn() || !sessionVars.IsAutocommit() ||
		sessionVars.GetStatusFlag(mysql.SERVER_STATUS_PREPARE) == true &&
		c.prepareConn!= nil && c.prepareConn.GetBindConn() {
		return
	}
	if !conn.IsProxySelf() {
		if dbtype != backend.BigCost {
			defer conn.Close()
		}
		if rollback {
			conn.Rollback()
		}
	}
	c.prepareConn = nil
	c.txConn = nil
	//stop the big size tidb when the big sql is finished.
	if dbtype == backend.BigCost {
		_, err := backend.ScaleTempTidb(c.server.cluster.Cfg.NameSpace, c.server.cluster.Cfg.ClusterName, 0, false, conn.GetAddr())
		if err != nil {
			fmt.Errorf("delete big size tidb %s faield: %s.", conn.GetAddr(), err)
		}
	}
}
