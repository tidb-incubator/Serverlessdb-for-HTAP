package server

import (
	"context"
	"fmt"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/tidb/proxy/backend"
	"github.com/pingcap/tidb/proxy/mysql"
	"sync/atomic"
)

/*处理query语句*/
func (c *clientConn) handleDMLForProxy(ctx context.Context,conn *backend.BackendConn,stmt ast.StmtNode) ( error) {
	sessionVars := c.ctx.GetSessionVars()
	var rs *mysql.Result

	rs, err := c.executeInNode(conn, stmt.Text(), nil,nil)
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

func (c *clientConn) getBackendConn(cluster *backend.Cluster) (co *backend.BackendConn, err error) {
	sessionVars := c.ctx.GetSessionVars()
	cost := int64(sessionVars.Proxy.Cost)
	if cost > cluster.MaxCostPerSql {
		atomic.StoreInt64(&cluster.MaxCostPerSql, cost)
	}
	fmt.Println("current cost is ", cost, " max cost is ", cluster.MaxCostPerSql)
	if !sessionVars.InTxn() {
		//fmt.Println("no tran")
		co, err = cluster.GetTidbConn(cost)
		if err != nil {
			return
		}
	} else {
		co = c.txConn

		if co == nil {
			if co, err = cluster.GetTidbConn(cost); err != nil {
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
			}
		}

	}

	if !co.IsProxySelf() {
		if err = co.UseDB(c.dbname); err != nil {
			//reset the database to null
			c.dbname = ""
			return
		}
	}

	return
}

func (c *clientConn) executeInNode(conn *backend.BackendConn, sql string,paramtype []byte,args []interface{}) (*mysql.Result, error) {

	r, err := conn.Execute(sql,paramtype,args...)

	if err != nil {
		return nil, err
	}

	return r, err
}

func (c *clientConn) closeConn(conn *backend.BackendConn, rollback bool) {
	sessionVars := c.ctx.GetSessionVars()
	fmt.Println("session in txn is ", sessionVars.InTxn())
	if conn != nil {
		fmt.Println("conn info is ", conn)
	}
	if sessionVars.InTxn() {
		return
	}
	if conn == nil {
		return
	}
	defer conn.Close()
	dbtype := conn.GetDbType()
	cost := int64(sessionVars.Proxy.Cost)
	if !conn.IsProxySelf() && (dbtype == backend.TiDBForTP || dbtype == backend.TiDBForAP) {
		atomic.AddInt64(&c.server.cluster.BackendPools[dbtype].Costs, -cost)
	}
	if conn.IsProxySelf() {
		atomic.AddInt64(&c.server.cluster.ProxyNode.ProxyCost, -cost)
	}

	if rollback {
		conn.Rollback()
	}

	//stop the big size tidb when the big sql is finished.
	if dbtype == backend.BigCost {
		_, err := backend.ScaleTempTidb(c.server.cluster.Cfg.NameSpace, c.server.cluster.Cfg.ClusterName, 0, false, conn.GetAddr())
		if err != nil {
			fmt.Errorf("delete big size tidb %s faield: %s.", conn.GetAddr(), err)
		}
	}
}
