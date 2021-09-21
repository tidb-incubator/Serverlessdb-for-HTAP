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
	"encoding/binary"
	"fmt"
	plannercore "github.com/pingcap/tidb/planner/core"

	//"github.com/pingcap/errors"
	//"github.com/pingcap/tidb/types"
	"math"
	//"strconv"
	//"strings"
	"context"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/tidb/proxy/backend"
	//"github.com/pingcap/tidb/proxy/core/golog"
	"github.com/pingcap/tidb/proxy/mysql"
	_ "github.com/pingcap/tidb/types/parser_driver"
	"github.com/pingcap/tidb/util/hack"
)

var paramFieldData []byte
var columnFieldData []byte

func init() {
	var p = &mysql.Field{Name: []byte("?")}
	var c = &mysql.Field{}

	paramFieldData = p.Dump()
	columnFieldData = c.Dump()
}

func (c *clientConn) handlePrepare(ctx context.Context,conn *backend.BackendConn,planstmt *plannercore.CachedPrepareStmt, s *TiDBStatement, args []interface{}) error {
	var rs *mysql.Result
	stmtctx := c.ctx.GetSessionVars().StmtCtx
	rs, err := c.executeInNode(conn,s,args)
	if err != nil {
		return err
	}

	if rs.Resultset != nil {
		err = c.writeResultsetForProxy(ctx, rs.Resultset)
	} else {
		if stmtctx.InSelectStmt {
			selectstmt, _ := planstmt.PreparedAst.Stmt.(*ast.SelectStmt)
			r := c.newEmptyResultsetAst(selectstmt)
			err = c.writeResultsetForProxy(ctx, r)
		}
		if stmtctx.InDeleteStmt || stmtctx.InInsertStmt || stmtctx.InUpdateStmt {
			err = c.writeOK(ctx)
		}
	}

	return err
}


func (c *clientConn) bindStmtArgs(s *TiDBStatement, args []interface{}, boundParams [][]byte,nullBitmap, paramTypes, paramValues []byte) error {
	//args := s.args

	pos := 0

	var v []byte
	var n int = 0
	var isNull bool
	var err error

	for i := 0; i < s.numParams; i++ {

		if boundParams[i] != nil {
			args[i] = boundParams[i]
			continue
		}


		if nullBitmap[i>>3]&(1<<(uint(i)%8)) > 0 {
			args[i] = nil
			continue
		}

		tp := paramTypes[i<<1]
		isUnsigned := (paramTypes[(i<<1)+1] & 0x80) > 0

		switch tp {
		case mysql.MYSQL_TYPE_NULL:
			args[i] = nil
			continue

		case mysql.MYSQL_TYPE_TINY:
			if len(paramValues) < (pos + 1) {
				return mysql.ErrMalformPacket
			}

			if isUnsigned {
				args[i] = uint8(paramValues[pos])
			} else {
				args[i] = int8(paramValues[pos])
			}

			pos++
			continue

		case mysql.MYSQL_TYPE_SHORT, mysql.MYSQL_TYPE_YEAR:
			if len(paramValues) < (pos + 2) {
				return mysql.ErrMalformPacket
			}

			if isUnsigned {
				args[i] = uint16(binary.LittleEndian.Uint16(paramValues[pos : pos+2]))
			} else {
				args[i] = int16((binary.LittleEndian.Uint16(paramValues[pos : pos+2])))
			}
			pos += 2
			continue

		case mysql.MYSQL_TYPE_INT24, mysql.MYSQL_TYPE_LONG:
			if len(paramValues) < (pos + 4) {
				return mysql.ErrMalformPacket
			}

			if isUnsigned {
				args[i] = uint32(binary.LittleEndian.Uint32(paramValues[pos : pos+4]))
			} else {
				args[i] = int32(binary.LittleEndian.Uint32(paramValues[pos : pos+4]))
			}
			pos += 4
			continue

		case mysql.MYSQL_TYPE_LONGLONG:
			if len(paramValues) < (pos + 8) {
				return mysql.ErrMalformPacket
			}

			if isUnsigned {
				args[i] = binary.LittleEndian.Uint64(paramValues[pos : pos+8])
			} else {
				args[i] = int64(binary.LittleEndian.Uint64(paramValues[pos : pos+8]))
			}
			pos += 8
			continue

		case mysql.MYSQL_TYPE_FLOAT:
			if len(paramValues) < (pos + 4) {
				return mysql.ErrMalformPacket
			}

			args[i] = float32(math.Float32frombits(binary.LittleEndian.Uint32(paramValues[pos : pos+4])))
			pos += 4
			continue

		case mysql.MYSQL_TYPE_DOUBLE:
			if len(paramValues) < (pos + 8) {
				return mysql.ErrMalformPacket
			}

			args[i] = math.Float64frombits(binary.LittleEndian.Uint64(paramValues[pos : pos+8]))
			pos += 8
			continue

		case mysql.MYSQL_TYPE_DECIMAL, mysql.MYSQL_TYPE_NEWDECIMAL, mysql.MYSQL_TYPE_VARCHAR,
			mysql.MYSQL_TYPE_BIT, mysql.MYSQL_TYPE_ENUM, mysql.MYSQL_TYPE_SET, mysql.MYSQL_TYPE_TINY_BLOB,
			mysql.MYSQL_TYPE_MEDIUM_BLOB, mysql.MYSQL_TYPE_LONG_BLOB, mysql.MYSQL_TYPE_BLOB,
			mysql.MYSQL_TYPE_VAR_STRING, mysql.MYSQL_TYPE_STRING, mysql.MYSQL_TYPE_GEOMETRY,
			mysql.MYSQL_TYPE_DATE, mysql.MYSQL_TYPE_NEWDATE,
			mysql.MYSQL_TYPE_TIMESTAMP, mysql.MYSQL_TYPE_DATETIME, mysql.MYSQL_TYPE_TIME:
			if len(paramValues) < (pos + 1) {
				return mysql.ErrMalformPacket
			}

			v, isNull, n, err = mysql.LengthEnodedString(paramValues[pos:])
			pos += n
			if err != nil {
				return err
			}

			if !isNull {
				args[i] = v
				continue
			} else {
				args[i] = nil
				continue
			}
		default:
			return fmt.Errorf("Stmt Unknown FieldType %d", tp)
		}
	}
	return nil
}



func (c *clientConn) newEmptyResultsetAst(stmt *ast.SelectStmt) *mysql.Resultset {
	r := new(mysql.Resultset)
	r.Fields = make([]*mysql.Field, len(stmt.Fields.Fields))

	for i, filed := range stmt.Fields.Fields {
		r.Fields[i] = &mysql.Field{}

		if filed.WildCard != nil {
			r.Fields[i].Name = []byte("*")
		} else if filed.Expr != nil {
			if filed.AsName.O != "" {
				r.Fields[i].Name = hack.Slice(filed.Expr.Text())
				r.Fields[i].OrgName = hack.Slice(filed.AsName.O)
			} else {
				r.Fields[i].Name = hack.Slice(filed.Expr.Text())
			}
		} else {
			r.Fields[i].Name = hack.Slice(filed.Expr.Text())
		}

		/*	switch e := filed.(type) {
			case *ast.StarExpr:
				r.Fields[i].Name = []byte("*")
			case *ast.NonStarExpr:
				if e.As != nil {
					r.Fields[i].Name = e.As
					r.Fields[i].OrgName = hack.Slice(nstring(e.Expr))
				} else {
					r.Fields[i].Name = hack.Slice(nstring(e.Expr))
				}

			default:
				r.Fields[i].Name = hack.Slice(nstring(e))
		*/
	}

	r.Values = make([][]interface{}, 0)
	r.RowDatas = make([]mysql.RowData, 0)

	return r
}
