/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package dal

import (
	"context"
)

import (
	"github.com/arana-db/arana/pkg/proto"
	"github.com/arana-db/arana/pkg/proto/rule"
	"github.com/arana-db/arana/pkg/runtime/ast"
	"github.com/arana-db/arana/pkg/runtime/optimize"
	"github.com/arana-db/arana/pkg/runtime/plan/dal"
)

func init() {
	optimize.Register(ast.SQLTypeShowIndex, optimizeShowIndex)
}

// 显示指定表的所有索引
// 需要最后一个逻辑表的 库名：表名映射
// 默认取库0：表0
func optimizeShowIndex(_ context.Context, o *optimize.Optimizer) (proto.Plan, error) {
	stmt := o.Stmt.(*ast.ShowIndex)

	ret := &dal.ShowIndexPlan{Stmt: stmt}
	ret.BindArgs(o.Args)

	vt, ok := o.Rule.VTable(stmt.TableName.Suffix())
	if !ok {
		return ret, nil
	}

	shards := rule.DatabaseTables{}

	topology := vt.Topology()
	if d, t, ok := topology.Render(0, 0); ok {
		shards[d] = append(shards[d], t)
	}
	ret.Shards = shards
	return ret, nil
}
