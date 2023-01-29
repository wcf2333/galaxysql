/*
 * Copyright [2013-2021], Alibaba Group Holding Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.polardbx.executor.operator;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.jdbc.BytesSql;
import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.jdbc.UnionBytesSql;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.utils.MathUtils;
import com.alibaba.polardbx.executor.chunk.Chunk;
import com.alibaba.polardbx.executor.mpp.metadata.Split;
import com.alibaba.polardbx.executor.mpp.split.JdbcSplit;
import com.alibaba.polardbx.executor.operator.lookup.LookupConditionBuilder;
import com.alibaba.polardbx.executor.operator.lookup.ShardingLookupConditionBuilder;
import com.alibaba.polardbx.executor.operator.spill.SpillerFactory;
import com.alibaba.polardbx.executor.utils.ExecUtils;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.join.LookupEquiJoinKey;
import com.alibaba.polardbx.optimizer.core.join.LookupPredicate;
import com.alibaba.polardbx.optimizer.core.rel.LogicalView;
import com.alibaba.polardbx.optimizer.partition.pruning.PartLookupPruningCache;
import com.alibaba.polardbx.optimizer.utils.RelUtils;
import com.google.common.base.Preconditions;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static com.alibaba.polardbx.common.exception.code.ErrorCode.ERR_EXECUTE_ON_MYSQL;

public class LookupTableScanExec extends TableScanExec implements LookupTableExec {
    private final boolean shardEnabled;

    List<Split> reservedSplits;

    private final LookupPredicate predicate;

    private final List<LookupEquiJoinKey> allJoinKeys; // including null-safe equal columns (`<=>`)

    public LookupTableScanExec(LogicalView logicalView, ExecutionContext context, TableScanClient scanClient,
                               boolean shardEnabled, SpillerFactory spillerFactory,
                               LookupPredicate predicate,
                               List<LookupEquiJoinKey> allJoinKeys, List<DataType> dataTypeList) {
        super(logicalView, context, scanClient, Long.MAX_VALUE, spillerFactory, dataTypeList);
        this.predicate = predicate;
        this.shardEnabled = shardEnabled;
        this.allJoinKeys = allJoinKeys;
    }

    @Override
    public void updateLookupPredicate(Chunk chunk) {
        if (!scanClient.noMoreSplit()) {
            throw new TddlRuntimeException(ERR_EXECUTE_ON_MYSQL, "input split not ready");
        }

        if (reservedSplits == null) {
            reservedSplits = new ArrayList<>();
            reservedSplits.addAll(scanClient.getSplitList());
        }

        scanClient.getSplitList().clear();

        if (shardEnabled) {
            updateShardedWhereSql(chunk);
        } else {
            // 不分片 把值全部下推
            updateNoShardedWhereSql(chunk);
        }
    }

    /**
     * 当判断可以将value按分片裁剪后
     * 为每一个 DynamicJdbcSplit 分配对应的where条件
     */
    private void updateShardedWhereSql(Chunk chunk) {

        // 获取到每个sqlNode对应的分片
        ShardingLookupConditionBuilder builder =
            new LookupConditionBuilder(allJoinKeys, predicate, logicalView, this.context).createSharding();

        Map<String, Map<String, SqlNode>> targetDBSqlNodeMap = builder.buildShardedCondition(chunk, context);
        for (Split split : reservedSplits) {
            updateShardedWhereSql(split, targetDBSqlNodeMap);
        }
    }

    private void updateShardedWhereSql(Split split,
                                       Map<String, Map<String, SqlNode>> targetDBSqlNodeMap) {
        JdbcSplit jdbcSplit = (JdbcSplit) (split.getConnectorSplit());

        List<List<String>> shardedTableNameList = jdbcSplit.getTableNames();
        String dbIndex = jdbcSplit.getDbIndex();
        Map<String, SqlNode> targetDbSqlNode = targetDBSqlNodeMap.get(dbIndex);
        if (targetDbSqlNode == null) {
            // 分库不存在的情况就不考虑
            return;
        }

        // 由于BKA plan在同一个split中使用了UNION
        // 需要记录下向每个分表发送的sql
        final int numTable = shardedTableNameList.size();

        List<SqlNode> conditions = new ArrayList<>(numTable);
        for (int tableIndex = 0; tableIndex < numTable; tableIndex++) {
            String tableName = shardedTableNameList.get(tableIndex).get(0);
            SqlNode shardedCondition = targetDbSqlNode.get(tableName);

            if (shardedCondition != null) {
                conditions.add(shardedCondition);
            } else {
                // nothing to lookup for this table
                conditions.add(LookupConditionBuilder.FALSE_CONDITION);
            }
        }
        if (conditions.size() > 0) {
            DynamicJdbcSplit dynamicSplit = new DynamicJdbcSplit(jdbcSplit, conditions);
            scanClient.addSplit(split.copyWithSplit(dynamicSplit));
        }
    }

    /**
     * 不分片，把值全部下推
     */
    private void updateNoShardedWhereSql(Chunk chunk) {
        LookupConditionBuilder builder = new LookupConditionBuilder(allJoinKeys, predicate, logicalView, this.context);
        SqlNode lookupCondition = builder.buildCondition(chunk);
        for (Split split : reservedSplits) {
            JdbcSplit jdbcSplit = (JdbcSplit) split.getConnectorSplit();
            DynamicJdbcSplit dynamicSplit = new DynamicJdbcSplit(jdbcSplit, lookupCondition);
            scanClient.addSplit(split.copyWithSplit(dynamicSplit));
        }
    }

    @Override
    public synchronized boolean resume() {
        this.isFinish = false;
        if (consumeResultSet != null) {
            consumeResultSet.close();
            consumeResultSet = null;
        }
        scanClient.reset();
        try {
            scanClient.executePrefetchThread(false);
        } catch (Throwable e) {
            TddlRuntimeException exception =
                new TddlRuntimeException(ErrorCode.ERR_EXECUTE_ON_MYSQL, e, e.getMessage());
            this.isFinish = true;
            scanClient.setException(exception);
            scanClient.throwIfFailed();
        }
        return true;
    }

    @Override
    synchronized void doClose() {
        super.doClose();
    }

    static class DynamicJdbcSplit extends JdbcSplit {

        private List<SqlNode> lookupConditions;

        public DynamicJdbcSplit(JdbcSplit jdbcSplit, SqlNode lookupCondition) {
            super(jdbcSplit);
            this.lookupConditions = Collections.nCopies(jdbcSplit.getTableNames().size(), lookupCondition);
            this.supportGalaxyPrepare = false;
        }

        public DynamicJdbcSplit(JdbcSplit jdbcSplit, List<SqlNode> lookupConditions) {
            super(jdbcSplit);
            Preconditions.checkArgument(jdbcSplit.getTableNames().size() == lookupConditions.size());
            this.lookupConditions = lookupConditions;
            this.supportGalaxyPrepare = false;
        }

        @Override
        public BytesSql getUnionBytesSql(boolean ignore) {
            if (hintSql == null) {
                int tableNum = getTableNames().stream().mapToInt(List::size).sum();
                String query = new UnionBytesSql(sqlTemplate.getBytesArray(), sqlTemplate.isParameterLast(), tableNum,
                        orderBy == null ? null : orderBy.getBytes(), null).toString(
                        null);
                for (SqlNode condition : lookupConditions) {
                    if (condition != null) {
                        query = StringUtils.replace(query, "'bka_magic' = 'bka_magic'",
                            RelUtils.toNativeSql(condition), 1);
                    }
                }
                hintSql = query;
            }
            return BytesSql.getBytesSql(hintSql);
        }

        @Override
        public List<ParameterContext> getFlattedParams() {
            if (flattenParams == null) {
                synchronized (this) {
                    if (flattenParams == null) {
                        List<List<ParameterContext>> params = getParams();
                        flattenParams = new ArrayList<>(params.size() > 0 ? params.get(0).size() : 0);
                        for (int i = 0; i < lookupConditions.size(); i++) {
                            flattenParams.addAll(params.get(i));
                        }
                    }
                }
            }
            return flattenParams;
        }

        @Override
        public void reset() {
            this.hintSql = null;
            this.lookupConditions = null;
        }
    }
}
