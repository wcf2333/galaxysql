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

    private final LookupPredicate predicate;

    private final List<LookupEquiJoinKey> allJoinKeys; // including null-safe equal columns (`<=>`)

    public LookupTableScanExec(LogicalView logicalView, ExecutionContext context, TableScanClient scanClient,
                               SpillerFactory spillerFactory,
                               LookupPredicate predicate,
                               List<LookupEquiJoinKey> allJoinKeys, List<DataType> dataTypeList) {
        super(logicalView, context, scanClient, Long.MAX_VALUE, spillerFactory, dataTypeList);
        this.predicate = predicate;
        this.allJoinKeys = allJoinKeys;
    }

    @Override
    public void updateLookupPredicate(Chunk chunk) {
        if (!scanClient.noMoreSplit()) {
            throw new TddlRuntimeException(ERR_EXECUTE_ON_MYSQL, "input split not ready");
        }

        List<Split> reservedSplits = new ArrayList<>();
        reservedSplits.addAll(scanClient.getSplitList());
        scanClient.getSplitList().clear();

        // 不分片 把值全部下推
        updateNoShardedWhereSql(reservedSplits, chunk);
    }

    /**
     * 不分片，把值全部下推
     */
    private void updateNoShardedWhereSql(List<Split> reservedSplits, Chunk chunk) {
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

        private SqlNode lookupConditions;

        public DynamicJdbcSplit(JdbcSplit jdbcSplit, SqlNode lookupCondition) {
            super(jdbcSplit);
            this.lookupConditions = lookupCondition;
            this.supportGalaxyPrepare = false;
        }

        @Override
        public BytesSql getUnionBytesSql(boolean ignore) {
            if (hintSql == null) {
                int tableNum = getTableNames().stream().mapToInt(List::size).sum();
                String query = new UnionBytesSql(sqlTemplate.getBytesArray(), sqlTemplate.isParameterLast(), tableNum,
                        orderBy == null ? null : orderBy.getBytes(), null).toString(
                        null);
                query = StringUtils.replace(query, "'bka_magic' = 'bka_magic'",
                    RelUtils.toNativeSql(lookupConditions));
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
                        for (int i = 0; i < params.size(); i++) {
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
