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

package com.alibaba.polardbx.executor.partitionmanagement.fastchecker;

import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.executor.fastchecker.FastChecker;
import com.alibaba.polardbx.executor.gsi.PhysicalPlanBuilder;
import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import com.alibaba.polardbx.optimizer.config.table.SchemaManager;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.PhyTableOperation;
import com.alibaba.polardbx.statistics.SQLRecorderLogger;

import java.text.MessageFormat;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Created by luoyanxin.
 *
 * @author luoyanxin
 */
public class AlterTableGroupFastChecker extends FastChecker {
    public AlterTableGroupFastChecker(String schemaName, String srcLogicalTableName,
                                      Map<String, Set<String>> srcPhyDbAndTables,
                                      Map<String, Set<String>> dstPhyDbAndTables,
                                      List<String> srcColumns, PhyTableOperation planSelectHashCheckSrc,
                                      PhyTableOperation planSelectHashCheckDst,
                                      PhyTableOperation planIdleSelectSrc, PhyTableOperation planIdleSelectDst,
                                      long parallelism, int lockTimeOut) {
        super(schemaName, srcLogicalTableName, srcLogicalTableName, null, srcPhyDbAndTables, dstPhyDbAndTables,
            srcColumns, srcColumns, planSelectHashCheckSrc, planSelectHashCheckDst, planIdleSelectSrc,
            planIdleSelectDst, parallelism, lockTimeOut);
    }

    public static FastChecker create(String schemaName, String tableName, Map<String, Set<String>> srcPhyDbAndTables,
                                     Map<String, Set<String>> dstPhyDbAndTables, long parallelism,
                                     ExecutionContext ec) {
        // Build select plan
        final SchemaManager sm = ec.getSchemaManager(schemaName);

        final TableMeta baseTableMeta = sm.getTable(tableName);

        final List<String> baseTableColumns = baseTableMeta.getAllColumns()
            .stream()
            .map(ColumnMeta::getName)
            .collect(Collectors.toList());

        final PhysicalPlanBuilder builder = new PhysicalPlanBuilder(schemaName, ec);

        final int lockTimeOut = ec.getParamManager().getInt(ConnectionParams.FASTCHECKER_LOCK_TIMEOUT);

        return new AlterTableGroupFastChecker(schemaName, tableName,
            srcPhyDbAndTables, dstPhyDbAndTables,
            baseTableColumns,
            builder.buildSelectHashCheckForChecker(baseTableMeta, baseTableColumns),
            builder.buildSelectHashCheckForChecker(baseTableMeta, baseTableColumns),
            builder.buildIdleSelectForChecker(baseTableMeta, baseTableColumns),
            builder.buildIdleSelectForChecker(baseTableMeta, baseTableColumns),
            parallelism, lockTimeOut);
    }

    @Override
    public boolean check(ExecutionContext baseEc) {
        boolean tsoCheckResult = tsoCheck(baseEc);
        if (tsoCheckResult) {
            return true;
        } else {
            SQLRecorderLogger.ddlLogger
                .warn(MessageFormat.format("[{0}] FastChecker with TsoCheck failed, begin XaCheck",
                    baseEc.getTraceId()));
        }

        /**
         * When tsoCheck is failed, bypath to use old checker directly.
         * because xaCheck of scaleout/gsi/alter tablegroup is easily to caused deadlock by using lock tables
         */
        //boolean xaCheckResult = xaCheckForHeterogeneousTable(baseEc);
        return tsoCheckResult;
    }
}
