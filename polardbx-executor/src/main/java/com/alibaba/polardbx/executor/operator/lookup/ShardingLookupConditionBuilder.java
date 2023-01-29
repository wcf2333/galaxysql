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

package com.alibaba.polardbx.executor.operator.lookup;

import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.executor.chunk.Chunk;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.join.LookupEquiJoinKey;
import com.alibaba.polardbx.optimizer.core.join.LookupPredicate;
import com.alibaba.polardbx.optimizer.core.rel.LogicalView;
import com.alibaba.polardbx.optimizer.core.rel.ShardProcessor;
import com.alibaba.polardbx.optimizer.core.rel.SimpleShardProcessor;
import com.alibaba.polardbx.optimizer.rule.TddlRuleManager;
import com.alibaba.polardbx.rule.TableRule;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.parser.SqlParserPos;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static com.alibaba.polardbx.optimizer.core.join.LookupPredicateBuilder.getJoinKeyColumnName;

/**
 * ShardingLookupConditionBuilder builds lookup condition for each shards
 *
 * @see LookupConditionBuilder
 */
public class ShardingLookupConditionBuilder extends LookupConditionBuilder {

    private final List<ColumnMeta> shardingColumns;
    private final int[] shardingKeyPositions;
    private final List<String> joinKeyColumnNames;

    ShardingLookupConditionBuilder(List<LookupEquiJoinKey> jk, LookupPredicate p, LogicalView v, ExecutionContext ec) {
        super(jk, p, v, ec);
        this.joinKeyColumnNames = collectJoinKeyColumns();
        this.shardingColumns = collectShardingColumnMeta();
        this.shardingKeyPositions = buildShardingKeyPositions(shardingColumns);
    }

    public Map<String, Map<String, SqlNode>> buildShardedCondition(Chunk joinKeysChunk, ExecutionContext context) {
        TddlRuleManager ruleManager = context.getSchemaManager(v.getSchemaName()).getTddlRuleManager();
        /*
         * Lookup Join 的 equi-condition columns 必须包含 sharding columns，因此有以下几种情况
         * 1. J = {a} , S = {a}
         * 2. J = {a, b} , S = {a, b}
         * 3. J = {a, b} , S = {a}
         * 4. J = {a, b, c} , S = {a, b}
         * 5. J = {a}, S = {a, b} - 由于分表键 b 未覆盖，仅能进行分库
         * 6. J = {a, c}, S = {a, b} - 同上
         */
        if (isSinglePredicateShardingKey()) {
            if (canUseSimpleShard(ruleManager.getTableRule(v.getShardingTable()))) {
                SimpleShardProcessor shardProcessor = ShardProcessor.buildSimple(v, v.getShardingTable(), context);
                return buildShardedConditionBySimpleProcessor(
                    buildTupleIterable(joinKeysChunk),
                    shardProcessor,
                    context);
            }
            throw new RuntimeException("single key without simple rule not implement");
        }
        throw new RuntimeException("multi key not implement");
    }

    /**
     * 谓词列与分片键均为一列且相同
     * 且符合简单分片规则
     */
    private boolean canUseSimpleShard(TableRule tableRule) {
        if (p.size() != 1 || shardingColumns.size() != 1) {
            return false;
        }
        // canShard() ensures joinkey contains shardingkey
        boolean joinKeysContainPredicate = false;
        for (String joinKey : joinKeyColumnNames) {
            if (joinKey.equalsIgnoreCase(p.getColumn(0).getSimple())) {
                joinKeysContainPredicate = true;
                break;
            }
        }
        if (!joinKeysContainPredicate) {
            return false;
        }

        return ShardProcessor.isSimpleRule(tableRule);
    }

    /**
     * 谓词列与分片键均为一列且相同
     */
    private boolean isSinglePredicateShardingKey() {
        return p.size() == 1 && shardingColumns.size() == 1
            && p.getColumn(0).getSimple().equalsIgnoreCase(shardingColumns.get(0).getName());
    }

    private Map<String, Map<String, Set<Object>>> shardValueBySimpleProcessor(Iterable<Tuple> joinKeyTuples,
                                                                              SimpleShardProcessor shardProcessor,
                                                                              ExecutionContext context) {
        Map<String, Map<String, Set<Object>>> shardedValues = new HashMap<>();

        final Map<String, Set<String>> topology = shardProcessor.getTableRule().getActualTopology();

        topology.forEach((dbKey, tables) -> {
            Map<String, Set<Object>> tableValues = new HashMap<>();
            for (String table : tables) {
                tableValues.put(table, null);
            }
            shardedValues.put(dbKey, tableValues);
        });

        for (Tuple tuple : joinKeyTuples) {
            // since p.size() == 1, we can simplify null value condition
            Object lookupValue = tuple.get(0);
            if (lookupValue == null) {
                continue;
            }
            Object shardValue = tuple.get(shardingKeyPositions[0]);
            // get lookup dbTable by sharding value
            Pair<String, String> dbAndTable = shardProcessor.shard(shardValue, context);
            Set<Object> valueSet = shardedValues.get(dbAndTable.getKey())
                .computeIfAbsent(dbAndTable.getValue(), (table) -> new HashSet<>());
            valueSet.add(lookupValue);
        }

        return shardedValues;
    }

    private Map<String, Map<String, SqlNode>> buildShardedConditionBySimpleProcessor(Iterable<Tuple> joinKeyTuples,
                                                                                     SimpleShardProcessor shardProcessor,
                                                                                     ExecutionContext context) {
        assert shardingColumns.size() == 1;
        assert p.size() == 1;
        Map<String, Map<String, Set<Object>>> shardedValues = shardValueBySimpleProcessor(joinKeyTuples, shardProcessor, context);

        Map<String, Map<String, SqlNode>> shardedCondition = new HashMap<>();
        SqlIdentifier key = p.getColumn(0);

        for (Map.Entry<String, Map<String, Set<Object>>> shardedValueEntry : shardedValues.entrySet()) {
            String dbIndex = shardedValueEntry.getKey();
            for (Map.Entry<String, Set<Object>> tableValueEntry : shardedValueEntry.getValue().entrySet()) {
                String tbName = tableValueEntry.getKey();
                Set<Object> valueSet = tableValueEntry.getValue();
                SqlNode node = null;
                if (valueSet != null && valueSet.size() != 0) {
                    SqlNodeList sqlNodeList = new SqlNodeList(SqlParserPos.ZERO);
                    for (Object value : valueSet) {
                        sqlNodeList.add(createLiteralValue(value));
                    }
                    node = new SqlBasicCall(p.getOperator(), new SqlNode[] {key, sqlNodeList}, SqlParserPos.ZERO);
                }

                if (node != null) {
                    shardedCondition.computeIfAbsent(dbIndex, (db) -> new HashMap<>()).put(tbName, node);
                }
            }
        }
        return shardedCondition;
    }

    private int[] buildShardingKeyPositions(List<ColumnMeta> shardingKeyMetas) {
        int[] shardingKeyPositionInValue = new int[shardingKeyMetas.size()];
        for (int i = 0; i < shardingKeyMetas.size(); i++) {
            int position = -1;
            for (int j = 0; j < jk.size(); j++) {
                String joinColumnName = getJoinKeyColumnName(jk.get(j));
                if (shardingKeyMetas.get(i).getName().equalsIgnoreCase(joinColumnName)) {
                    position = j;
                    break;
                }
            }
            if (position == -1) {
                throw new AssertionError("impossible: sharding column not found");
            }
            shardingKeyPositionInValue[i] = position;
        }
        return shardingKeyPositionInValue;
    }

    private List<ColumnMeta> collectShardingColumnMeta() {
        final OptimizerContext oc = OptimizerContext.getContext(v.getSchemaName());
        final TableMeta tableMeta = ec.getSchemaManager(v.getSchemaName()).getTable(v.getShardingTable());
        List<String> shardColumns = oc.getRuleManager().getSharedColumns(v.getShardingTable());

        return shardColumns.stream()
            // filter the sharding columns covered by lookup predicate
            .filter(c -> containsIgnoreCase(joinKeyColumnNames, c))
            .map(tableMeta::getColumnIgnoreCase)
            .collect(Collectors.toList());
    }

    private static boolean containsIgnoreCase(Collection<String> a, String b) {
        List<String> ca = a.stream().map(String::toLowerCase).collect(Collectors.toList());
        return ca.contains(b.toLowerCase());
    }
}
