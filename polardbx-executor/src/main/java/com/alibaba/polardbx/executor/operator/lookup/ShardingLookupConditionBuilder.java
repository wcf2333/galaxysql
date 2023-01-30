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

import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.model.sqljep.Comparative;
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
import com.alibaba.polardbx.optimizer.rule.Partitioner;
import com.alibaba.polardbx.optimizer.rule.TddlRuleManager;
import com.alibaba.polardbx.rule.TableRule;
import com.alibaba.polardbx.rule.model.Field;
import com.alibaba.polardbx.rule.model.TargetDB;
import com.alibaba.polardbx.rule.utils.CalcParamsAttribute;
import com.google.common.collect.Iterables;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.parser.SqlParserPos;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
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
    private static final String DEFAULT_TABLE = "DEFAULT_TABLE";

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
            Chunk lookupKeysChunk = extractLookupKeys(joinKeysChunk);
            Iterable<Tuple> distinctLookupKeys = distinctLookupKeysChunk(lookupKeysChunk);
            return buildSimpleShardedCondition(
                p.getColumn(0),
                extractSimpleValues(distinctLookupKeys),
                shardingColumns.get(0),
                ruleManager,
                context
            );
        }
        return buildGeneralShardedCondition(
            buildTupleIterable(joinKeysChunk),
            shardingColumns,
            ruleManager.getTableRule(v.getShardingTable()),
            ruleManager,
            context
        );
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

    private Map<String, Map<String, SqlNode>> buildSimpleShardedCondition(
        SqlIdentifier key, List<Object> values,
        ColumnMeta shardingKeyMeta, TddlRuleManager tddlRuleManager,
        ExecutionContext context) {

        Partitioner partitioner = OptimizerContext.getContext(tddlRuleManager.getSchemaName()).getPartitioner();
        Map<Integer, ParameterContext> params =
            context.getParams() == null ? null : context.getParams().getCurrentParameter();
        final List<TargetDB> targetDbList;
        Map<String, Comparative> comparatives = Partitioner.getComparativeORWithSingleColumn(
            shardingKeyMeta, values, shardingKeyMeta.getName());

        // fullComparative保障了分表条件可见

        Map<String, Comparative> fullComparative = partitioner.getInsertFullComparative(comparatives);
        Map<String, Object> calcParams = new HashMap<>();
        calcParams.put(CalcParamsAttribute.SHARD_FOR_EXTRA_DB, false);
        calcParams.put(CalcParamsAttribute.COM_DB_TB, fullComparative);
        calcParams.put(CalcParamsAttribute.CONN_TIME_ZONE, context.getTimeZone());
        calcParams.put(CalcParamsAttribute.EXECUTION_CONTEXT, context);
        targetDbList = tddlRuleManager.shard(v.getShardingTable(), false, false,
            comparatives, params, calcParams, context);

        Map<String, Map<String, SqlNode>> shardedConditions = new HashMap<>(targetDbList.size());
        for (TargetDB targetDB : targetDbList) {
            Map<String, SqlNode> tableConditions = new HashMap<>();
            for (Map.Entry<String, Field> tableNameField : targetDB.getTableNameMap().entrySet()) {
                Field field = tableNameField.getValue();
                SqlNode sqlNode;

                if (field == null) {
                    // 考虑不带sourceKey的情况
                    sqlNode = buildSimpleCondition(key, values);
                } else {
                    sqlNode = buildSimpleCondition(key, field.getSourceKeys().get(key.getSimple()));
                }
                if (sqlNode == FALSE_CONDITION) {
                    continue;
                }
                tableConditions.put(tableNameField.getKey(), sqlNode);
            }
            shardedConditions.put(targetDB.getDbIndex(), tableConditions);
        }
        return shardedConditions;
    }

    private Map<String, Map<String, SqlNode>> buildGeneralShardedCondition(
        Iterable<Tuple> joinKeyTuples, List<ColumnMeta> shardingKeyMetas,
        TableRule rule, TddlRuleManager tddlRuleManager,
        ExecutionContext context) {
        Partitioner partitioner = OptimizerContext.getContext(context.getSchemaName()).getPartitioner();
        final boolean shardByTable;
        // ensured by `canShard()`
        assert containsAllIgnoreCase(joinKeyColumnNames, rule.getDbPartitionKeys());

        // If shard-by-table is unavailable, we could use a 'DEFAULT_TABLE' as placeholder to represent any
        // table in one group, and unfold it to the actual topology later. This optimization helps deduce memory
        // usage while number of table partitions is very high
        shardByTable = containsAllIgnoreCase(joinKeyColumnNames, rule.getTbPartitionKeys());

        Map<Integer, ParameterContext> params =
            context.getParams() == null ? null : context.getParams().getCurrentParameter();

        // reused objects
        List<Object> shardingKeyValues = new ArrayList<>(jk.size());
        for (int i = 0; i < shardingKeyMetas.size(); i++) {
            shardingKeyValues.add(new Object());
        }

        // db_name -> tb_name -> list of tuples
        Map<String, Map<String, List<Tuple>>> shardedTuples = new HashMap<>();
        final Map<String, Set<String>> topology;

        topology = rule.getActualTopology();

        topology.forEach((dbKey, tables) -> {
            Map<String, List<Tuple>> tableValues = new HashMap<>();
            if (shardByTable) {
                for (String table : tables) {
                    tableValues.put(table, new ArrayList<>());
                }
            } else {
                tableValues.put(DEFAULT_TABLE, new ArrayList<>());
            }
            shardedTuples.put(dbKey, tableValues);
        });
        Map<String, Object> calcParams = new HashMap<>();
        calcParams.put(CalcParamsAttribute.SHARD_FOR_EXTRA_DB, false);
        calcParams.put(CalcParamsAttribute.COM_DB_TB, new Object());
        calcParams.put(CalcParamsAttribute.CONN_TIME_ZONE, context.getTimeZone());
        calcParams.put(CalcParamsAttribute.EXECUTION_CONTEXT, context);
        for (Tuple tuple : joinKeyTuples) {
            for (int i = 0; i < shardingKeyPositions.length; i++) {
                shardingKeyValues.set(i, tuple.get(shardingKeyPositions[i]));
            }

            Map<String, Comparative> comparatives =
                Partitioner.getLookupComparative(shardingKeyValues, shardingKeyMetas);

            // fullComparative保障了分表条件可见
            Map<String, Comparative> fullComparative = partitioner.getInsertFullComparative(comparatives);
            calcParams.put(CalcParamsAttribute.COM_DB_TB, fullComparative);

            final List<TargetDB> targetDbs = tddlRuleManager.shard(v.getShardingTable(), false, false,
                comparatives, params, calcParams, context);
            if (targetDbs.size() != 1) {
                throw new RuntimeException("expect one target db"); // see canShard()
            }

            for (TargetDB targetDb : targetDbs) {
                Collection<String> targetTables;
                if (shardByTable) {
                    targetTables = targetDb.getTableNames();
                } else {
                    targetTables = Collections.singleton(DEFAULT_TABLE);
                }
                for (String targetTable : targetTables) {
                    shardedTuples.compute(targetDb.getDbIndex(), (db, tableMap) -> {
                        assert tableMap != null;
                        tableMap.compute(targetTable, (tb, values) -> {
                            assert values != null;
                            values.add(tuple);
                            return values;
                        });
                        return tableMap;
                    });
                }
            }
        }

        // db_name -> tb_name -> condition
        Map<String, Map<String, SqlNode>> shardedCondition = new HashMap<>();
        shardedTuples.forEach((db, tableMap) -> {
            Map<String, SqlNode> tableCondMap = new HashMap<>();
            tableMap.forEach((tb, tableTuples) -> {
                if (!tableTuples.isEmpty()) {
                    SqlNode condition = buildCondition(tableTuples);
                    if (condition != FALSE_CONDITION) {
                        tableCondMap.put(tb, condition);
                    }
                }
            });
            if (!tableCondMap.isEmpty()) {
                shardedCondition.put(db, tableCondMap);
            }
        });

        if (!shardByTable) {
            // unfold groups -> DEFAULT_TABLE -> sharded condition
            // to groups -> actual tables -> sharded condition
            Map<String, Map<String, SqlNode>> unfoldedShardedCondition = new HashMap<>(shardedCondition.size());
            topology.forEach((dbKey, tables) -> {
                Map<String, SqlNode> t = shardedCondition.get(dbKey);
                if (t != null) {
                    final SqlNode cond = t.get(DEFAULT_TABLE);
                    Map<String, SqlNode> tableCond = new HashMap<>(tables.size());
                    for (String table : tables) {
                        tableCond.put(table, cond);
                    }
                    unfoldedShardedCondition.put(dbKey, tableCond);
                }
            });
            return unfoldedShardedCondition;
        }
        return shardedCondition;
    }

    private SqlNode buildCondition(Collection<Tuple> joinKeyTuples) {
        Iterable<Tuple> lookupKeys = extractLookupKeys(joinKeyTuples);
        Collection<Tuple> distinctLookupKeys = distinctLookupKeysChunk(lookupKeys);

        if (p.size() == 1) {
            List<Object> flattedValues = distinctLookupKeys.stream()
                .map(b -> b.get(0))
                .collect(Collectors.toList());
            return buildSimpleCondition(p.getColumn(0), flattedValues);
        } else {
            return buildMultiCondition(distinctLookupKeys);
        }
    }

    private Collection<Tuple> distinctLookupKeysChunk(Iterable<Tuple> lookupKeysTuples) {
        // Distinct by HashSet
        Set<Tuple> distinctLookupKeys = new HashSet<>();
        for (Tuple tuple : lookupKeysTuples) {
            distinctLookupKeys.add(tuple);
        }
        return distinctLookupKeys;
    }

    private Iterable<Tuple> extractLookupKeys(Iterable<Tuple> joinKeysTuple) {
        return Iterables.transform(joinKeysTuple, t -> {
            if (t == null) {
                return null;
            } else {
                Object[] data = new Object[p.size()];
                for (int i = 0; i < p.size(); i++) {
                    data[i] = t.get(lookupColumnPositions[i]);
                }
                return new Tuple(data);
            }
        });
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
