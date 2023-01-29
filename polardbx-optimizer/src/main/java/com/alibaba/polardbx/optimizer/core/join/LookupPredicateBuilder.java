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

package com.alibaba.polardbx.optimizer.core.join;

import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.parser.SqlParserPos;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class LookupPredicateBuilder {

    private final Join join;

    private final LookupPredicate predicate;

    public LookupPredicateBuilder(Join join, List<String> lvOriginNames) {
        this.join = join;
        boolean notIn = (join.getJoinType() == JoinRelType.ANTI);
        this.predicate = new LookupPredicate(notIn, lvOriginNames);
    }

    public LookupPredicate build(List<LookupEquiJoinKey> joinKeys) {
        for (LookupEquiJoinKey key : joinKeys) {
            if (key.isNullSafeEqual()) {
                continue; // '<=>' semantics can not be represented as IN expression
            }
            if (!key.isCanFindOriginalColumn()) {
                continue; // can not be represented as IN expression, because column origin is null
            }
            String column = getJoinKeyColumnName(key);
            int targetIndex = key.getOuterIndex();
            predicate.addEqualPredicate(
                new SqlIdentifier(column, SqlParserPos.ZERO), targetIndex, key.getUnifiedType());
        }
        return predicate;
    }

    public static String getJoinKeyColumnName(LookupEquiJoinKey joinKey) {
        return joinKey.getLookupColunmnName();
    }
}
