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

package com.alibaba.polardbx.optimizer.core.planner.rule;

import com.alibaba.polardbx.optimizer.core.rel.OSSTableScan;
import com.alibaba.polardbx.optimizer.hint.util.CheckJoinHint;
import com.alibaba.polardbx.optimizer.utils.RelUtils;
import com.google.common.collect.ImmutableList;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.optimizer.PlannerContext;
import com.alibaba.polardbx.optimizer.core.DrdsConvention;
import com.alibaba.polardbx.optimizer.core.rel.LogicalView;
import com.alibaba.polardbx.optimizer.core.rel.SemiBKAJoin;
import com.alibaba.polardbx.optimizer.hint.operator.HintType;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.plan.volcano.RelSubset;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.SemiJoin;
import org.apache.calcite.rel.logical.LogicalSemiJoin;

/**
 * Avoid semi join produces too much data on the right LV
 *
 * @author hongxi.chx
 */
public class LogicalSemiJoinToSemiBKAJoinRule extends RelOptRule {

    public static final LogicalSemiJoinToSemiBKAJoinRule INSTANCE = new LogicalSemiJoinToSemiBKAJoinRule(
        operand(LogicalSemiJoin.class,
            operand(RelSubset.class, any()),
            operand(LogicalView.class, null, RelOptUtil.NO_COLLATION_AND_DISTRIBUTION, any())), "INSTANCE");

    LogicalSemiJoinToSemiBKAJoinRule(RelOptRuleOperand operand, String desc) {
        super(operand, "LogicalSemiJoinToSemiBKAJoinRule:" + desc);
    }

    @Override
    public Convention getOutConvention() {
        return DrdsConvention.INSTANCE;
    }

    public static boolean enable(PlannerContext plannerContext) {
        return plannerContext.getParamManager().getBoolean(ConnectionParams.ENABLE_SEMI_BKA_JOIN);
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
        final RelNode rel = call.rel(0);
        if (!enable(PlannerContext.getPlannerContext(rel))) {
            return false;
        }

        if (rel instanceof SemiJoin) {
            final JoinRelType joinType = ((SemiJoin) rel).getJoinType();
            if (joinType == JoinRelType.SEMI || joinType == JoinRelType.LEFT) {
                return true;
            }
        }
        return false;
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        onMatchLogicalView(call);
    }

    private void onMatchLogicalView(RelOptRuleCall call) {
        final LogicalSemiJoin join = call.rel(0);
        RelNode left = call.rel(1);
        final LogicalView logicalView = call.rel(2);
        final RelTraitSet leftTraitSet;
        final RelTraitSet rightTraitSet;
        if (RelOptUtil.NO_COLLATION_AND_DISTRIBUTION.test(join)) {
            leftTraitSet = join.getCluster().getPlanner().emptyTraitSet().replace(DrdsConvention.INSTANCE);
            rightTraitSet = join.getCluster().getPlanner().emptyTraitSet().replace(DrdsConvention.INSTANCE);
        } else {
            leftTraitSet = join.getLeft().getTraitSet().replace(DrdsConvention.INSTANCE);
            rightTraitSet = join.getRight().getTraitSet().replace(DrdsConvention.INSTANCE);
        }

        left = convert(left, leftTraitSet);
        LogicalView right = logicalView.copy(rightTraitSet);

        SemiBKAJoin bkaJoin = SemiBKAJoin.create(
            join.getTraitSet().replace(DrdsConvention.INSTANCE),
            left,
            right,
            join.getCondition(),
            join.getVariablesSet(),
            join.getJoinType(),
            join.isSemiJoinDone(),
            ImmutableList.copyOf(join.getSystemFieldList()),
            join.getHints(), join);
        RelOptCost fixedCost = CheckJoinHint.check(join, HintType.CMD_SEMI_BKA_JOIN);
        if (fixedCost != null) {
            bkaJoin.setFixedCost(fixedCost);
        }
        right.setIsMGetEnabled(true);
        right.setJoin(bkaJoin);
        RelUtils.changeRowType(bkaJoin, join.getRowType());

        call.transformTo(bkaJoin);
    }
}