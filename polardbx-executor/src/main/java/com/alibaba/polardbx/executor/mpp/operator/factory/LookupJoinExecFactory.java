package com.alibaba.polardbx.executor.mpp.operator.factory;

import com.alibaba.polardbx.executor.operator.Executor;
import com.alibaba.polardbx.executor.operator.LookupJoinExec;
import com.alibaba.polardbx.executor.utils.ExecUtils;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.expression.calc.IExpression;
import com.alibaba.polardbx.optimizer.core.join.EquiJoinKey;
import com.alibaba.polardbx.optimizer.core.join.EquiJoinUtils;
import com.alibaba.polardbx.optimizer.core.rel.Gather;
import com.alibaba.polardbx.optimizer.core.rel.LogicalIndexScan;
import com.alibaba.polardbx.optimizer.core.rel.LogicalView;
import com.alibaba.polardbx.optimizer.core.rel.SemiBKAJoin;
import com.alibaba.polardbx.optimizer.utils.RexUtils;
import com.alibaba.polardbx.statistics.RuntimeStatHelper;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class LookupJoinExecFactory extends ExecutorFactory {
    private Join join;
    private List<EquiJoinKey> allJoinKeys; // including null-safe equal (`<=>`)
    private List<EquiJoinKey> joinKeys;

    public LookupJoinExecFactory(Join join, ExecutorFactory outerFactory, ExecutorFactory innerFactory) {
        this.join = join;
        this.allJoinKeys = EquiJoinUtils.buildEquiJoinKeys(join, join.getOuter(), join.getInner(),
            (RexCall) join.getCondition(), join.getJoinType(), true);
        this.joinKeys = allJoinKeys.stream().filter(k -> !k.isNullSafeEqual()).collect(Collectors.toList());
        addInput(innerFactory);
        addInput(outerFactory);
    }

    @Override
    public Executor createExecutor(ExecutionContext context, int index) {
        Executor ret;
        Executor inner;
        Executor outer = getInputs().get(1).createExecutor(context, index);

        inner = getInputs().get(0).createExecutor(context, index);
        IExpression otherCondition = convertExpression(join.getCondition(), context);
        ret = new LookupJoinExec(outer, inner, join.getJoinType(), joinKeys, allJoinKeys,
            otherCondition, context);

        ret.setId(join.getRelatedId());
        return ret;
    }

    private IExpression convertExpression(RexNode rexNode, ExecutionContext context) {
        return RexUtils.buildRexNode(rexNode, context, new ArrayList<>());
    }
}
