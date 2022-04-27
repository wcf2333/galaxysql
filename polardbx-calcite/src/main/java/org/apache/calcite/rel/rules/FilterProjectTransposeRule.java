/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.calcite.rel.rules;

import com.google.common.collect.ImmutableSet;
import org.apache.calcite.plan.RelOptPredicateList;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexOver;
import org.apache.calcite.rex.RexSimplify;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.tools.RelBuilderFactory;

/**
 * Planner rule that pushes
 * a {@link org.apache.calcite.rel.logical.LogicalFilter}
 * past a {@link org.apache.calcite.rel.logical.LogicalProject}.
 */
public class FilterProjectTransposeRule extends RelOptRule {
  /** The default instance of
   * {@link org.apache.calcite.rel.rules.FilterProjectTransposeRule}.
   *
   * <p>It matches any kind of join or filter, and generates the same kind of
   * join and filter. */
  public static final FilterProjectTransposeRule INSTANCE =
      new FilterProjectTransposeRule(Filter.class, Project.class, true, true,
          RelFactories.LOGICAL_BUILDER);

  private final boolean copyFilter;
  private final boolean copyProject;

  //~ Constructors -----------------------------------------------------------

  /**
   * Creates a FilterProjectTransposeRule.
   *
   * <p>If {@code filterFactory} is null, creates the same kind of filter as
   * matched in the rule. Similarly {@code projectFactory}.</p>
   */
  public FilterProjectTransposeRule(
      Class<? extends Filter> filterClass,
      Class<? extends Project> projectClass,
      boolean copyFilter, boolean copyProject,
      RelBuilderFactory relBuilderFactory) {
    this(
        operand(filterClass,
            operand(projectClass, any())),
        copyFilter, copyProject, relBuilderFactory);
  }

  @Deprecated 
  public FilterProjectTransposeRule(
      Class<? extends Filter> filterClass,
      RelFactories.FilterFactory filterFactory,
      Class<? extends Project> projectClass,
      RelFactories.ProjectFactory projectFactory) {
    this(filterClass, projectClass, filterFactory == null,
        projectFactory == null,
        RelBuilder.proto(filterFactory, projectFactory));
  }

  protected FilterProjectTransposeRule(
      RelOptRuleOperand operand,
      boolean copyFilter,
      boolean copyProject,
      RelBuilderFactory relBuilderFactory) {
    super(operand, relBuilderFactory, null);
    this.copyFilter = copyFilter;
    this.copyProject = copyProject;
  }

  //~ Methods ----------------------------------------------------------------

  public void onMatch(RelOptRuleCall call) {
    final Filter filter = call.rel(0);
    final Project project = call.rel(1);

    if (RexOver.containsOver(project.getProjects(), null)) {
      // In general a filter cannot be pushed below a windowing calculation.
      // Applying the filter before the aggregation function changes
      // the results of the windowing invocation.
      //
      // When the filter is on the PARTITION BY expression of the OVER clause
      // it can be pushed down. For now we don't support this.
      return;
    }

    if (filter.getVariablesSet().size() > 0 && project.getVariablesSet().size() > 0 && !filter.getVariablesSet()
        .containsAll(project.getVariablesSet())) {
      return;
    }

    if (RexUtil.containsCorrelation(filter.getCondition())
        || (null != filter.getVariablesSet() && filter.getVariablesSet().size() > 0)) {
      // If there is a correlation condition anywhere in the filter, don't
      // push this filter past project since in some cases it can prevent a
      // Correlate from being de-correlated.
      return;
    }

    // convert the filter to one that references the child of the project
    RexNode newCondition =
        RelOptUtil.pushPastProject(filter.getCondition(), project);
    RexUtil.DynamicFinder dynamicFinder = new RexUtil.DynamicFinder();
    newCondition.accept(dynamicFinder);
    if ((dynamicFinder.getScalar().size() > 0 &&
        project.getVariablesSet() != null &&
        project.getVariablesSet().size() > 0) ||
        dynamicFinder.getCorrelateScalar().size() > 0) {
      return;
    }

    final RelBuilder relBuilder = call.builder();
    RelNode newFilterRel;
    if (copyFilter) {
      final RelOptPredicateList predicates = RelOptPredicateList.EMPTY;
      final RexSimplify simplify =
          new RexSimplify(relBuilder.getRexBuilder(), predicates, false,
              RexUtil.EXECUTOR);
      newCondition = simplify.removeNullabilityCast(newCondition);
      newFilterRel = filter.copy(filter.getTraitSet(), project.getInput(),
          newCondition);
    } else {
      newFilterRel =
          relBuilder.push(project.getInput()).filter(newCondition).build();
    }
    newFilterRel = LogicalFilter.create(((LogicalFilter) newFilterRel).getInput(), newCondition,
        ImmutableSet.<CorrelationId>builder().addAll(project.getVariablesSet()).build());

    RelNode newProjRel =
        copyProject
            ? project.copy(project.getTraitSet(), newFilterRel,
                project.getProjects(), project.getRowType())
            : relBuilder.push(newFilterRel)
                .project(project.getProjects(), project.getRowType().getFieldNames())
                .build();

    call.transformTo(newProjRel);
  }
}

// End FilterProjectTransposeRule.java
