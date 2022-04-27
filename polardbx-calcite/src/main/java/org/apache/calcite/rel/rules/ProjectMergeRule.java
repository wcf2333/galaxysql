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

import com.google.common.collect.Sets;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.core.RelFactories.ProjectFactory;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.tools.RelBuilderFactory;
import org.apache.calcite.util.Permutation;

import java.util.List;
import java.util.Set;

/**
 * ProjectMergeRule merges a {@link org.apache.calcite.rel.core.Project} into
 * another {@link org.apache.calcite.rel.core.Project},
 * provided the projects aren't projecting identical sets of input references.
 */
public class ProjectMergeRule extends RelOptRule {
    public static final ProjectMergeRule INSTANCE =
        new ProjectMergeRule(true, RelFactories.LOGICAL_BUILDER);

    //~ Instance fields --------------------------------------------------------

    /**
     * Whether to always merge projects.
     */
    private final boolean force;

    //~ Constructors -----------------------------------------------------------

    /**
     * Creates a ProjectMergeRule, specifying whether to always merge projects.
     *
     * @param force Whether to always merge projects
     */
    public ProjectMergeRule(boolean force, RelBuilderFactory relBuilderFactory) {
        super(
            operand(Project.class,
                operand(Project.class, any())),
            relBuilderFactory,
            "ProjectMergeRule" + (force ? ":force_mode" : ""));
        this.force = force;
    }

    @Deprecated 
    public ProjectMergeRule(boolean force, ProjectFactory projectFactory) {
        this(force, RelBuilder.proto(projectFactory));
    }

    //~ Methods ----------------------------------------------------------------

    public void onMatch(RelOptRuleCall call) {
        final Project topProject = call.rel(0);
        final Project bottomProject = call.rel(1);
        final RelBuilder relBuilder = call.builder();

        // If one or both projects are permutations, short-circuit the complex logic
        // of building a RexProgram.
        final Permutation topPermutation = topProject.getPermutation();
        if (topPermutation != null) {
            if (topPermutation.isIdentity()) {
                // Let ProjectRemoveRule handle this.
                return;
            }
            final Permutation bottomPermutation = bottomProject.getPermutation();
            if (bottomPermutation != null) {
                if (bottomPermutation.isIdentity()) {
                    // Let ProjectRemoveRule handle this.
                    return;
                }
                final Permutation product = topPermutation.product(bottomPermutation);
                relBuilder.push(bottomProject.getInput());
                relBuilder.project(relBuilder.fields(product),
                    topProject.getRowType().getFieldNames());
                RelNode output = relBuilder.build();
                RelCollation collation = topProject.getTraitSet().simplify().getCollation();
                RelDistribution distribution = topProject.getTraitSet().simplify().getDistribution();
                output = output.copy(output.getTraitSet().replace(collation).replace(distribution), output.getInputs());
                call.transformTo(output);
                return;
            }
        }

        // If we're not in force mode and the two projects reference identical
        // inputs, then return and let ProjectRemoveRule replace the projects.
        if (!force) {
            if (RexUtil.isIdentity(topProject.getProjects(),
                topProject.getInput().getRowType())) {
                return;
            }
        }

        final List<RexNode> newProjects =
            RelOptUtil.pushPastProject(topProject.getProjects(), bottomProject);
        final RelNode input = bottomProject.getInput();
        if (RexUtil.isIdentity(newProjects, input.getRowType())) {
            if (force
                || input.getRowType().getFieldNames()
                .equals(topProject.getRowType().getFieldNames())) {
                call.transformTo(input);
                return;
            }
        }

        Set<CorrelationId> corList = Sets.newHashSet();
        if (topProject.getVariablesSet() != null) {
            corList.addAll(topProject.getVariablesSet());
        }
        if (bottomProject.getVariablesSet() != null) {
            corList.addAll(bottomProject.getVariablesSet());
        }

        if (corList.size() > 1) {
            return;
        }
        // replace the two projects with a combined projection
        relBuilder.push(bottomProject.getInput());
        relBuilder.project(newProjects, topProject.getRowType().getFieldNames(), corList);
        RelNode output = relBuilder.build();
        RelCollation collation = topProject.getTraitSet().simplify().getCollation();
        RelDistribution distribution = topProject.getTraitSet().simplify().getDistribution();
        output =
            output.copy(output.getTraitSet().replace(collation).replace(distribution), output.getInputs());
        call.transformTo(output);
    }
}

// End ProjectMergeRule.java
