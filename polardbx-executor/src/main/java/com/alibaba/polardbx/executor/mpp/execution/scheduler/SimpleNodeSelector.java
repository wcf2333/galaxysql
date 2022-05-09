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

package com.alibaba.polardbx.executor.mpp.execution.scheduler;

import com.alibaba.polardbx.common.partition.MurmurHashUtils;
import com.alibaba.polardbx.executor.mpp.split.OssSplit;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.executor.mpp.execution.NodeTaskMap;
import com.alibaba.polardbx.executor.mpp.execution.RemoteTask;
import com.alibaba.polardbx.executor.mpp.metadata.Split;
import com.alibaba.polardbx.gms.node.InternalNode;
import com.alibaba.polardbx.gms.node.InternalNodeManager;
import com.alibaba.polardbx.gms.node.Node;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class SimpleNodeSelector implements NodeSelector {

    private static final Logger log = LoggerFactory.getLogger(SimpleNodeSelector.class);

    private final InternalNodeManager nodeManager;
    private final NodeTaskMap nodeTaskMap;
    private final int limitCandidates;
    private final int maxSplitsPerNode;
    private final boolean enableOssRoundRobin;
    private final List<Node> workerNodes;

    public SimpleNodeSelector(InternalNodeManager nodeManager, NodeTaskMap nodeTaskMap, Set<InternalNode> nodes,
                              int limitCandidates, int maxSplitsPerNode, boolean enableOssRoundRobin) {
        this.nodeManager = requireNonNull(nodeManager, "nodeManager is null");
        this.nodeTaskMap = requireNonNull(nodeTaskMap, "nodeTaskMap is null");
        this.limitCandidates = limitCandidates;
        this.maxSplitsPerNode = maxSplitsPerNode;
        this.workerNodes = selectSuitableNodes(limitCandidates, nodes);
        this.enableOssRoundRobin = enableOssRoundRobin;
    }

    private <T extends Node> List<Node> selectSuitableNodes(int limit, Collection<T> internalNodes) {
        checkArgument(limit > 0, "limit must be at least 1");
        Iterator<T> candidates = new ResettableRandomizedIterator<T>(internalNodes);
        List<Node> selected = new ArrayList<>(limit);
        while (selected.size() < limit && candidates.hasNext()) {
            Node node = candidates.next();
            if (node.isWorker()) {
                selected.add(node);
            }
        }
        return selected;
    }

    @Override
    public Node selectCurrentNode() {
        return nodeManager.getCurrentNode();
    }

    @Override
    public List<Node> selectRandomNodes(int limit) {
        return selectSuitableNodes(limit, workerNodes);
    }

    private List<Node> selectNodes(int limit, Iterator<Node> candidates, NodeAssignmentStats assignmentStats) {
        checkArgument(limit > 0, "limit must be at least 1");
        List<Node> selected = new ArrayList<>(limit);
        while (selected.size() < limit && candidates.hasNext()) {
            Node ownerNode = candidates.next();
            if (assignmentStats.getTotalSplitCount(ownerNode) < maxSplitsPerNode || maxSplitsPerNode == 0) {
                selected.add(ownerNode);
            }
        }
        return selected;
    }

    @Override
    public Multimap<Node, Split> computeAssignments(List<Split> splits, List<RemoteTask> existingTasks) {
        NodeAssignmentStats assignmentStats = new NodeAssignmentStats(nodeTaskMap, workerNodes, existingTasks);

        ResettableRandomizedIterator<Node> randomCandidates = new ResettableRandomizedIterator<>(workerNodes);
        List<Node> candidateNodes = selectNodes(limitCandidates, randomCandidates, assignmentStats);
        if (candidateNodes.isEmpty()) {
            log.error(String.format("No nodes available to schedule."));
            throw new TddlRuntimeException(ErrorCode.ERR_NO_NODES_AVAILABLE, "No nodes available to run query");
        }

        if (splits.stream().allMatch(x -> x.getConnectorSplit() instanceof OssSplit)) {
            // split affinity
            return OssSplitAffinityAssigment(splits, candidateNodes, assignmentStats);
        }

        Multimap<Node, Split> assignment = HashMultimap.create();
        ResettableRandomizedIterator<Node> nodeIterator = new ResettableRandomizedIterator(candidateNodes);
        for (Split split : splits) {
            if (!nodeIterator.hasNext()) {
                nodeIterator.reset();
            }
            Node chosenNode = nodeIterator.next();
            if (chosenNode != null) {
                assignment.put(chosenNode, split);
                assignmentStats.addAssignedSplit(chosenNode);
            } else {
                throw new TddlRuntimeException(ErrorCode.ERR_NO_NODES_AVAILABLE, "No nodes available to run query");
            }
        }
        return assignment;
    }

    private Multimap<Node, Split> OssSplitAffinityAssigment(List<Split> splits, List<Node> candidateNodes,
                                                            NodeAssignmentStats assignmentStats) {
        Multimap<Node, Split> assignment = HashMultimap.create();
        candidateNodes.sort((a, b) -> a.getNodeIdentifier().compareTo(b.getNodeIdentifier()));

        final boolean allSplitFileCurrent = splits
            .stream()
            .allMatch(split -> ((OssSplit) split.getConnectorSplit()).getDesignatedFile() != null);
        if (enableOssRoundRobin && allSplitFileCurrent) {
            // use round robin for oss query
            int currentId = 0;
            for (Split split : splits) {
                int position = (currentId++) % candidateNodes.size();
                doAssign(candidateNodes, assignmentStats, assignment, split, position);
            }
            return assignment;
        }

        for (Split split : splits) {
            long hashCode;
            if (((OssSplit) split.getConnectorSplit()).getDesignatedFile() != null) {
                hashCode = ((OssSplit) split.getConnectorSplit()).getDesignatedFile().hashCode();
                hashCode = MurmurHashUtils.murmurHashWithZeroSeed(hashCode);
            } else {
                List<String> phyTableNameList = ((OssSplit) split.getConnectorSplit()).getPhyTableNameList();
                hashCode = phyTableNameList.stream().map(x -> x.hashCode()).reduce(31, (a, b) -> a + b).longValue();
                hashCode = MurmurHashUtils.murmurHashWithZeroSeed(hashCode);
            }

            int position = (int) hashCode % candidateNodes.size();
            if (position < 0) {
                position += candidateNodes.size();
            }
            doAssign(candidateNodes, assignmentStats, assignment, split, position);
        }
        return assignment;
    }

    private void doAssign(List<Node> candidateNodes, NodeAssignmentStats assignmentStats,
                          Multimap<Node, Split> assignment, Split split, int position) {
        Node chosenNode = candidateNodes.get(position);
        if (chosenNode != null) {
            assignment.put(chosenNode, split);
            assignmentStats.addAssignedSplit(chosenNode);
        } else {
            throw new TddlRuntimeException(ErrorCode.ERR_NO_NODES_AVAILABLE, "No nodes available to run query");
        }
    }
}
