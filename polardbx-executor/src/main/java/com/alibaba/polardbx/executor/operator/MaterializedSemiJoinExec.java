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

import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.ListenableFuture;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.executor.chunk.Chunk;
import com.alibaba.polardbx.executor.operator.util.BufferInputBatchQueue;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.expression.calc.IExpression;
import com.alibaba.polardbx.optimizer.core.join.EquiJoinKey;
import com.alibaba.polardbx.optimizer.memory.MemoryAllocatorCtx;
import com.alibaba.polardbx.optimizer.memory.MemoryPool;
import com.alibaba.polardbx.optimizer.memory.MemoryPoolUtils;
import org.apache.calcite.rel.core.JoinRelType;

import java.util.List;

/**
 * Materialized Semi-Join Executor
 * inner 有可能比较复杂，outer一定是view
 */
public class MaterializedSemiJoinExec extends AbstractJoinExec implements ConsumerExecutor {

    private final int batchSize;
    private BufferInputBatchQueue bufferInputBatchQueue;
    private boolean distinctInput;

    private boolean isFinish;
    ListenableFuture<?> blocked;
    // equal with this outerInput
    private LookupTableExec lookUpExec;

    public MaterializedSemiJoinExec(Executor outerInput, Executor innerInput,
                                    boolean distinctInput, List<EquiJoinKey> joinKeys,
                                    JoinRelType joinType, IExpression condition,
                                    ExecutionContext context) {
        super(outerInput, innerInput, joinType, false, joinKeys, condition, null, null, context);
        createBlockBuilders();
        Preconditions.checkArgument(joinType == JoinRelType.SEMI || joinType == JoinRelType.ANTI);
        if (joinType == JoinRelType.SEMI) {
            this.batchSize = context.getParamManager().getInt(ConnectionParams.JOIN_BLOCK_SIZE);
        } else { // ANTI JOIN
            this.batchSize = Integer.MAX_VALUE;
        }
        this.distinctInput = distinctInput;
        this.blocked = ProducerExecutor.NOT_BLOCKED;

        Executor outerChild = outerInput;
        while (outerChild != null) {
            if (outerChild instanceof LookupTableExec) {
                this.lookUpExec = (LookupTableExec) outerChild;
                break;
            } else {
                outerChild = outerChild.getInputs().get(0);
            }
        }
    }

    private void innerResultIsEmpty() {
        if (joinType == JoinRelType.SEMI) {
            this.isFinish = true;
            // anti join
        } else {
            outerInput.open();
        }
    }

    @Override
    public void doOpen() {
        if (!isFinish) {
            Chunk inputChunk = bufferInputBatchQueue.pop();
            if (inputChunk == null) {
                innerResultIsEmpty();
                return;
            }
            Chunk lookupKeys = innerKeyChunkGetter.apply(inputChunk);
            lookUpExec.updateLookupPredicate(lookupKeys);
            outerInput.open();
        }
    }

    @Override
    public void openConsume() {
        bufferInputBatchQueue = new BufferInputBatchQueue(
            batchSize, innerInput.getDataTypes(), chunkLimit, context);
    }

    @Override
    public void consumeChunk(Chunk chunk) {
        if (distinctInput) {
            bufferInputBatchQueue.addDistinctChunk(chunk);
        } else {
            bufferInputBatchQueue.addChunk(chunk);
        }
    }

    @Override
    public void buildConsume() {
        if (distinctInput) {
            bufferInputBatchQueue.buildChunks();
        }
    }

    @Override
    void doClose() {
        closeConsume(true);
    }

    @Override
    Chunk doNextChunk() {
        if (isFinish) {
            return null;
        } else {
            Chunk result = outerInput.nextChunk();
            if (result != null) {
                return result;
            } else {
                if (lookUpExec.produceIsFinished()) {
                    Chunk chunk = bufferInputBatchQueue.pop();

                    if (chunk == null) {
                        this.isFinish = true;
                    } else {
                        Chunk lookupKeys = innerKeyChunkGetter.apply(chunk);
                        lookUpExec.updateLookupPredicate(lookupKeys);
                        lookUpExec.resume();
                        this.blocked = lookUpExec.produceIsBlocked();
                    }
                    return null;
                } else {
                    this.blocked = lookUpExec.produceIsBlocked();
                    return null;
                }
            }
        }
    }

    @Override
    public void closeConsume(boolean force) {
        innerInput.close();
        outerInput.close();
        bufferInputBatchQueue = null;
    }

    @Override
    public boolean produceIsFinished() {
        return isFinish;
    }

    @Override
    public ListenableFuture<?> produceIsBlocked() {
        return blocked;
    }
}
