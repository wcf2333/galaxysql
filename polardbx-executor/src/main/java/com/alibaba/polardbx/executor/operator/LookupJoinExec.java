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

import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.executor.chunk.Chunk;
import com.alibaba.polardbx.executor.chunk.ChunkConverter;
import com.alibaba.polardbx.executor.chunk.Converters;
import com.alibaba.polardbx.executor.operator.util.BufferInputBatchQueue;
import com.alibaba.polardbx.executor.operator.util.ChunksIndex;
import com.alibaba.polardbx.executor.operator.util.ConcurrentRawHashTable;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.expression.calc.IExpression;
import com.alibaba.polardbx.optimizer.core.join.EquiJoinKey;
import com.google.common.util.concurrent.ListenableFuture;
import org.apache.calcite.rel.core.JoinRelType;

import java.util.Arrays;
import java.util.List;

import static com.alibaba.polardbx.executor.operator.AbstractHashJoinExec.LIST_END;
import static com.alibaba.polardbx.executor.utils.ExecUtils.buildOneChunk;

/**
 * Lookup Join (aka. BKA Join)
 * <p>
 * Note: outer 有可能比较复杂，inner 一定是view
 */
public class LookupJoinExec extends AbstractBufferedJoinExec {

    int batchSize;

    LookupTableExec lookupTableExec;
    final ChunkConverter allOuterKeyChunkGetter;

    BufferInputBatchQueue batchQueue;
    Chunk saveProbeChunk;
    ListenableFuture<?> blocked;
    boolean isFinish;

    ConcurrentRawHashTable hashTable;
    int[] positionLinks;

    LookupJoinStatus status = LookupJoinStatus.CONSUMING_OUTER;

    public LookupJoinExec(Executor outerInput, Executor innerInput,
                          JoinRelType joinType,
                          List<EquiJoinKey> joinKeys, List<EquiJoinKey> allJoinKeys, IExpression otherCondition,
                          ExecutionContext context) {
        super(outerInput, innerInput, joinType, false, joinKeys, otherCondition, null, null, context);
        getInnerLookupTableExec();

        this.blocked = ProducerExecutor.NOT_BLOCKED;

        DataType[] keyColumnTypes = allJoinKeys.stream().map(t -> t.getUnifiedType()).toArray(DataType[]::new);
        int[] outerKeyColumns = allJoinKeys.stream().mapToInt(t -> t.getOuterIndex()).toArray();
        this.allOuterKeyChunkGetter =
            Converters.createChunkConverter(
                outerInput.getDataTypes(), outerKeyColumns, keyColumnTypes, context);

        this.streamJoin = true;
        batchSize = context.getParamManager().getInt(ConnectionParams.JOIN_BLOCK_SIZE);
    }

    private void getInnerLookupTableExec() {
        Executor innerChild = innerInput;
        while (innerChild != null) {
            if (innerChild instanceof LookupTableExec) {
                lookupTableExec = (LookupTableExec) innerChild;
                break;
            } else {
                innerChild = innerChild.getInputs().get(0);
            }
        }
    }

    @Override
    public void doOpen() {
        createBlockBuilders();
        this.batchQueue = new BufferInputBatchQueue(batchSize, outerInput.getDataTypes(), context);
        this.outerInput.open();
    }

    @Override
    Chunk doNextChunk() {

        if (isFinish) {
            return null;
        }

        switch (status) {
        case CONSUMING_OUTER:
            Chunk outerChunk = outerInput.nextChunk();
            if (outerChunk != null) {
                batchQueue.addChunk(outerChunk);
                if (batchQueue.getTotalRowCount().get() > batchSize) {
                    status = LookupJoinStatus.INIT_INNER_LOOKUP;
                    blocked = ProducerExecutor.NOT_BLOCKED;
                }
            } else {
                if (outerInput.produceIsFinished()) {
                    outerInput.close();
                    status = LookupJoinStatus.INIT_INNER_LOOKUP;
                    blocked = ProducerExecutor.NOT_BLOCKED;
                } else {
                    blocked = outerInput.produceIsBlocked();
                }
            }
            return null;
        case INIT_INNER_LOOKUP:
            buildChunks = new ChunksIndex();
            buildKeyChunks = new ChunksIndex();
            saveProbeChunk = batchQueue.pop();
            if (saveProbeChunk != null) {
                Chunk allJoinKeys = allOuterKeyChunkGetter.apply(saveProbeChunk);
                getLookupTableExec().updateLookupPredicate(allJoinKeys);
                getLookupTableExec().open();
                status = LookupJoinStatus.CACHE_INNER_RESULT;
            } else {
                isFinish = true;
            }
            return null;
        case CACHE_INNER_RESULT:
            Chunk result = innerInput.nextChunk();
            if (result != null) {
                buildChunks.addChunk(result);
                buildKeyChunks.addChunk(innerKeyChunkGetter.apply(result));
                this.blocked = ProducerExecutor.NOT_BLOCKED;
            } else {
                if (getLookupTableExec().produceIsFinished()) {
                    status = LookupJoinStatus.BUILD_HASH_TABLE;
                    this.blocked = ProducerExecutor.NOT_BLOCKED;
                } else {
                    this.blocked = getLookupTableExec().produceIsBlocked();
                }
            }
            return null;
        case BUILD_HASH_TABLE:
            buildHashTable();
            status = LookupJoinStatus.PROBE_AND_OUTPUT;
            return null;
        case PROBE_AND_OUTPUT:
            Chunk ret = super.doNextChunk();
            if (ret == null) {
                if (outerInput.produceIsFinished()) {
                    isFinish = true;
                } else {
                    status = LookupJoinStatus.CONSUMING_OUTER;
                }
            }
            return ret;
        default:
            throw new RuntimeException("not expected status: " + status);
        }
    }

    protected LookupTableExec getLookupTableExec() {
        return lookupTableExec;
    }

    @Override
    Chunk nextProbeChunk() {
        Chunk ret = saveProbeChunk;
        saveProbeChunk = null;
        return ret;
    }

    void buildHashTable() {
        final int size = buildKeyChunks.getPositionCount();
        hashTable = new ConcurrentRawHashTable(size);
        positionLinks = new int[size];
        Arrays.fill(positionLinks, LIST_END);
        int position = 0;
        for (int chunkId = 0; chunkId < buildKeyChunks.getChunkCount(); ++chunkId) {
            final Chunk keyChunk = buildKeyChunks.getChunk(chunkId);
            buildOneChunk(keyChunk, position, hashTable, positionLinks, null, getIgnoreNullsInJoinKey());
            position += keyChunk.getPositionCount();
        }
        assert position == size;
    }

    @Override
    int matchInit(Chunk keyChunk, int[] hashCodes, int position) {
        int hashCode = hashCodes[position];

        int matchedPosition = hashTable.get(hashCode);
        while (matchedPosition != LIST_END) {
            if (buildKeyChunks.equals(matchedPosition, keyChunk, position)) {
                break;
            }
            matchedPosition = positionLinks[matchedPosition];
        }
        return matchedPosition;
    }

    @Override
    int matchNext(int current, Chunk keyChunk, int position) {
        int matchedPosition = positionLinks[current];
        while (matchedPosition != LIST_END) {
            if (buildKeyChunks.equals(matchedPosition, keyChunk, position)) {
                break;
            }
            matchedPosition = positionLinks[matchedPosition];
        }
        return matchedPosition;
    }

    @Override
    boolean matchValid(int current) {
        return current != LIST_END;
    }

    @Override
    void doClose() {
        // Hash Join
        hashTable = null;
        positionLinks = null;
        buildChunks = null;
        buildKeyChunks = null;

        innerInput.close();
        outerInput.close();
        batchQueue = null;
        isFinish = true;
        saveProbeChunk = null;
    }

    @Override
    public boolean produceIsFinished() {
        return isFinish;
    }

    @Override
    public ListenableFuture<?> produceIsBlocked() {
        return blocked;
    }

    protected enum LookupJoinStatus {
        CONSUMING_OUTER,
        INIT_INNER_LOOKUP,
        CACHE_INNER_RESULT,
        BUILD_HASH_TABLE,
        PROBE_AND_OUTPUT
    }
}
