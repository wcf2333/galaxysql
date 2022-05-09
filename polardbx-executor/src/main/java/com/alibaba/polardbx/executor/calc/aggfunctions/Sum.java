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

package com.alibaba.polardbx.executor.calc.aggfunctions;

import com.alibaba.polardbx.executor.chunk.Block;
import com.alibaba.polardbx.executor.chunk.BlockBuilder;
import com.alibaba.polardbx.executor.chunk.Chunk;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.executor.calc.AbstractAggregator;
import com.alibaba.polardbx.optimizer.state.NullableObjectGroupState;

/**
 * Created by chuanqin on 17/8/10.
 */
public class Sum extends AbstractAggregator {
    private NullableObjectGroupState groupState;

    public Sum(int targetIndexes, boolean distinct, DataType returnType, int filterArg) {
        super(new int[] {targetIndexes}, distinct, null, returnType, filterArg);
    }

    @Override
    public void open(int capacity) {
        groupState = new NullableObjectGroupState(capacity);
    }

    @Override
    public void accumulate(int groupId, Chunk inputChunk, int position) {
        Block block = inputChunk.getBlock(aggIndexInChunk[0]);
        if (block.isNull(position)) {
            return;
        }

        final Object value = block.getObject(position);
        if (groupState.isNull(groupId)) {
            groupState.set(groupId, returnType.convertFrom(value));
        } else {
            Object oldValue = groupState.get(groupId);
            groupState.set(groupId, returnType.getCalculator().add(oldValue, value));
        }
    }

    @Override
    public void writeResultTo(int groupId, BlockBuilder bb) {
        if (groupState.isNull(groupId)) {
            bb.appendNull();
        } else {
            Object sum = groupState.get(groupId);
            bb.writeObject(sum);
        }
    }

    @Override
    public void appendInitValue() {
        groupState.appendNull();
    }

    @Override
    public void resetToInitValue(int groupId) {
        groupState.setNull(groupId);
    }

    @Override
    public long estimateSize() {
        return groupState.estimateSize();
    }
}
