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

import com.alibaba.polardbx.executor.chunk.Chunk;
import com.alibaba.polardbx.executor.calc.AbstractAggregator;
import com.alibaba.polardbx.optimizer.state.LongGroupState;
import com.alibaba.polardbx.executor.chunk.Block;
import com.alibaba.polardbx.executor.chunk.BlockBuilder;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;

public class Long2LongSum0 extends AbstractAggregator {
    private LongGroupState groupState;

    public Long2LongSum0(int targetIndexes, boolean distinct, DataType inputType, DataType outputType, int filterArg) {
        super(new int[] {targetIndexes}, distinct, new DataType[] {inputType}, outputType, filterArg);
    }

    @Override
    public void open(int capacity) {
        groupState = new LongGroupState(capacity);
    }

    @Override
    public void appendInitValue() {
        groupState.append(0L);
    }

    @Override
    public void accumulate(int groupId, Chunk chunk, int position) {
        Block block = chunk.getBlock(aggIndexInChunk[0]);
        if (block.isNull(position)) {
            return;
        }

        long value = block.getLong(position);
        long beforeValue = groupState.get(groupId);
        long afterValue = beforeValue + value;
        groupState.set(groupId, afterValue);
    }

    @Override
    public void resetToInitValue(int groupId) {
        groupState.set(groupId, 0L);
    }

    @Override
    public void writeResultTo(int groupId, BlockBuilder bb) {
        bb.writeLong(groupState.get(groupId));
    }

    @Override
    public long estimateSize() {
        return groupState.estimateSize();
    }
}
