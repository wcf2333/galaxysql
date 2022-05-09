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

package com.alibaba.polardbx.executor.operator.util;

import com.alibaba.polardbx.executor.chunk.Block;
import com.alibaba.polardbx.executor.chunk.BlockBuilder;
import com.alibaba.polardbx.executor.chunk.BlockBuilders;
import com.alibaba.polardbx.executor.chunk.Chunk;
import com.alibaba.polardbx.executor.chunk.ChunkBuilder;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;

import java.util.ArrayList;
import java.util.List;

/**
 * Appendable buffer for arbitrary data types
 *
 * @author Eric Fu
 * @see ChunksIndex
 */
public class TypedBuffer {

    private BlockBuilder[] blockBuilders;
    private final int chunkSize;

    private int currentSize;
    private final List<Chunk> chunks = new ArrayList<>();
    private long estimateSize = 0;
    private ExecutionContext context;

    private TypedBuffer(BlockBuilder[] blockBuilders, int chunkSize, ExecutionContext context) {
        this.blockBuilders = blockBuilders;
        this.chunkSize = chunkSize;
        this.context = context;
    }

    public static TypedBuffer create(DataType[] dataTypes, int chunkSize, ExecutionContext context) {
        BlockBuilder[] blockBuilders = new BlockBuilder[dataTypes.length];
        for (int i = 0; i < dataTypes.length; i++) {
            blockBuilders[i] = BlockBuilders.create(dataTypes[i], context);
        }
        return new TypedBuffer(blockBuilders, chunkSize, context);
    }

    public boolean equals(int position, Chunk otherChunk, int otherPosition) {
        final int chunkId = chunkIndexOf(position);
        final int offset = offsetOf(position);

        if (chunkId < chunks.size()) {
            // Just compare both chunks
            Chunk chunk = chunks.get(chunkId);
            return chunk.equals(offset, otherChunk, otherPosition);
        } else {
            // Compare the block builders with given chunk (block by block)
            assert chunkId == chunks.size();
            for (int i = 0; i < blockBuilders.length; i++) {
                if (!otherChunk.getBlock(i).equals(otherPosition, blockBuilders[i], offset)) {
                    return false;
                }
            }
            return true;
        }
    }

    public void appendRow(Chunk chunk, int position) {
        // Check fulfilled before appending
        if (currentSize == chunkSize) {
            Chunk buildingChunk = getBuildingChunk();
            chunks.add(buildingChunk);
            estimateSize += buildingChunk.estimateSize();
            for (int i = 0; i < blockBuilders.length; i++) {
                blockBuilders[i] = blockBuilders[i].newBlockBuilder();
                currentSize = 0;
            }
        }

        for (int i = 0; i < blockBuilders.length; i++) {
            chunk.getBlock(i).writePositionTo(position, blockBuilders[i]);
        }
        currentSize++;
    }

    public List<Chunk> buildChunks() {
        ArrayList<Chunk> allChunks = new ArrayList<>(this.chunks);
        if (currentSize > 0) {
            allChunks.add(getBuildingChunk());
        }
        return allChunks;
    }

    private int chunkIndexOf(int position) {
        return position / chunkSize;
    }

    private int offsetOf(int position) {
        return position % chunkSize;
    }

    public void appendValuesTo(int position, ChunkBuilder chunkBuilder) {
        final int chunkId = chunkIndexOf(position);
        final int offset = offsetOf(position);
        if (chunkId < chunks.size()) {
            // Just compare both chunks
            Chunk chunk = chunks.get(chunkId);
            for (int i = 0; i < chunk.getBlockCount(); ++i) {
                final Block block = chunk.getBlock(i);
                chunkBuilder.appendTo(block, i, offset);
            }
        } else {
            // Compare the block builders with given chunk (block by block)
            assert chunkId == chunks.size();
            for (int i = 0; i < blockBuilders.length; i++) {
                Block block = blockBuilders[i].build();
                chunkBuilder.appendTo(block, i, offset);
            }
        }
    }

    private Chunk getBuildingChunk() {
        Block[] blocks = new Block[blockBuilders.length];
        for (int i = 0; i < blockBuilders.length; i++) {
            blocks[i] = blockBuilders[i].build();
        }
        return new Chunk(currentSize, blocks);
    }

    public long estimateSize() {
        return estimateSize;
    }
}
