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

package com.alibaba.polardbx.executor.chunk;

import org.junit.Test;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class ByteArrayBlockTest extends BaseBlockTest {

    @Test
    public void test() {
        final byte[][] values = new byte[][] {
            "Hello!".getBytes(),
            new byte[] {(byte) 0xFF, 0x00},
            null,
            new byte[0],
        };

        ByteArrayBlockBuilder blockBuilder = new ByteArrayBlockBuilder(CHUNK_SIZE, 10);
        for (byte[] value : values) {
            if (value != null) {
                blockBuilder.writeByteArray(value);
            } else {
                blockBuilder.appendNull();
            }
        }

        assertEquals(values.length, blockBuilder.getPositionCount());
        for (int i = 0; i < values.length; i++) {
            if (values[i] != null) {
                assertFalse(blockBuilder.isNull(i));
                assertArrayEquals(values[i], blockBuilder.getByteArray(i));
            } else {
                assertTrue(blockBuilder.isNull(i));
            }
        }

        Block block = blockBuilder.build();

        assertEquals(values.length, block.getPositionCount());
        for (int i = 0; i < values.length; i++) {
            if (values[i] != null) {
                assertFalse(block.isNull(i));
                assertArrayEquals(values[i], block.getByteArray(i));
                assertTrue(block.equals(i, block, i));
            } else {
                assertTrue(block.isNull(i));
            }
        }

        BlockBuilder anotherBuilder = new ByteArrayBlockBuilder(CHUNK_SIZE, 10);

        for (int i = 0; i < values.length; i++) {
            block.writePositionTo(i, anotherBuilder);

            if (values[i] != null) {
                assertArrayEquals(values[i], anotherBuilder.getByteArray(i));
            } else {
                assertTrue(anotherBuilder.isNull(i));
            }
            assertTrue(block.equals(i, anotherBuilder, i));
            assertEquals(block.hashCode(i), anotherBuilder.hashCode(i));
        }
    }
}
