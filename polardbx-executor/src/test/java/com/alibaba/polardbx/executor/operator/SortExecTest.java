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

import com.alibaba.polardbx.common.properties.MppConfig;
import com.alibaba.polardbx.executor.operator.spill.AsyncFileSingleStreamSpillerFactory;
import com.alibaba.polardbx.executor.operator.spill.GenericSpillerFactory;
import com.alibaba.polardbx.executor.operator.spill.SpillerFactory;
import com.alibaba.polardbx.executor.operator.spill.SyncFileCleaner;
import com.alibaba.polardbx.executor.utils.OrderByOption;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.alibaba.polardbx.executor.chunk.Chunk;
import com.alibaba.polardbx.executor.chunk.IntegerBlock;
import com.alibaba.polardbx.executor.chunk.StringBlock;
import com.google.common.io.MoreFiles;
import com.google.common.io.RecursiveDeleteOption;
import org.apache.calcite.rel.RelFieldCollation;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

//
public class SortExecTest extends BaseExecTest {
    private static SpillerFactory spillerFactory;
    private static Path tempPath = Paths.get("./tmp/" + UUID.randomUUID());

    @BeforeClass
    public static void beforeClass() {
        List<Path> spillPaths = new ArrayList<>();
        spillPaths.add(tempPath);
        MppConfig.getInstance().getSpillPaths().clear();
        MppConfig.getInstance().getSpillPaths().addAll(spillPaths);
        AsyncFileSingleStreamSpillerFactory singleStreamSpillerFactory =
            new AsyncFileSingleStreamSpillerFactory(new SyncFileCleaner(), ImmutableList.of(tempPath), 2);
        spillerFactory = new GenericSpillerFactory(singleStreamSpillerFactory);
    }

    @AfterClass
    public static void afterClass() throws IOException {
        MoreFiles.deleteRecursively(tempPath, RecursiveDeleteOption.ALLOW_INSECURE);
    }

    @Test
    public void testIntegerMemSort() {
        MockExec input1 = MockExec.builder(DataTypes.IntegerType, DataTypes.IntegerType)
            .withChunk(new Chunk(IntegerBlock.of(null, 22, 5, 3), IntegerBlock.of(3, 3, 4, 9)))
            .build();

        OrderByOption orderByOption = new OrderByOption(0,
            RelFieldCollation.Direction.ASCENDING,
            RelFieldCollation.NullDirection.FIRST);

        List<OrderByOption> orderByOptions = Lists.newArrayList();
        orderByOptions.add(orderByOption);

        SortExec exec = new SortExec(input1.getDataTypes(), orderByOptions, context, spillerFactory);
        SingleExecTest test = new SingleExecTest.Builder(exec, input1).build();
        test.exec();
        assertExecResultByRow(test.result(),
            Collections.singletonList(new Chunk(IntegerBlock.of(null, 3, 5, 22), IntegerBlock.of(3, 9, 4, 3))), true);
    }

    @Test
    public void testIntegerWithNullMemSort() {
        MockExec input1 = MockExec.builder(DataTypes.IntegerType, DataTypes.IntegerType)
            .withChunk(new Chunk(IntegerBlock.of(null, 22, 5, 3, -1), IntegerBlock.of(3, 3, 4, 9, null)))
            .build();

        OrderByOption orderByOption = new OrderByOption(0,
            RelFieldCollation.Direction.ASCENDING,
            RelFieldCollation.NullDirection.FIRST);

        List<OrderByOption> orderByOptions = Lists.newArrayList();
        orderByOptions.add(orderByOption);

        SortExec exec = new SortExec(input1.getDataTypes(), orderByOptions, context, spillerFactory);
        SingleExecTest test = new SingleExecTest.Builder(exec, input1).build();
        test.exec();
        assertExecResultByRow(test.result(), Collections
            .singletonList(new Chunk(IntegerBlock.of(null, -1, 3, 5, 22), IntegerBlock.of(3, null, 9, 4, 3))), true);
    }

    @Test
    public void testInteger2ColMemSort() {
        MockExec input1 = MockExec.builder(DataTypes.IntegerType, DataTypes.IntegerType)
            .withChunk(new Chunk(IntegerBlock.of(null, 8, 5, 5), IntegerBlock.of(9, 4, 3, 2)))
            .withChunk(new Chunk(IntegerBlock.of(3, 6, 1), IntegerBlock.of(3, 3, 3)))
            .build();

        OrderByOption orderByOption1 = new OrderByOption(1,
            RelFieldCollation.Direction.DESCENDING,
            RelFieldCollation.NullDirection.UNSPECIFIED);
        OrderByOption orderByOption2 = new OrderByOption(0,
            RelFieldCollation.Direction.DESCENDING,
            RelFieldCollation.NullDirection.UNSPECIFIED);

        List<OrderByOption> orderByOptions = Lists.newArrayList();
        orderByOptions.add(orderByOption1);
        orderByOptions.add(orderByOption2);

        SortExec exec = new SortExec(input1.getDataTypes(), orderByOptions, context, spillerFactory);
        SingleExecTest test = new SingleExecTest.Builder(exec, input1).build();
        test.exec();
        assertExecResultByRow(test.result(), Collections.singletonList(
            new Chunk(IntegerBlock.of(null, 8, 6, 5, 3, 1, 5), IntegerBlock.of(9, 4, 3, 3, 3, 3, 2))), true);
    }

    @Test
    public void testInteger2ColWithDiffDirectionsMemSort() {
        MockExec input1 = MockExec.builder(DataTypes.IntegerType, DataTypes.IntegerType)
            .withChunk(new Chunk(IntegerBlock.of(null, 8, 5, 5), IntegerBlock.of(9, 9, 3, 2)))
            .withChunk(new Chunk(IntegerBlock.of(3, 6, 1), IntegerBlock.of(3, 3, 3)))
            .build();

        OrderByOption orderByOption1 = new OrderByOption(1,
            RelFieldCollation.Direction.DESCENDING,
            RelFieldCollation.NullDirection.UNSPECIFIED);
        OrderByOption orderByOption2 = new OrderByOption(0,
            RelFieldCollation.Direction.ASCENDING,
            RelFieldCollation.NullDirection.UNSPECIFIED);

        List<OrderByOption> orderByOptions = Lists.newArrayList();
        orderByOptions.add(orderByOption1);
        orderByOptions.add(orderByOption2);

        SortExec exec = new SortExec(input1.getDataTypes(), orderByOptions, context, spillerFactory);
        SingleExecTest test = new SingleExecTest.Builder(exec, input1).build();
        test.exec();

        assertExecResultByRow(test.result(), Collections.singletonList(
            new Chunk(IntegerBlock.of(null, 8, 1, 3, 5, 6, 5), IntegerBlock.of(9, 9, 3, 3, 3, 3, 2))), true);
    }

    @Test
    public void testInteger2ColWithDiffDirectionsAnd4InputsMergeSort() {
        MockExec input1 = MockExec.builder(DataTypes.IntegerType, DataTypes.IntegerType)
            .withChunk(new Chunk(IntegerBlock.of(null, 8, 9, 5), IntegerBlock.of(9, 4, 3, 2)))
            .withChunk(new Chunk(IntegerBlock.of(3, 6, 1), IntegerBlock.of(9, 4, 3)))
            .withChunk(new Chunk(IntegerBlock.of(null, 6, 15), IntegerBlock.of(null, null, null)))
            .withChunk(new Chunk(IntegerBlock.of(3, 6, null), IntegerBlock.of(96, 42, 33)))
            .build();

        OrderByOption orderByOption1 = new OrderByOption(1,
            RelFieldCollation.Direction.ASCENDING,
            RelFieldCollation.NullDirection.UNSPECIFIED);
        OrderByOption orderByOption2 = new OrderByOption(0,
            RelFieldCollation.Direction.DESCENDING,
            RelFieldCollation.NullDirection.UNSPECIFIED);

        List<OrderByOption> orderByOptions = Lists.newArrayList();
        orderByOptions.add(orderByOption1);
        orderByOptions.add(orderByOption2);

        SortExec exec = new SortExec(input1.getDataTypes(), orderByOptions, context, spillerFactory);
        SingleExecTest test = new SingleExecTest.Builder(exec, input1).build();
        test.exec();
        assertExecResultByRow(test.result(), Collections.singletonList(
            new Chunk(IntegerBlock.of(15, 6, null, 5, 9, 1, 8, 6, 3, null, null, 6, 3),
                IntegerBlock.of(null, null, null, 2, 3, 3, 4, 4, 9, 9, 33, 42, 96))), true);
    }

    @Test
    public void testIntegerMixString2ColWithDiffDirectionsAnd4InputsMemSort() {
        MockExec input1 = MockExec.builder(DataTypes.IntegerType, DataTypes.StringType)
            .withChunk(new Chunk(IntegerBlock.of(null, 8, 9, 5), StringBlock.of("9", "4", "3", "2")))
            .withChunk(new Chunk(IntegerBlock.of(null, 6, 15), StringBlock.of(null, "null", "NULL")))
            .withChunk(new Chunk(IntegerBlock.of(3, 6, null), StringBlock.of("96", "42", "33")))
            .withChunk(new Chunk(IntegerBlock.of(3, 6, 1), StringBlock.of("9", "4", "3")))
            .build();

        OrderByOption orderByOption1 = new OrderByOption(1,
            RelFieldCollation.Direction.DESCENDING,
            RelFieldCollation.NullDirection.UNSPECIFIED);
        OrderByOption orderByOption2 = new OrderByOption(0,
            RelFieldCollation.Direction.ASCENDING,
            RelFieldCollation.NullDirection.UNSPECIFIED);

        List<OrderByOption> orderByOptions = Lists.newArrayList();
        orderByOptions.add(orderByOption1);
        orderByOptions.add(orderByOption2);

        SortExec exec = new SortExec(input1.getDataTypes(), orderByOptions, context, spillerFactory);
        SingleExecTest test = new SingleExecTest.Builder(exec, input1).build();
        test.exec();
        assertExecResultByRow(test.result(), Collections.singletonList(
            new Chunk(IntegerBlock.of(6, 15, 3, null, 3, 6, 6, 8, null, 1, 9, 5, null),
                StringBlock.of("null", "NULL", "96", "9", "9", "42", "4", "4", "33", "3", "3", "2", null))), true);
    }

    @Test
    public void testIntegerMixStringSensitive2ColWithDiffDirectionsAnd4InputsMemSort() {
        MockExec input1 = MockExec.builder(DataTypes.IntegerType, DataTypes.SensitiveStringType)
            .withChunk(new Chunk(IntegerBlock.of(null, 8, 9, 5), StringBlock.of("A", "B", "3", "b")))
            .withChunk(new Chunk(IntegerBlock.of(null, 6, 15), StringBlock.of(null, "null", "NuLL")))
            .withChunk(new Chunk(IntegerBlock.of(3, 6, null), StringBlock.of("96", "42", "33")))
            .withChunk(new Chunk(IntegerBlock.of(3, 6, 1), StringBlock.of("a", "4", "3")))
            .build();

        OrderByOption orderByOption1 = new OrderByOption(1,
            RelFieldCollation.Direction.DESCENDING,
            RelFieldCollation.NullDirection.UNSPECIFIED);
        OrderByOption orderByOption2 = new OrderByOption(0,
            RelFieldCollation.Direction.ASCENDING,
            RelFieldCollation.NullDirection.UNSPECIFIED);

        List<OrderByOption> orderByOptions = Lists.newArrayList();
        orderByOptions.add(orderByOption1);
        orderByOptions.add(orderByOption2);

        SortExec exec = new SortExec(input1.getDataTypes(), orderByOptions, context, spillerFactory);
        SingleExecTest test = new SingleExecTest.Builder(exec, input1).build();
        test.exec();
        assertExecResultByRow(test.result(), Collections.singletonList(
            new Chunk(IntegerBlock.of(6, 5, 3, 15, 8, null, 3, 6, 6, null, 1, 9, null),
                StringBlock.of("null", "b", "a", "NuLL", "B", "A", "96", "42", "4", "33", "3", "3", null))), true);
    }

}
