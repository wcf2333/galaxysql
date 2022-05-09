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

package com.alibaba.polardbx.executor.handler;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.cursor.impl.AffectRowCursor;
import com.alibaba.polardbx.executor.spi.IRepository;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.rpc.cdc.RplCommandResponse;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import io.grpc.Channel;
import io.grpc.ManagedChannel;
import org.apache.calcite.rel.RelNode;

/**
 * @author shicai.xsc 2021/3/5 15:07
 * @desc
 * @since 5.0.0.0
 */
public class LogicalReplicationBaseHandler extends HandlerCommon {

    public LogicalReplicationBaseHandler(IRepository repo) {
        super(repo);
    }

    @Override
    public Cursor handle(RelNode logicalPlan, ExecutionContext executionContext) {
        return null;
    }

    public Cursor handleRplCommandResponse(RplCommandResponse response, Channel channel) {
        if (channel instanceof ManagedChannel) {
            ((ManagedChannel)channel).shutdown();
        }

        if (response.getResultCode() == 0) {
            return new AffectRowCursor(new int[] {0});
        } else {
            throw new TddlRuntimeException(ErrorCode.ERR_REPLICATION_RESULT, String.format(response.getError()));
        }
    }
}