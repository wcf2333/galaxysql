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

import com.alibaba.fastjson.JSON;
import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.spi.IRepository;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.dal.LogicalDal;
import com.alibaba.polardbx.rpc.CdcRpcClient;
import com.alibaba.polardbx.rpc.cdc.CdcServiceGrpc;
import com.alibaba.polardbx.rpc.cdc.RplCommandResponse;
import com.alibaba.polardbx.rpc.cdc.StopSlaveRequest;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.SqlStopSlave;

/**
 * @author shicai.xsc 2021/3/5 14:32
 * @desc
 * @since 5.0.0.0
 */
public class LogicalStopSlaveHandler extends LogicalReplicationBaseHandler {

    public LogicalStopSlaveHandler(IRepository repo){
        super(repo);
    }

    @Override
    public Cursor handle(RelNode logicalPlan, ExecutionContext executionContext) {
        LogicalDal dal = (LogicalDal) logicalPlan;
        SqlStopSlave sqlNode = (SqlStopSlave) dal.getNativeSqlNode();

        StopSlaveRequest request = StopSlaveRequest.newBuilder()
            .setRequest(JSON.toJSONString(sqlNode.getParams()))
            .build();

        final CdcServiceGrpc.CdcServiceBlockingStub blockingStub = CdcRpcClient.getCdcRpcClient().getCdcServiceBlockingStub();
        RplCommandResponse response = blockingStub.stopSlave(request);
        return handleRplCommandResponse(response, blockingStub.getChannel());
    }
}
