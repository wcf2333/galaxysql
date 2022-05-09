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

package com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement;

import com.alibaba.polardbx.druid.sql.ast.SQLExpr;
import com.alibaba.polardbx.druid.sql.ast.SQLName;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLAlterStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.visitor.MySqlASTVisitor;

/**
 * @author chenzilin
 */
public class DrdsAlterFileStorageStatement extends MySqlStatementImpl implements SQLAlterStatement {
    private SQLName name;
    private SQLExpr timestamp;
    private boolean purgeBefore;
    private boolean asOf;
    private boolean backup;

    public SQLName getName() {
        return name;
    }

    public void setName(SQLName name) {
        if (name != null) {
            name.setParent(this);
        }
        this.name = name;
    }

    public SQLExpr getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(SQLExpr timestamp) {
        if (timestamp != null) {
            timestamp.setParent(this);
        }
        this.timestamp = timestamp;
    }

    public boolean isPurgeBefore() {
        return purgeBefore;
    }

    public void setPurgeBefore(boolean purgeBefore) {
        this.purgeBefore = purgeBefore;
    }

    public boolean isAsOf() {
        return asOf;
    }

    public void setAsOf(boolean asOf) {
        this.asOf = asOf;
    }

    public boolean isBackup() {
        return backup;
    }

    public void setBackup(boolean backup) {
        this.backup = backup;
    }

    public void accept0(MySqlASTVisitor visitor) {
        if (visitor.visit(this)) {
            acceptChild(visitor, name);
        }
        visitor.endVisit(this);
    }
}