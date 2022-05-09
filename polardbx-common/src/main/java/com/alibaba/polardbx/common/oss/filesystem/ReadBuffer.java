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

package com.alibaba.polardbx.common.oss.filesystem;

import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

public class ReadBuffer {
    enum STATUS {
        INIT, SUCCESS, ERROR
    }
    private final ReentrantLock lock = new ReentrantLock();

    private Condition readyCondition = lock.newCondition();

    private byte[] buffer;
    private STATUS status;
    private long byteStart;
    private long byteEnd;

    public ReadBuffer(long byteStart, long byteEnd) {
        this.buffer = new byte[(int)(byteEnd - byteStart) + 1];

        this.status = STATUS.INIT;
        this.byteStart = byteStart;
        this.byteEnd = byteEnd;
    }

    public void lock() {
        lock.lock();
    }

    public void unlock() {
        lock.unlock();
    }

    public void await(STATUS waitStatus) throws InterruptedException {
        while (this.status == waitStatus) {
            readyCondition.await();
        }
    }

    public void signalAll() {
        readyCondition.signalAll();
    }

    public byte[] getBuffer() {
        return buffer;
    }

    public STATUS getStatus() {
        return status;
    }

    public void setStatus(STATUS status) {
        this.status = status;
    }

    public long getByteStart() {
        return byteStart;
    }

    public long getByteEnd() {
        return byteEnd;
    }

    @Override
    public String toString() {
        return String.format(
            "{interval = [%s, %s], len = %s}",
            byteStart,
            byteEnd,
            byteEnd - byteStart
        );
    }
}