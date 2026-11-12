/*+*****************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2026 QuestDB
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 ******************************************************************************/

package io.questdb.client.test.cutlass.qwp.client;

import io.questdb.client.cutlass.qwp.client.QwpColumnBatch;
import io.questdb.client.cutlass.qwp.client.QwpColumnBatchHandler;
import io.questdb.client.cutlass.qwp.client.QwpQueryDetails;
import io.questdb.client.cutlass.qwp.client.QwpServerInfo;
import org.junit.Assert;

import java.util.ArrayList;
import java.util.List;

/**
 * Records what the most detailed callbacks of {@link QwpColumnBatchHandler}
 * receive, copying the {@link QwpQueryDetails} values out while they are
 * valid. The simpler overloads fail: the client must call the detailed ones.
 * Batches must carry a single LONG column.
 */
public class RecordingQueryHandler implements QwpColumnBatchHandler {
    public final List<Long> values = new ArrayList<>();
    public int batchCount;
    public long clientWaitNanos;
    public long compileNanos;
    public long countNanos;
    public int endCount;
    public long endRequestId;
    public int errorCount;
    public String errorMessage;
    public int errorPosition;
    public long errorRequestId;
    public byte errorStatus;
    public int execDoneCount;
    public long executeNanos;
    public int failoverResetCount;
    public boolean hasRowCount;
    public boolean hasStats;
    public boolean isJitCompiled;
    public short opType;
    public long rowCount;
    public long rowsAffected;
    public long totalRows;

    public void assertNoDetails() {
        Assert.assertFalse("row count", hasRowCount);
        Assert.assertEquals(-1, rowCount);
        Assert.assertFalse("stats", hasStats);
        Assert.assertEquals(-1, compileNanos);
        Assert.assertEquals(-1, executeNanos);
        Assert.assertEquals(-1, countNanos);
        Assert.assertEquals(-1, clientWaitNanos);
        Assert.assertFalse(isJitCompiled);
    }

    public void assertOnlyError() {
        Assert.assertEquals("errors", 1, errorCount);
        Assert.assertEquals("ends", 0, endCount);
        Assert.assertEquals("exec dones", 0, execDoneCount);
    }

    @Override
    public void onBatch(QwpColumnBatch batch) {
        batchCount++;
        for (int row = 0; row < batch.getRowCount(); row++) {
            values.add(batch.getLongValue(0, row));
        }
    }

    @Override
    public void onEnd(long totalRows) {
        Assert.fail("the client must call onEnd(requestId, totalRows, details)");
    }

    @Override
    public void onEnd(long requestId, long totalRows, QwpQueryDetails details) {
        endCount++;
        endRequestId = requestId;
        this.totalRows = totalRows;
        copy(details);
    }

    @Override
    public void onError(byte status, String message) {
        Assert.fail("the client must call onError(requestId, status, message, position)");
    }

    @Override
    public void onError(long requestId, byte status, String message, int position) {
        errorCount++;
        errorRequestId = requestId;
        errorStatus = status;
        errorMessage = message;
        errorPosition = position;
    }

    @Override
    public void onExecDone(short opType, long rowsAffected) {
        Assert.fail("the client must call onExecDone(requestId, opType, rowsAffected, details)");
    }

    @Override
    public void onExecDone(long requestId, short opType, long rowsAffected, QwpQueryDetails details) {
        execDoneCount++;
        this.opType = opType;
        this.rowsAffected = rowsAffected;
        copy(details);
    }

    @Override
    public void onFailoverReset(long requestId, QwpServerInfo newNode) {
        failoverResetCount++;
        values.clear();
    }

    private void copy(QwpQueryDetails details) {
        Assert.assertNotNull("details must never be null", details);
        hasRowCount = details.hasRowCount();
        rowCount = details.getRowCount();
        hasStats = details.hasStats();
        compileNanos = details.getCompileNanos();
        executeNanos = details.getExecuteNanos();
        countNanos = details.getCountNanos();
        clientWaitNanos = details.getClientWaitNanos();
        isJitCompiled = details.isJitCompiled();
    }
}
