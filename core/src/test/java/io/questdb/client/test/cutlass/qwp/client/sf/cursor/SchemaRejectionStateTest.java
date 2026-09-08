/*******************************************************************************
 * Copyright (c) 2014-2026 QuestDB
 * Licensed under the Apache License, Version 2.0.
 ******************************************************************************/

package io.questdb.client.test.cutlass.qwp.client.sf.cursor;

import io.questdb.client.LineSenderServerException;
import io.questdb.client.SenderError;
import io.questdb.client.cutlass.qwp.client.sf.cursor.SchemaRejectionState;
import io.questdb.client.cutlass.qwp.client.sf.cursor.CursorSendEngine;
import io.questdb.client.cutlass.qwp.protocol.QwpConstants;
import io.questdb.client.std.MemoryTag;
import io.questdb.client.std.Unsafe;
import org.junit.Test;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;

import static org.junit.Assert.*;

public class SchemaRejectionStateTest {
    @Rule
    public final TemporaryFolder temp = new TemporaryFolder();

    @Test
    public void testRecoveredGroupRetiresThroughCloserRegardlessOfNewLeaseMode() throws Exception {
        String path = temp.newFolder("recovered-group").getAbsolutePath();
        try (CursorSendEngine original = new CursorSendEngine(path, 4096)) {
            append(original, true);
            append(original, true);
            append(original, false);
            append(original, false);
        }
        try (CursorSendEngine recovered = new CursorSendEngine(path, 4096)) {
            SchemaRejectionState state = new SchemaRejectionState();
            state.setEngine(recovered);
            state.beginLease(1, 4, false);
            append(recovered, false);
            assertTrue(state.reject(1, 0, error(1)));
            assertEquals(2, state.sealedRange().lastFsn);
            assertFalse(state.hasOwnedFailure(1));
        }
    }

    @Test
    public void testRecoveredOpenTailCannotUseNewProducerCloser() throws Exception {
        String path = temp.newFolder("recovered-tail").getAbsolutePath();
        try (CursorSendEngine original = new CursorSendEngine(path, 4096)) {
            append(original, true);
            append(original, true);
        }
        try (CursorSendEngine recovered = new CursorSendEngine(path, 4096)) {
            SchemaRejectionState state = new SchemaRejectionState();
            state.setEngine(recovered);
            state.beginLease(1, 2, false);
            append(recovered, false);
            assertTrue(state.reject(0, 0, error(0)));
            assertEquals(1, state.sealedRange().lastFsn);
            assertFalse(state.hasOwnedFailure(1));
        }
    }

    private static void append(CursorSendEngine engine, boolean deferred) {
        long frame = Unsafe.malloc(QwpConstants.HEADER_SIZE, MemoryTag.NATIVE_DEFAULT);
        try {
            Unsafe.getUnsafe().setMemory(frame, QwpConstants.HEADER_SIZE, (byte) 0);
            Unsafe.getUnsafe().putInt(frame, QwpConstants.MAGIC_MESSAGE);
            Unsafe.getUnsafe().putByte(frame + QwpConstants.HEADER_OFFSET_FLAGS,
                    (byte) (deferred ? QwpConstants.FLAG_DEFER_COMMIT : 0));
            engine.appendBlocking(frame, QwpConstants.HEADER_SIZE);
        } finally {
            Unsafe.free(frame, QwpConstants.HEADER_SIZE, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testReturnCapturesRejectionArrivingAfterFinalProducerCall() {
        SchemaRejectionState state = new SchemaRejectionState();
        state.beginLease(7, 10, true);
        assertTrue(state.reject(12, 10, error(12)));
        LineSenderServerException failure = state.endLease(7, 15);
        assertNotNull(failure);
        assertEquals(15, failure.getServerError().getToFsn());
        assertNull("duplicate return must not throw again", state.endLease(7, 15));
    }

    @Test
    public void testOpenTransactionSealsOnFirstProducerObservation() {
        SchemaRejectionState state = new SchemaRejectionState();
        state.beginLease(7, 10, true);
        assertTrue(state.reject(12, 10, error(12)));
        assertTrue(state.hasOwnedFailure(7));
        assertNull(state.sealedRange());

        LineSenderServerException first = state.ownedFailure(7, 15);
        assertNotNull(first);
        assertSame(first, state.ownedFailure(7, 99));
        assertEquals(10, first.getServerError().getFromFsn());
        assertEquals(15, first.getServerError().getToFsn());
        assertEquals(15, state.sealedRange().lastFsn);
    }

    @Test
    public void testOrdinaryRangeIsImmediatelySealedAtRejectedFrame() {
        SchemaRejectionState state = new SchemaRejectionState();
        state.beginLease(2, 3, false);
        assertTrue(state.reject(5, 4, error(5)));
        assertEquals(5, state.sealedRange().lastFsn);
        assertEquals(5, state.ownedFailure(2, 8).getServerError().getToFsn());
    }

    @Test
    public void testRecoveredFrameWithoutLeaseStillRetiresAndReports() {
        SchemaRejectionState state = new SchemaRejectionState();
        assertTrue(state.reject(4, 2, error(4)));
        SchemaRejectionState.Range range = state.sealedRange();
        assertNotNull(range);
        assertEquals(2, range.firstFsn);
        assertEquals(4, range.lastFsn);
        assertEquals(4, range.error.getRejectedFsn());
    }

    @Test
    public void testReturnedUnackedTransactionalLeaseRetainsOwnershipAndFinalEnd() {
        SchemaRejectionState state = new SchemaRejectionState();
        state.beginLease(7, 10, true);
        state.endLease(7, 15);

        assertTrue(state.reject(12, 10, error(12)));
        assertEquals(10, state.stopFsn());
        assertEquals(15, state.sealedRange().lastFsn);
        LineSenderServerException failure = state.ownedFailure(7, 99);
        assertNotNull(failure);
        assertEquals(10, failure.getServerError().getFromFsn());
        assertEquals(15, failure.getServerError().getToFsn());
    }

    @Test
    public void testClosedTransactionDoesNotRetireNextTransactionInSameLease() {
        try (CursorSendEngine engine = new CursorSendEngine(null, 4096)) {
            long frame = Unsafe.malloc(QwpConstants.HEADER_SIZE, MemoryTag.NATIVE_DEFAULT);
            try {
                Unsafe.getUnsafe().setMemory(frame, QwpConstants.HEADER_SIZE, (byte) 0);
                Unsafe.getUnsafe().putInt(frame, QwpConstants.MAGIC_MESSAGE);
                // Two closed transactions share one sender lease.
                for (int i = 0; i < 4; i++) {
                    Unsafe.getUnsafe().putByte(frame + QwpConstants.HEADER_OFFSET_FLAGS, (byte) ((i & 1) == 0 ? QwpConstants.FLAG_DEFER_COMMIT : 0));
                    engine.appendBlocking(frame, QwpConstants.HEADER_SIZE);
                }
                SchemaRejectionState state = new SchemaRejectionState();
                state.setEngine(engine);
                state.beginLease(1, 0, true);
                assertTrue(state.reject(0, 0, error(0)));
                assertEquals(1, state.sealedRange().lastFsn);
                assertEquals(1, state.ownedFailure(1, 3).getServerError().getToFsn());
                state.completeRetirement(1);
                // A second rejected transaction has its own notification span;
                // the handle keeps the first immutable exception.
                assertTrue(state.reject(2, 2, error(2)));
                assertEquals(2, state.sealedRange().error.getRejectedFsn());
                assertEquals(3, state.sealedRange().lastFsn);
                assertEquals(1, state.ownedFailure(1, 3).getServerError().getToFsn());
            } finally {
                Unsafe.free(frame, QwpConstants.HEADER_SIZE, MemoryTag.NATIVE_DEFAULT);
            }
        }
    }

    private static SenderError error(long fsn) {
        return new SenderError(SenderError.Category.SCHEMA_MISMATCH,
                SenderError.Policy.REJECT_AND_CONTINUE, 7, "mismatch", 1,
                fsn, fsn, "tab", System.nanoTime());
    }
}
