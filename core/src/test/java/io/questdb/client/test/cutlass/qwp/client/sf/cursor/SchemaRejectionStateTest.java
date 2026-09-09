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
    public void testLateOrdinaryRejectionAfterManyBorrows() throws Exception {
        SchemaRejectionState state = new SchemaRejectionState();
        for (int i = 0; i < 20_000; i++) {
            state.beginLease(i, i, false);
            state.endLease(i, i);
        }
        state.beginLease(20_000, 20_000, false);
        assertTrue(state.reject(10_000, 10_000, error(10_000)));
        assertEquals(10_000, state.sealedRange().lastFsn);
        assertFalse(state.hasOwnedFailure(20_000));
        assertNull(state.ownedFailure(20_000, 20_000));
    }

    @Test
    public void testLateTransactionalRejectionAfterManyBorrows() throws Exception {
        try (CursorSendEngine engine = new CursorSendEngine(null, 4096)) {
            SchemaRejectionState state = new SchemaRejectionState();
            state.setEngine(engine);
            for (int i = 0; i < 20_000; i++) {
                state.beginLease(i, 2L * i, true);
                append(engine, true);
                append(engine, false);
                state.endLease(i, 2L * i + 1);
            }
                state.beginLease(20_000, 40_000, true);
            assertTrue(state.reject(20_000, 20_000, error(20_000)));
            assertEquals(20_001, state.sealedRange().lastFsn);
            assertFalse(state.hasOwnedFailure(20_000));
            state.completeRetirement(20_001);
            assertTrue(state.reject(20_002, 20_002, error(20_002)));
            assertEquals(20_003, state.sealedRange().lastFsn);
            assertFalse(state.hasOwnedFailure(20_000));
        }
    }

    @Test
    public void testUnfinishedReturnedTransactionCannotConsumeLaterBorrow() throws Exception {
        try (CursorSendEngine engine = new CursorSendEngine(null, 4096)) {
            SchemaRejectionState state = new SchemaRejectionState();
            state.setEngine(engine);
            state.beginLease(1, 0, true);
            append(engine, true);
            append(engine, true);
            assertTrue(state.reject(0, 0, error(0)));
            assertNull(state.sealedRange());
            state.endLease(1, 1);
            state.beginLease(2, 2, true);
            append(engine, false);
            state.endLease(2, 2);
            state.beginLease(3, 3, true);
            assertEquals(1, state.sealedRange().lastFsn);
            assertFalse(state.hasOwnedFailure(3));
        }
    }

    @Test
    public void testPendingRetirementSurvivesManyBorrows() throws Exception {
        SchemaRejectionState state = new SchemaRejectionState();
        state.beginLease(1, 0, false);
        state.endLease(1, 0);
        assertTrue(state.reject(0, 0, error(0)));
        for (int i = 2; i < 20_000; i++) {
            state.beginLease(i, i - 1, false);
            state.endLease(i, i - 1);
        }
        assertEquals(0, state.sealedRange().lastFsn);
        state.completeRetirement(0);
        state.beginLease(20_000, 19_999, false);
        assertTrue(state.reject(1, 1, error(1)));
        assertEquals(1, state.sealedRange().lastFsn);
        assertFalse(state.hasOwnedFailure(20_000));
    }

    @Test
    public void testPendingRangeAndNextBorrowHaveIndependentFailures() {
        SchemaRejectionState state = new SchemaRejectionState();
        state.beginLease(1, 0, false);
        state.endLease(1, 0);
        state.beginLease(2, 1, false);
        assertTrue(state.reject(1, 1, error(1)));
        LineSenderServerException failure = state.ownedFailure(2, 1);
        assertSame(failure, state.endLease(2, 1));

        // Reborrowing cannot replace the pending notification's original range.
        state.beginLease(3, 2, false);
        assertEquals(1, state.sealedRange().firstFsn);
        assertEquals(1, state.sealedRange().lastFsn);
        assertFalse(state.hasOwnedFailure(2));
        assertFalse(state.hasOwnedFailure(3));
        state.completeRetirement(1);
        assertTrue(state.reject(2, 2, error(2)));
        assertTrue(state.hasOwnedFailure(3));
        LineSenderServerException nextFailure = state.ownedFailure(3, 2);
        assertNotSame(failure, nextFailure);
        assertEquals(2, nextFailure.getServerError().getRejectedFsn());
    }

    @Test
    public void testEmptyBorrowsDoNotOwnEarlierPublications() throws Exception {
        SchemaRejectionState state = new SchemaRejectionState();
        state.beginLease(0, 0, false);
        state.endLease(0, 0);
        for (int i = 1; i < 20_000; i++) {
            state.beginLease(i, 1, false);
            state.endLease(i, 0);
        }
        state.beginLease(20_000, 1, false);
        state.endLease(20_000, 0);
    }

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
    public void testReturnedUnackedTransactionalLeaseReportsWithoutOwnedFailure() {
        SchemaRejectionState state = new SchemaRejectionState();
        state.beginLease(7, 10, true);
        state.endLease(7, 15);

        assertTrue(state.reject(12, 10, error(12)));
        assertEquals(10, state.stopFsn());
        assertEquals(15, state.sealedRange().lastFsn);
        assertFalse(state.hasOwnedFailure(7));
        assertNull(state.ownedFailure(7, 99));
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

    @Test
    public void testUnclosedReturnDoesNotReleaseProducerOwnership() throws Exception {
        try (CursorSendEngine engine = new CursorSendEngine(null, 4096)) {
            SchemaRejectionState state = new SchemaRejectionState();
            state.setEngine(engine);
            state.beginLease(1, 0, true);
            append(engine, true);
            try {
                state.endLease(1, 0);
                fail("normal return needs a closer");
            } catch (IllegalStateException expected) {
                assertTrue(expected.getMessage().contains("no commit or rejection boundary"));
            }
            // Producer ownership remains intact; only sealing its rejection
            // permits this unfinished transaction to be followed by a new borrow.
            assertTrue(state.reject(0, 0, error(0)));
            assertNotNull(state.endLease(1, 0));
            state.beginLease(2, 1, true);
            append(engine, false);
            assertEquals(0, state.sealedRange().lastFsn);
            assertFalse(state.hasOwnedFailure(2));
        }
    }

    @Test
    public void testReturnRacingRejectionNeverFailsNextBorrow() throws Exception {
        java.util.concurrent.ExecutorService io = java.util.concurrent.Executors.newSingleThreadExecutor();
        try {
            for (int i = 0; i < 500; i++) {
                SchemaRejectionState state = new SchemaRejectionState();
                state.beginLease(1, 0, false);
                java.util.concurrent.CyclicBarrier start = new java.util.concurrent.CyclicBarrier(2);
                java.util.concurrent.Future<Boolean> rejected = io.submit(() -> {
                    start.await(5, java.util.concurrent.TimeUnit.SECONDS);
                    return state.reject(0, 0, error(0));
                });
                start.await(5, java.util.concurrent.TimeUnit.SECONDS);
                LineSenderServerException returned = state.endLease(1, 0);
                state.beginLease(2, 1, false);
                assertTrue(rejected.get(5, java.util.concurrent.TimeUnit.SECONDS));
                if (returned != null) assertEquals(0, returned.getServerError().getRejectedFsn());
                assertFalse(state.hasOwnedFailure(2));
                assertNull(state.ownedFailure(2, 1));
                assertEquals(0, state.sealedRange().lastFsn);
            }
        } finally {
            io.shutdownNow();
            assertTrue(io.awaitTermination(5, java.util.concurrent.TimeUnit.SECONDS));
        }
    }

    private static SenderError error(long fsn) {
        return new SenderError(SenderError.Category.SCHEMA_MISMATCH,
                SenderError.Policy.REJECT_AND_CONTINUE, 7, "mismatch", 1,
                fsn, fsn, "tab", System.nanoTime());
    }
}
