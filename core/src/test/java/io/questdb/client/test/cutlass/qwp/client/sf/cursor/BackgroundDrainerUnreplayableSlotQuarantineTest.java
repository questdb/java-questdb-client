/*******************************************************************************
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

package io.questdb.client.test.cutlass.qwp.client.sf.cursor;

import io.questdb.client.cutlass.qwp.client.sf.cursor.BackgroundDrainer;
import io.questdb.client.cutlass.qwp.client.sf.cursor.MmapSegment;
import io.questdb.client.cutlass.qwp.client.sf.cursor.OrphanScanner;
import io.questdb.client.std.Files;
import io.questdb.client.std.MemoryTag;
import io.questdb.client.std.Unsafe;
import io.questdb.client.test.tools.TestUtils;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * CRITICAL-1 regression: recovery that has to skip an unreadable segment must quarantine
 * the slot ({@code .failed} sentinel + {@link BackgroundDrainer.DrainOutcome#FAILED}), not
 * merely fail this one run and leave the slot re-adoptable.
 * <p>
 * Before this fix, {@code BackgroundDrainer.run()} caught only {@code IllegalStateException}
 * around {@code new CursorSendEngine(...)}. {@code UnreplayableSlotException} fell through
 * to the generic {@code catch (Throwable)}, whose {@code engine != null} gate -- correct for
 * a genuinely pre-adoption failure -- also suppressed the sentinel write here, because
 * {@code engine} is null precisely because the constructor threw. With no sentinel, the slot
 * stayed a valid orphan candidate: the skipped segment is already renamed {@code .corrupt} on
 * disk, so a second recovery attempt recomputes {@code skippedSegmentCount} as zero, sees
 * only the two survivors, and seeds {@code ackedFsn} one below their lowest baseSeq --
 * declaring the frames the skipped segment held as already-acked and reporting a clean drain
 * over data that was never actually sent.
 */
public class BackgroundDrainerUnreplayableSlotQuarantineTest {

    private static final int FRAME_PAYLOAD_BYTES = 16;

    @Rule
    public final TemporaryFolder temporaryFolder = TemporaryFolder.builder().assureDeletion().build();

    @Test
    public void testSecondDrainerNeverReAdoptsAQuarantinedSlot() throws Exception {
        String slotPath = temporaryFolder.newFolder("slot").getAbsolutePath();
        long segSize = MmapSegment.HEADER_SIZE + 10 * (MmapSegment.FRAME_HEADER_SIZE + FRAME_PAYLOAD_BYTES);
        TestUtils.assertMemoryLeak(() -> {
            // Three segments: bases 0, 10, 20. The oldest is corrupted after being
            // written, so recovery has to skip it -- exactly the loss chain CRITICAL-1
            // describes.
            String oldestPath = slotPath + "/skip-oldest-0.sfa";
            writeSegment(oldestPath, 0, segSize, 10);
            writeSegment(slotPath + "/skip-oldest-1.sfa", 10, segSize, 10);
            writeSegment(slotPath + "/skip-oldest-2.sfa", 20, segSize, 1);
            corruptMagic(oldestPath);

            AtomicInteger connectAttempts = new AtomicInteger();
            BackgroundDrainer drainer1 = new BackgroundDrainer(
                    slotPath,
                    segSize,
                    1L << 20,
                    () -> {
                        connectAttempts.incrementAndGet();
                        throw new AssertionError(
                                "recovery failure must be caught before any connect attempt");
                    },
                    5_000L, 1L, 5L, true, 200L);

            drainer1.run();

            assertEquals("a slot whose recovery had to skip an unreadable segment must FAIL",
                    BackgroundDrainer.DrainOutcome.FAILED, drainer1.outcome());
            assertTrue("the failed sentinel must be written so a later scan excludes this slot",
                    Files.exists(slotPath + "/" + OrphanScanner.FAILED_SENTINEL_NAME));
            assertFalse("a quarantined slot must not look like a candidate orphan any more",
                    OrphanScanner.isCandidateOrphan(slotPath));
            assertEquals("quarantine must happen before any connect attempt",
                    0, connectAttempts.get());

            // Simulate the pool's next scan cycle with a fresh drainer against the same
            // slot. Without the fix above, no sentinel was written by drainer1, so the
            // slot still looks like a candidate: this second recovery attempt succeeds
            // cleanly (the skipped segment is already renamed .corrupt, so
            // skippedSegmentCount recomputes as zero), CursorSendEngine seeds ackedFsn
            // past the 10 frames the skipped segment held, and the drainer goes on to
            // replay only the survivors against the wire -- reporting a clean drain over
            // silently lost data. What actually stops that here is the revalidation
            // inside run() (OrphanScanner.isCandidateOrphan): it now sees the .failed
            // sentinel and bails out before ever touching CursorSendEngine again, so the
            // trap factory below must never run.
            BackgroundDrainer drainer2 = new BackgroundDrainer(
                    slotPath,
                    segSize,
                    1L << 20,
                    () -> {
                        connectAttempts.incrementAndGet();
                        throw new AssertionError("a quarantined slot must never be re-adopted");
                    },
                    5_000L, 1L, 5L, true, 200L);

            drainer2.run();

            assertEquals("a quarantined slot must never be re-adopted by a later scan",
                    0, connectAttempts.get());
        });
    }

    private static void corruptMagic(String path) {
        int fd = Files.openRW(path);
        assertTrue("openRW failed", fd >= 0);
        long buf = Unsafe.malloc(4, MemoryTag.NATIVE_DEFAULT);
        try {
            Unsafe.getUnsafe().putInt(buf, 0xBADBAD00);
            Files.write(fd, buf, 4, 0);
        } finally {
            Unsafe.free(buf, 4, MemoryTag.NATIVE_DEFAULT);
            Files.close(fd);
        }
    }

    private static void writeSegment(String path, long baseSeq, long segSizeBytes, int frameCount) {
        long buf = Unsafe.malloc(FRAME_PAYLOAD_BYTES, MemoryTag.NATIVE_DEFAULT);
        try {
            MmapSegment seg = MmapSegment.create(path, baseSeq, segSizeBytes);
            try {
                for (int i = 0; i < frameCount; i++) {
                    seg.tryAppend(buf, FRAME_PAYLOAD_BYTES);
                }
            } finally {
                seg.close();
            }
        } finally {
            Unsafe.free(buf, FRAME_PAYLOAD_BYTES, MemoryTag.NATIVE_DEFAULT);
        }
    }
}
