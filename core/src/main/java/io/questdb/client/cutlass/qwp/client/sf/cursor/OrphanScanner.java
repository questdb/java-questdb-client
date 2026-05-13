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

package io.questdb.client.cutlass.qwp.client.sf.cursor;

import io.questdb.client.std.Files;
import io.questdb.client.std.ObjList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Reads the SF group root and reports sibling slots that look like they
 * still hold unacked data — candidates for background-drainer adoption.
 * <p>
 * A slot is a "candidate orphan" iff:
 * <ul>
 *   <li>It's a child directory of {@code sfDir}.</li>
 *   <li>It's NOT the caller's own slot (filtered by name).</li>
 *   <li>It contains at least one {@code *.sfa} segment file.</li>
 *   <li>It does NOT contain a {@link #FAILED_SENTINEL_NAME} file —
 *       that flag means a previous drainer gave up and the data needs
 *       human attention before automation tries again.</li>
 * </ul>
 * <p>
 * Lock state is intentionally not part of the candidate filter — testing
 * it requires actually opening + flocking the lock file, which races
 * with concurrent drainers/senders. The drainer pool attempts to acquire
 * each candidate's lock in turn and skips ones that fail; this keeps the
 * scanner pure and read-only.
 * <p>
 * Empty slot dirs (no {@code .sfa} files but a stale {@code .lock} from
 * a clean shutdown) are NOT candidates — there's nothing to drain. Spec
 * decision #13 ("no automatic cleanup of empty slot dirs") leaves them
 * in place; scanning past them is fine.
 */
public final class OrphanScanner {

    private static final Logger LOG = LoggerFactory.getLogger(OrphanScanner.class);

    /** Name of the sentinel that disqualifies a slot from auto-drain. */
    public static final String FAILED_SENTINEL_NAME = ".failed";

    private OrphanScanner() {
    }

    /**
     * Walks {@code sfDir}'s children once and returns the candidate
     * orphan slot paths. {@code excludeSlotName} (typically the
     * foreground sender's {@code sender_id}) is filtered out so we
     * don't list our own slot as an orphan.
     * <p>
     * Returns an empty list if {@code sfDir} doesn't exist or is empty —
     * never throws on missing directory; the caller wants a clean
     * "no orphans" answer in that case.
     */
    public static ObjList<String> scan(String sfDir, String excludeSlotName) {
        ObjList<String> orphans = new ObjList<>();
        if (sfDir == null || !Files.exists(sfDir)) {
            return orphans;
        }
        long find = Files.findFirst(sfDir);
        if (find < 0) {
            LOG.warn("orphan scan could not enumerate {} — treating as no orphans, "
                    + "but this may indicate a permission or transient error", sfDir);
            return orphans;
        }
        if (find == 0) {
            return orphans;
        }
        try {
            int rc = 1;
            while (rc > 0) {
                String name = Files.utf8ToString(Files.findName(find));
                rc = Files.findNext(find);
                if (name == null || ".".equals(name) || "..".equals(name)) {
                    continue;
                }
                if (excludeSlotName != null && excludeSlotName.equals(name)) {
                    continue;
                }
                String slotPath = sfDir + "/" + name;
                if (!isCandidateOrphan(slotPath)) {
                    continue;
                }
                orphans.add(slotPath);
            }
        } finally {
            Files.findClose(find);
        }
        return orphans;
    }

    /**
     * True iff {@code slotPath} looks like a slot dir with unacked data
     * and no failure sentinel. Visible for testing.
     */
    public static boolean isCandidateOrphan(String slotPath) {
        if (!Files.exists(slotPath)) {
            return false;
        }
        if (Files.exists(slotPath + "/" + FAILED_SENTINEL_NAME)) {
            return false;
        }
        return hasAnySegmentFile(slotPath);
    }

    /**
     * Drops a {@link #FAILED_SENTINEL_NAME} file in {@code slotPath}.
     * Idempotent — touching an existing sentinel is a no-op (its presence
     * is the signal; contents don't matter to scanning logic, though we
     * write a one-line reason for human readers).
     */
    public static void markFailed(String slotPath, String reason) {
        String path = slotPath + "/" + FAILED_SENTINEL_NAME;
        int fd = Files.openRW(path);
        if (fd < 0) {
            // Best-effort — even if we can't write the sentinel, the
            // drainer is exiting anyway, and the next scan will retry.
            return;
        }
        try {
            byte[] payload = (reason == null ? "drainer failed" : reason)
                    .getBytes(java.nio.charset.StandardCharsets.UTF_8);
            Files.truncate(fd, 0L);
            long addr = io.questdb.client.std.Unsafe.malloc(
                    payload.length,
                    io.questdb.client.std.MemoryTag.NATIVE_DEFAULT);
            try {
                for (int i = 0; i < payload.length; i++) {
                    io.questdb.client.std.Unsafe.getUnsafe().putByte(addr + i, payload[i]);
                }
                Files.write(fd, addr, payload.length, 0L);
            } finally {
                io.questdb.client.std.Unsafe.free(
                        addr, payload.length,
                        io.questdb.client.std.MemoryTag.NATIVE_DEFAULT);
            }
        } finally {
            Files.close(fd);
        }
    }

    private static boolean hasAnySegmentFile(String slotPath) {
        long find = Files.findFirst(slotPath);
        if (find < 0) {
            LOG.warn("could not enumerate slot {} when checking for segment files", slotPath);
            return false;
        }
        if (find == 0) {
            return false;
        }
        try {
            int rc = 1;
            while (rc > 0) {
                String name = Files.utf8ToString(Files.findName(find));
                rc = Files.findNext(find);
                if (name != null && name.endsWith(".sfa")) {
                    return true;
                }
            }
        } finally {
            Files.findClose(find);
        }
        return false;
    }
}
