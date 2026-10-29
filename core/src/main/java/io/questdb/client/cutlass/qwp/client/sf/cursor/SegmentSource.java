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

package io.questdb.client.cutlass.qwp.client.sf.cursor;

import io.questdb.client.std.QuietCloseable;

/**
 * Where a {@link SegmentRing} gets the segment it rotates into. Called on the
 * producer thread, inside {@link SegmentRing#appendOrFsn}, when the active
 * segment is full. {@link SegmentProvisioner} is the production source.
 */
public interface SegmentSource extends QuietCloseable {

    /**
     * The next segment, starting at {@code baseSeq}, or {@code null} to
     * backpressure the producer (budget exhausted, create failed and its retry
     * is pending). Must not throw. A segment returned here is owned by the
     * ring unless it is handed back through {@link #discard}.
     */
    MmapSegment next(long baseSeq);

    /**
     * Takes back a segment {@link #next} returned that the rotation could not
     * commit (the manifest update threw). The segment keeps its file and its
     * accounting: the manifest record naming its base may already be in the
     * page cache, so recovery may need the file. The next {@link #next} at
     * the same base returns it again.
     */
    void discard(MmapSegment segment);

    /**
     * Bytes of segments the source holds for the ring's next rotation (a
     * segment handed back through {@link #discard}). They stay charged to the
     * budget, so {@link SegmentRing#totalSegmentBytes()} counts them and a
     * deregister releases them with the rest of the ring.
     */
    default long heldBytes() {
        return 0L;
    }

    /** Releases whatever the source still holds; the owning ring calls this from its own close. */
    @Override
    void close();
}
