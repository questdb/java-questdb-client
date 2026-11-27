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

package io.questdb.client.test.cutlass.qwp.client.sf.cursor;

import io.questdb.client.cutlass.qwp.client.sf.cursor.MmapSegment;
import io.questdb.client.cutlass.qwp.client.sf.cursor.SegmentRing;
import io.questdb.client.cutlass.qwp.client.sf.cursor.SegmentSource;

import java.util.ArrayDeque;

/**
 * Test stand-in for {@code SegmentProvisioner}: hands a ring pre-built
 * segments in order, like the manager's hot spare used to. Segments never
 * consumed are closed with the ring.
 */
public final class QueueSegmentSource implements SegmentSource {
    private final ArrayDeque<MmapSegment> queue = new ArrayDeque<>();

    /** Queues {@code segment} as the ring's next rotation target, installing a queue source if needed. */
    public static void install(SegmentRing ring, MmapSegment segment) {
        of(ring).queue.add(segment);
    }

    /** True when the ring has no queued segment to rotate into. */
    public static boolean isEmpty(SegmentRing ring) {
        SegmentSource s = ring.getSegmentSourceForTesting();
        return !(s instanceof QueueSegmentSource) || ((QueueSegmentSource) s).queue.isEmpty();
    }

    public static MmapSegment peek(SegmentRing ring) {
        return of(ring).queue.peek();
    }

    private static QueueSegmentSource of(SegmentRing ring) {
        SegmentSource s = ring.getSegmentSourceForTesting();
        if (s instanceof QueueSegmentSource) {
            return (QueueSegmentSource) s;
        }
        QueueSegmentSource q = new QueueSegmentSource();
        ring.setSegmentSource(q);
        return q;
    }

    @Override
    public void close() {
        MmapSegment s;
        while ((s = queue.poll()) != null) {
            s.close();
        }
    }

    @Override
    public void discard(MmapSegment segment) {
        segment.close();
    }

    @Override
    public MmapSegment next(long baseSeq) {
        return queue.poll();
    }
}
