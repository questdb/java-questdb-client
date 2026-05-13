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

/**
 * Hard failure of the MmapSegment layer — bad header, mmap rejection, file
 * too short for header, etc. Indicates the segment is unusable, not that
 * the disk is full (the latter surfaces as backpressure on the producer
 * via {@link io.questdb.client.cutlass.qwp.client.LineSenderException}).
 */
public final class MmapSegmentException extends RuntimeException {
    public MmapSegmentException(String message) {
        super(message);
    }

    public MmapSegmentException(String message, Throwable cause) {
        super(message, cause);
    }
}
