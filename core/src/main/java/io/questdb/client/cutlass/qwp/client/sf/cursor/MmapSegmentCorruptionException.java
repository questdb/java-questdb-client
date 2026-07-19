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
 * Positively-identified segment corruption: the file's own bytes prove it is
 * not (or no longer) a readable SF segment — truncated below the fixed header,
 * wrong magic, or a negative {@code baseSeq}. Distinct from its parent
 * {@link MmapSegmentException}, which recovery treats as an <b>operational</b>
 * failure (open/read/mmap error on a file
 * whose contents may be perfectly intact) and must therefore be fatal.
 * <p>
 * Recovery quarantines corruption (rename to {@code <name>.corrupt}) and
 * relies on manifest boundaries / FSN contiguity to decide whether the
 * quarantined file was load-bearing; operational failures always abort
 * startup so a transient {@code EMFILE}/{@code ENOMEM} can never silently
 * drop durable frames.
 */
public final class MmapSegmentCorruptionException extends MmapSegmentException {

    public MmapSegmentCorruptionException(String message) {
        super(message);
    }

    public MmapSegmentCorruptionException(String message, Throwable cause) {
        super(message, cause);
    }
}
