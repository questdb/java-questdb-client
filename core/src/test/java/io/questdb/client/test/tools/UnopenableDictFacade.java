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

package io.questdb.client.test.tools;

/**
 * Refuses to create a slot's {@code .symbol-dict}, leaving nothing at its path,
 * so the engine runs in full-dictionary mode and the sender keeps its
 * frame-per-flush path.
 * <p>
 * It injects the open failure instead of planting a directory where the
 * dictionary file belongs: {@code openClean()} refuses to start on top of a
 * blocker it cannot truncate (see {@code PersistedSymbolDictTest}), so a planted
 * blocker would throw instead of degrading. The facade reproduces the case
 * {@code openClean()} still tolerates, a transient failure (EIO, fd exhaustion)
 * with nothing at the path to lose.
 */
public final class UnopenableDictFacade extends DelegatingFilesFacade {
    @Override
    public int openCleanRW(String path) {
        return -1;
    }
}
