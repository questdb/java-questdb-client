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

package io.questdb.client.std;

import java.lang.management.ManagementFactory;

/**
 * JDK-version-specific helpers. This is the Java 8 variant; the parallel copy
 * under {@code src/main/java11} provides Java 9+ implementations of the same
 * API.
 */
public final class Compat {

    private Compat() {
    }

    /**
     * Returns the current process id, or {@code -1} if it cannot be determined.
     * Diagnostic use only — callers must never depend on a real value. Java 8
     * has no {@code ProcessHandle}, so the pid is parsed from the
     * {@code pid@host} runtime name.
     */
    public static long currentPid() {
        try {
            String name = ManagementFactory.getRuntimeMXBean().getName();
            int at = name.indexOf('@');
            if (at > 0) {
                return Long.parseLong(name.substring(0, at));
            }
        } catch (Throwable ignored) {
            // fall through
        }
        return -1L;
    }

    /**
     * Spin-loop pause hint. No-op on Java 8 — {@code Thread.onSpinWait()} does
     * not exist there. The parallel Java 9+ variant delegates to it.
     */
    public static void onSpinWait() {
    }

    /**
     * No-op on Java 8: {@code sun.misc} is open, so {@code FDBigInteger} needs
     * no module export. Present for symmetry with the Java 9+ variant.
     */
    static void exportFdBigInteger() {
    }
}
