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

import java.lang.reflect.Method;

/**
 * JDK-version-specific helpers. This is the Java 9+ variant; the parallel copy
 * under {@code src/main/java8} provides Java 8 implementations of the same API.
 */
public final class Compat {

    private Compat() {
    }

    /**
     * Returns the current process id, or {@code -1} if it cannot be determined.
     * Diagnostic use only — callers must never depend on a real value.
     */
    public static long currentPid() {
        try {
            return ProcessHandle.current().pid();
        } catch (Throwable ignored) {
            return -1L;
        }
    }

    /**
     * Spin-loop pause hint. Delegates to {@code Thread.onSpinWait()} on Java 9+;
     * the Java 8 variant is a no-op (no such hint exists there).
     */
    public static void onSpinWait() {
        Thread.onSpinWait();
    }

    /**
     * Opens {@code java.base/jdk.internal.math} to this module so that
     * {@code FDBigInteger} is reachable at runtime, mirroring the
     * {@code --add-exports} flag used at compile time.
     */
    static void exportFdBigInteger() {
        try {
            Module base = System.class.getModule();
            Module current = Compat.class.getModule();
            Method implAddExports = Module.class.getDeclaredMethod("implAddExports", String.class, Module.class);
            Unsafe.makeAccessible(implAddExports);
            implAddExports.invoke(base, "jdk.internal.math", current);
        } catch (ReflectiveOperationException e) {
            e.printStackTrace(System.out);
        }
    }
}
