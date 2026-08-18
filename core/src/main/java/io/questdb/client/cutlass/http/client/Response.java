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

package io.questdb.client.cutlass.http.client;

/**
 * Interface for receiving HTTP response data.
 */
public interface Response {
    /**
     * Receives the next fragment of response data using the default timeout.
     *
     * @return the received fragment
     */
    Fragment recv();

    /**
     * Receives the next fragment of response data. A positive {@code timeout} bounds the whole call to that
     * many milliseconds in total (not per socket read), so a server dribbling the body one byte at a time
     * cannot keep a single call running past it; a non-positive {@code timeout} disables the bound.
     * <p>
     * Defaulted rather than abstract for compatibility: this interface is exported, ships with a javadoc
     * jar, and gained {@code recv(int)} after {@link #recv()}, so an implementation written against the
     * earlier interface must keep both compiling and linking. The default ignores the bound and defers to
     * {@link #recv()} -- precisely what such an implementation did before this overload existed.
     * <p>
     * Every implementation in this library overrides it, and any implementation that wants the bound
     * honoured must do the same. An overriding implementation must not then implement {@link #recv()} by
     * calling back into this default, which would recurse.
     *
     * @param timeout the receive timeout in milliseconds
     * @return the received fragment, or null once the body has been fully read
     */
    default Fragment recv(int timeout) {
        return recv();
    }
}
