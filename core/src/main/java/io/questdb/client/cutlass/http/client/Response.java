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
     * Receives the next fragment of response data, bounded by this response's default timeout - the
     * {@link io.questdb.client.HttpClientConfiguration#getTimeout()} of the client that produced it.
     * <p>
     * The bound {@link #recv(int)} describes applies here too, because the implementations in this library
     * implement this method as {@code recv(defaultTimeout)}: it caps the WHOLE call rather than each socket
     * read, so a server dribbling the body cannot keep one call running past it. Every configuration this
     * library builds supplies a positive timeout - {@code request_timeout} is rejected below 1 on both the
     * builder and the configuration-string paths - so the bound is live unless a caller supplies its own
     * {@code HttpClientConfiguration} returning a non-positive value, which disables it.
     * <p>
     * Size that timeout against a SINGLE fragment read rather than the whole body: each call starts its own
     * budget, so a large body spread over many calls is unaffected, and only one call that cannot complete
     * within the timeout aborts.
     * <p>
     * Note the two methods delegate in OPPOSITE directions, which is what decides whether the bound exists
     * at all. The {@link #recv(int)} default defers down to this method and discards its argument; the
     * implementations here do the reverse. So an implementation overriding only this method is unbounded on
     * both, while one extending {@code AbstractResponse} or {@code AbstractChunkedResponse} is bounded on
     * both.
     *
     * @return the received fragment, or null once the body has been fully read
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
