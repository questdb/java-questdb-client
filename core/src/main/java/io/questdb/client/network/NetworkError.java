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

package io.questdb.client.network;

import io.questdb.client.std.FlyweightMessageContainer;
import io.questdb.client.std.ThreadLocal;
import io.questdb.client.std.str.CharSink;
import io.questdb.client.std.str.Sinkable;
import io.questdb.client.std.str.StringSink;
import org.jetbrains.annotations.NotNull;

public class NetworkError extends Error implements Sinkable, FlyweightMessageContainer {
    private static final ThreadLocal<NetworkError> tlException = new ThreadLocal<>(NetworkError::new);
    private final StringSink message = new StringSink();
    private int errno;

    public static NetworkError instance(int errno, CharSequence message) {
        NetworkError ex = tlException.get();
        ex.errno = errno;
        ex.message.clear();
        ex.message.put(message);
        return ex;
    }

    @Override
    public CharSequence getFlyweightMessage() {
        return message;
    }

    @Override
    public String getMessage() {
        return "[" + errno + "] " + message;
    }

    @Override
    public void toSink(@NotNull CharSink<?> sink) {
        sink.putAscii("[errno=").put(errno).putAscii("] ").put(message);
    }
}