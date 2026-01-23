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

package io.questdb.client.std;

import io.questdb.client.std.ex.FatalError;
import io.questdb.client.std.str.StringSink;
import io.questdb.client.std.str.Utf8StringSink;

import java.io.Closeable;
import java.io.IOException;
import java.util.Arrays;

public final class Misc {
    public static final String EOL = "\r\n";
    private static final ThreadLocal<Decimal128> tlDecimal128 = new ThreadLocal<>(Decimal128::new);
    private static final ThreadLocal<Decimal256> tlDecimal256 = new ThreadLocal<>(Decimal256::new);
    private static final ThreadLocal<StringSink> tlSink = new ThreadLocal<>(StringSink::new);
    private static final ThreadLocal<Utf8StringSink> tlUtf8Sink = new ThreadLocal<>(Utf8StringSink::new);

    private Misc() {
    }

    public static <T extends Closeable> T free(T object) {
        if (object != null) {
            try {
                object.close();
            } catch (IOException e) {
                throw new FatalError(e);
            }
        }
        return null;
    }

    // same as free() but can be used when input object type is not guaranteed to be Closeable
    public static <T> T freeIfCloseable(T object) {
        if (object instanceof Closeable) {
            try {
                ((Closeable) object).close();
            } catch (IOException e) {
                throw new FatalError(e);
            }
        }
        return null;
    }

    public static <T extends Closeable> void freeObjList(ObjList<T> list) {
        if (list != null) {
            freeObjList0(list);
        }
    }

    public static <T extends Closeable> void freeObjListAndClear(ObjList<T> list) {
        if (list != null) {
            for (int i = 0, n = list.size(); i < n; i++) {
                free(list.getQuick(i));
            }
            list.clear();
        }
    }

    public static Decimal128 getThreadLocalDecimal128() {
        return tlDecimal128.get();
    }

    public static Decimal256 getThreadLocalDecimal256() {
        return tlDecimal256.get();
    }

    public static StringSink getThreadLocalSink() {
        StringSink b = tlSink.get();
        b.clear();
        return b;
    }

    public static Utf8StringSink getThreadLocalUtf8Sink() {
        Utf8StringSink b = tlUtf8Sink.get();
        b.clear();
        return b;
    }

    public static int[] getWorkerAffinity(int workerCount) {
        int[] res = new int[workerCount];
        Arrays.fill(res, -1);
        return res;
    }

    private static <T> void freeObjList0(ObjList<T> list) {
        for (int i = 0, n = list.size(); i < n; i++) {
            list.setQuick(i, freeIfCloseable(list.getQuick(i)));
        }
    }

    private static class Utf8SinkPool {

    }
}