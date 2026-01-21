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

package io.questdb.std;

public final class MemoryTag {
    public static final int MMAP_DEFAULT = 0;

    // All malloc calls should use NATIVE_* tags
    public static final int NATIVE_PATH = MMAP_DEFAULT + 1;
    public static final int NATIVE_DEFAULT = NATIVE_PATH + 1;
    public static final int NATIVE_DIRECT_UTF8_SINK = NATIVE_DEFAULT + 1;
    public static final int NATIVE_HTTP_CONN = NATIVE_DIRECT_UTF8_SINK + 1;
    public static final int NATIVE_ILP_RSS = NATIVE_HTTP_CONN + 1;
    public static final int NATIVE_IO_DISPATCHER_RSS = NATIVE_ILP_RSS + 1;
    public static final int NATIVE_TEXT_PARSER_RSS = NATIVE_IO_DISPATCHER_RSS + 1;
    public static final int NATIVE_TLS_RSS = NATIVE_TEXT_PARSER_RSS + 1;
    public static final int NATIVE_ND_ARRAY = NATIVE_TLS_RSS + 1;
    public static final int SIZE = NATIVE_ND_ARRAY + 1;
}