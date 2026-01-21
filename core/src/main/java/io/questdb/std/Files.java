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

import java.io.File;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

public final class Files {
    public static final char SEPARATOR;
    public static final Charset UTF_8;

    private Files() {
        // Prevent construction.
    }

    public static int close(int fd) {
        // do not close `stdin` and `stdout`
        if (fd > 2) {
            return close0(fd);
        }
        // failed to close
        return -1;
    }

    native static int close0(int fd);

    static {
        Os.init();
        UTF_8 = StandardCharsets.UTF_8;
        SEPARATOR = File.separatorChar;
    }
}