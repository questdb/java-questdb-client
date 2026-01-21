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
#include "jni.h"
#include <cstring>
#include <cassert>
#include "util.h"
#include "simd.h"
#include "ooo_dispatch.h"
#include <algorithm>

extern "C" {

DECLARE_DISPATCHER(platform_memcpy);
JNIEXPORT void JNICALL Java_io_questdb_std_Vect_memcpy0
        (JNIEnv *e, jclass cl, jlong src, jlong dst, jlong len) {
    platform_memcpy(
            reinterpret_cast<void *>(dst),
            reinterpret_cast<void *>(src),
            __JLONG_REINTERPRET_CAST__(int64_t, len)
    );
}

DECLARE_DISPATCHER(platform_memmove);
JNIEXPORT void JNICALL Java_io_questdb_std_Vect_memmove
        (JNIEnv *e, jclass cl, jlong dst, jlong src, jlong len) {
    platform_memmove(
            reinterpret_cast<void *>(dst),
            reinterpret_cast<void *>(src),
            __JLONG_REINTERPRET_CAST__(int64_t, len)
    );
}

DECLARE_DISPATCHER(platform_memset);
JNIEXPORT void JNICALL Java_io_questdb_std_Vect_memset
        (JNIEnv *e, jclass cl, jlong dst, jlong len, jint value) {
    platform_memset(
            reinterpret_cast<void *>(dst),
            value,
            __JLONG_REINTERPRET_CAST__(int64_t, len)
    );
}
}
// extern "C"
