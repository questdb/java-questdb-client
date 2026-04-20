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

/*
 * JNI wrapper over the bundled libzstd (decompression only). The server ships
 * compression support in the Rust qdbr crate; this file covers the client
 * decompression path so RESULT_BATCH frames with FLAG_ZSTD can be decoded
 * without any external native dependency.
 *
 * libzstd is vendored as a git submodule at share/zstd/ pinned to v1.5.7;
 * this file lives alongside (not inside) the submodule so upstream resets
 * don't nuke our JNI glue.
 */

#include <jni.h>
#include <stdlib.h>
#include "zstd.h"

JNIEXPORT jlong JNICALL Java_io_questdb_client_std_Zstd_createDCtx(
        JNIEnv *env, jclass cls) {
    return (jlong) (uintptr_t) ZSTD_createDCtx();
}

JNIEXPORT void JNICALL Java_io_questdb_client_std_Zstd_freeDCtx(
        JNIEnv *env, jclass cls, jlong ptr) {
    if (ptr != 0) {
        ZSTD_freeDCtx((ZSTD_DCtx *) (uintptr_t) ptr);
    }
}

JNIEXPORT jlong JNICALL Java_io_questdb_client_std_Zstd_decompress(
        JNIEnv *env, jclass cls,
        jlong ctx,
        jlong src_addr, jlong src_len,
        jlong dst_addr, jlong dst_cap) {
    if (ctx == 0) {
        return -1;
    }
    ZSTD_DCtx *dctx = (ZSTD_DCtx *) (uintptr_t) ctx;
    size_t n = ZSTD_decompressDCtx(
            dctx,
            (void *) (uintptr_t) dst_addr, (size_t) dst_cap,
            (const void *) (uintptr_t) src_addr, (size_t) src_len);
    if (ZSTD_isError(n)) {
        unsigned code = ZSTD_getErrorCode(n);
        return -(jlong) code;
    }
    return (jlong) n;
}
