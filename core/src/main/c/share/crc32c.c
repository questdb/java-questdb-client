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

#include <jni.h>
#include <stdint.h>
#include <stddef.h>

#define CRC32C_POLY_REVERSED 0x82F63B78u

static uint32_t crc32c_table[256];
static volatile int crc32c_table_ready = 0;

static void crc32c_init(void) {
    for (int i = 0; i < 256; i++) {
        uint32_t c = (uint32_t) i;
        for (int j = 0; j < 8; j++) {
            c = (c & 1u) ? (c >> 1) ^ CRC32C_POLY_REVERSED : (c >> 1);
        }
        crc32c_table[i] = c;
    }
    crc32c_table_ready = 1;
}

JNIEXPORT jint JNICALL Java_io_questdb_client_std_Crc32c_update
        (JNIEnv *e, jclass cl, jint seed, jlong addr, jlong len) {
    if (len <= 0) {
        return seed;
    }
    if (!crc32c_table_ready) {
        crc32c_init();
    }
    uint32_t crc = ~((uint32_t) seed);
    const uint8_t *buf = (const uint8_t *) (uintptr_t) addr;
    size_t n = (size_t) len;
    while (n--) {
        crc = (crc >> 8) ^ crc32c_table[(crc ^ *buf++) & 0xffu];
    }
    return (jint) ~crc;
}
