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

#include <cstring>
#include "util.h"
#include "simd.h"
#include "ooo_dispatch.h"
#include <algorithm>

void platform_memcpy(void *dst, const void *src, const size_t len)
{
    __MEMCPY(dst, src, len);
}

void platform_memcmp(const void *a, const void *b, const size_t len, int *res)
{
    *res = __MEMCMP(a, b, len);
}

void platform_memset(void *dst, const int val, const size_t len)
{
    __MEMSET(dst, val, len);
}

void platform_memmove(void *dst, const void *src, const size_t len)
{
    __MEMMOVE(dst, src, len);
}
