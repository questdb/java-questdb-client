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

#ifndef QUESTDB_GLIBC_COMPAT_H
#define QUESTDB_GLIBC_COMPAT_H

// Pin clock_gettime() to its original GLIBC_2.2.5 symbol version.
//
// glibc 2.17 moved clock_gettime() out of librt and into libc, exporting it
// under a NEW version node: clock_gettime@GLIBC_2.17. The release binaries are
// built in a modern toolchain container (CI uses manylinux_2_28 / glibc 2.28),
// so without this pin the linker binds our calls to clock_gettime@GLIBC_2.17.
// That single symbol raises the whole library's glibc floor to 2.17 and makes
// it fail to LOAD on hosts running glibc 2.14-2.16 with:
//
//     version `GLIBC_2.17' not found (required by libquestdb.so)
//
// The original clock_gettime@GLIBC_2.2.5 symbol is still exported as a compat
// symbol by librt.so.1 on every glibc since (and by libc after the 2.34 librt
// merge), so forcing the reference back to it keeps the library loadable down
// to the previous floor (glibc 2.14, set by memcpy@GLIBC_2.14) with no change
// in runtime behaviour. librt is already a NEEDED dependency (CMake links rt).
//
// Scope: x86-64 glibc only. aarch64 glibc started at 2.17 and has only ever
// shipped clock_gettime in libc@GLIBC_2.17 -- there is no GLIBC_2.2.5 version
// there, so emitting the pin on aarch64 would fail the link with an undefined
// clock_gettime@GLIBC_2.2.5. The directive is a no-op on macOS/Windows.
#if defined(__linux__) && defined(__GLIBC__) && defined(__x86_64__)
__asm__(".symver clock_gettime,clock_gettime@GLIBC_2.2.5");
#endif

#endif // QUESTDB_GLIBC_COMPAT_H
