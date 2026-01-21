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

#include <shlwapi.h>
#include <minwindef.h>
#include <fileapi.h>
#include <winbase.h>
#include <direct.h>
#include <stdint.h>
#include <windows.h>
#include "../share/files.h"
#include "errno.h"
#include "files.h"

#include <stdio.h>
#include <ntdef.h>

JNIEXPORT jint JNICALL Java_io_questdb_client_std_Files_close0
        (JNIEnv *e, jclass cl, jint fd) {
    jint r = CloseHandle(FD_TO_HANDLE(fd));
    if (!r) {
        SaveLastError();
        return -1;
    }
    return 0;
}