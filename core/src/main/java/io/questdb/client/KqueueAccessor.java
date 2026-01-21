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

package io.questdb.client;

public class KqueueAccessor {
    public static final short DATA_OFFSET;
    public static final short EVFILT_READ;
    public static final short EVFILT_WRITE;
    public static final short EV_ADD;
    public static final short EV_ONESHOT;
    public static final short FD_OFFSET;
    public static final short FILTER_OFFSET;
    public static final short FLAGS_OFFSET;
    public static final short SIZEOF_KEVENT;

    public static native int kevent(int kq, long changeList, int nChanges, long eventList, int nEvents, int timeout);

    public static native int kqueue();

    private static native short getDataOffset();

    private static native short getEvAdd();

    private static native short getEvOneshot();

    private static native short getEvfiltRead();

    private static native short getEvfiltWrite();

    private static native short getFdOffset();

    private static native short getFilterOffset();

    private static native short getFlagsOffset();

    private static native short getSizeofKevent();

    static {
        EVFILT_READ = getEvfiltRead();
        EVFILT_WRITE = getEvfiltWrite();
        SIZEOF_KEVENT = getSizeofKevent();
        FD_OFFSET = getFdOffset();
        FILTER_OFFSET = getFilterOffset();
        FLAGS_OFFSET = getFlagsOffset();
        DATA_OFFSET = getDataOffset();
        EV_ADD = getEvAdd();
        EV_ONESHOT = getEvOneshot();
    }
}