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

package io.questdb.client.cairo;

import io.questdb.client.std.Numbers;
import io.questdb.client.std.NumericException;
import io.questdb.client.std.str.CharSink;

public class GeoHashes {

    // geohash null value: -1
    // we use the highest bit of every storage size (byte, short, int, long)
    // to indicate null value. When a null value is cast down, nullity is
    // preserved, i.e. highest bit remains set:
    //     long nullLong = -1L;
    //     short nullShort = (short) nullLong;
    //     nullShort == nullLong;
    // in addition, -1 is the first negative non geohash value.
    public static final int MAX_STRING_LENGTH = 12;
    public static final long NULL = -1L;

    private static final char[] base32 = {
            '0', '1', '2', '3', '4', '5', '6', '7',
            '8', '9', 'b', 'c', 'd', 'e', 'f', 'g',
            'h', 'j', 'k', 'm', 'n', 'p', 'q', 'r',
            's', 't', 'u', 'v', 'w', 'x', 'y', 'z'
    };

    public static void append(long hash, int bits, CharSink<?> sink) {
        if (hash == GeoHashes.NULL) {
            sink.putAscii("null");
        } else {
            sink.putAscii('\"');
            if (bits < 0) {
                GeoHashes.appendCharsUnsafe(hash, -bits, sink);
            } else {
                GeoHashes.appendBinaryStringUnsafe(hash, bits, sink);
            }
            sink.putAscii('\"');
        }
    }

    public static void appendBinaryStringUnsafe(long hash, int bits, CharSink<?> sink) {
        // Below assertion can happen if there is corrupt metadata
        // which should not happen in production code since reader and writer check table metadata
        assert bits > 0 && bits <= ColumnType.GEOLONG_MAX_BITS;
        for (int i = bits - 1; i >= 0; --i) {
            sink.putAscii(((hash >> i) & 1) == 1 ? '1' : '0');
        }
    }

    public static void appendChars(long hash, int chars, CharSink<?> sink) {
        if (hash != NULL) {
            appendCharsUnsafe(hash, chars, sink);
        }
    }

    public static void appendCharsUnsafe(long hash, int chars, CharSink<?> sink) {
        // Below assertion can happen if there is corrupt metadata
        // which should not happen in production code since reader and writer check table metadata
        assert chars > 0 && chars <= MAX_STRING_LENGTH;
        for (int i = chars - 1; i >= 0; --i) {
            sink.putAscii(base32[(int) ((hash >> i * 5) & 0x1F)]);
        }
    }

    public static long fromCoordinatesDeg(double lat, double lon, int bits) throws NumericException {
        if (lat < -90.0 || lat > 90.0) {
            throw NumericException.instance();
        }
        if (lon < -180.0 || lon > 180.0) {
            throw NumericException.instance();
        }
        if (bits < 0 || bits > ColumnType.GEOLONG_MAX_BITS) {
            throw NumericException.instance();
        }
        return fromCoordinatesDegUnsafe(lat, lon, bits);
    }

    public static long fromCoordinatesDegUnsafe(double lat, double lon, int bits) {
        long latq = (long) Math.scalb((lat + 90.0) / 180.0, 32);
        long lngq = (long) Math.scalb((lon + 180.0) / 360.0, 32);
        return Numbers.interleaveBits(latq, lngq) >>> (64 - bits);
    }
}
