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

import io.questdb.client.std.Decimals;
import io.questdb.client.std.IntHashSet;
import io.questdb.client.std.IntObjHashMap;
import io.questdb.client.std.Long256;
import io.questdb.client.std.LowerCaseAsciiCharSequenceIntHashMap;
import io.questdb.client.std.Numbers;
import io.questdb.client.std.str.StringSink;

// ColumnType layout - 32bit
//
// | Handling bit | Extra type information | Timestamp Flag | GeoHash Flag | Extra type information | Type discriminant (tag) |
// +--------------+------------------------+----------------+--------------+------------------------+-------------------------+
// |    1 bit     |        13 bits         |     1 bit      |    1 bit     |         8 bits         |         8 bits          |
// +--------------+------------------------+----------------+--------------+------------------------+-------------------------+
//
// Handling bit:
//   Skip column use case:
//       The top bit is set for columns that should be skipped.
//       I.e. `if (columnType < 0) { skip }`.
//   PG Wire Format use case:
//       Reserved for bit-shifting operations as part of `PGOids` to
//       determine if a PG Wire column should be handled as text or binary.
//       Also see `bindSelectColumnFormats` and `bindVariableTypes` in
//       `PGConnectionContext`.

/**
 * Column types as numeric (integer) values
 */
public final class ColumnType {
    public static final int ARRAY_NDIMS_LIMIT = 32; // inclusive
    public static final String[] ARRAY_DIM_SUFFIX = new String[ARRAY_NDIMS_LIMIT + 1];
    public static final int GEOLONG_MAX_BITS = 60;
    public static final int MIGRATION_VERSION = 426;
    // our type system is absolutely ordered ranging
    // - from UNDEFINED: index 0, represents lack of type, an internal parsing concept.
    // - to NULL: index must be last, other parts of the codebase rely on this fact.
    public static final short UNDEFINED = 0;                    // = 0
    public static final short BOOLEAN = UNDEFINED + 1;          // = 1
    public static final short BYTE = BOOLEAN + 1;               // = 2
    public static final short SHORT = BYTE + 1;                 // = 3
    public static final short CHAR = SHORT + 1;                 // = 4
    public static final short INT = CHAR + 1;                   // = 5
    public static final short LONG = INT + 1;                   // = 6
    public static final short DATE = LONG + 1;                  // = 7
    public static final short TIMESTAMP = DATE + 1;             // = 8
    public static final short FLOAT = TIMESTAMP + 1;            // = 9
    public static final short DOUBLE = FLOAT + 1;               // = 10
    public static final short STRING = DOUBLE + 1;              // = 11
    public static final short SYMBOL = STRING + 1;              // = 12
    public static final short LONG256 = SYMBOL + 1;             // = 13
    public static final short GEOBYTE = LONG256 + 1;            // = 14
    public static final short GEOSHORT = GEOBYTE + 1;           // = 15
    public static final short GEOINT = GEOSHORT + 1;            // = 16
    public static final short GEOLONG = GEOINT + 1;             // = 17
    public static final short BINARY = GEOLONG + 1;             // = 18
    public static final short UUID = BINARY + 1;                // = 19
    public static final short CURSOR = UUID + 1;                // = 20
    public static final short VAR_ARG = CURSOR + 1;             // = 21
    public static final short RECORD = VAR_ARG + 1;             // = 22
    // GEOHASH is not stored. It is used on function
    // arguments to resolve overloads. We also build
    // overload matrix, which logic relies on GEOHASH
    // value >UUID and <MAX.
    public static final short GEOHASH = RECORD + 1;             // = 23
    public static final short LONG128 = GEOHASH + 1;            // = 24  Limited support, few tests only
    public static final short IPv4 = LONG128 + 1;               // = 25
    public static final short VARCHAR = IPv4 + 1;               // = 26
    public static final short ARRAY = VARCHAR + 1;              // = 27
    // Similarly to GeoHash, Decimal is separated in 2 kinds of type:
    //  - Stored ones with the number of bits used in the suffix (selected from the precision
    // through getStorageSize).
    //  - Unstored one that is used as a surrogate to resolve decimal functions.
    //
    // Stored decimal uses the Extra type information to store the precision
    // and scale, giving this layout:
    //        31        30~24     23~16       15~8                7~0
    // +--------------+--------+----------+-----------+-------------------------+
    // | Handling bit | Scale  | Reserved | Precision | Type discriminant (tag) |
    // +--------------+--------+----------+-----------+-------------------------+
    // |    1 bit     | 8 bits |  7 bits  |  8 bits   |         8 bits          |
    // +--------------+--------+----------+-----------+-------------------------+
    public static final short DECIMAL8 = ARRAY + 1;     // = 28;
    public static final short DECIMAL16 = DECIMAL8 + 1;    // = 29;
    public static final short DECIMAL32 = DECIMAL16 + 1;   // = 30;
    public static final short DECIMAL64 = DECIMAL32 + 1;   // = 31;
    public static final short DECIMAL128 = DECIMAL64 + 1;  // = 32;
    public static final short DECIMAL256 = DECIMAL128 + 1; // = 33;
    public static final short DECIMAL = DECIMAL256 + 1;    // = 34;
    // PG specific types to work with 3rd party software
    // with canned catalogue queries:
    // REGCLASS, REGPROCEDURE, ARRAY_STRING, PARAMETER
    public static final short REGCLASS = DECIMAL + 1;            // = 35;
    public static final short REGPROCEDURE = REGCLASS + 1;     // = 36;
    public static final short ARRAY_STRING = REGPROCEDURE + 1; // = 37;
    public static final short PARAMETER = ARRAY_STRING + 1;    // = 38;
    public static final short INTERVAL = PARAMETER + 1;        // = 39;
    public static final short NULL = INTERVAL + 1;          // = 40; ALWAYS the last
    private static final short[] TYPE_SIZE = new short[NULL + 1];
    private static final short[] TYPE_SIZE_POW2 = new short[TYPE_SIZE.length];
    public static final int INTERVAL_RAW = INTERVAL;
    public static final int INTERVAL_TIMESTAMP_MICRO = INTERVAL | 1 << 17;
    public static final int INTERVAL_TIMESTAMP_NANO = INTERVAL | 1 << 18;
    public static final int TIMESTAMP_MICRO = TIMESTAMP;
    public static final int TIMESTAMP_NANO = 1 << 18 | TIMESTAMP;
    // column type version as written to the metadata file
    public static final int VERSION = 426;
    static final int[] GEO_TYPE_SIZE_POW2;

    private static final int ARRAY_ELEMTYPE_FIELD_MASK = 0x3F;
    private static final int ARRAY_ELEMTYPE_FIELD_POS = 8;
    private static final int ARRAY_NDIMS_FIELD_MASK = ARRAY_NDIMS_LIMIT - 1;
    private static final int ARRAY_NDIMS_FIELD_POS = 14;
    private static final int BYTE_BITS = 8;
    private static final int TYPE_FLAG_ARRAY_WEAK_DIMS = (1 << 19);

    private static final int TYPE_FLAG_GEO_HASH = (1 << 16);
    private static final IntHashSet arrayTypeSet = new IntHashSet();
    private static final LowerCaseAsciiCharSequenceIntHashMap nameTypeMap = new LowerCaseAsciiCharSequenceIntHashMap();
    private static final IntObjHashMap<String> typeNameMap = new IntObjHashMap<>();

    private ColumnType() {
    }

    public static int decodeArrayDimensionality(int encodedType) {
        final int dims = ColumnType.decodeWeakArrayDimensionality(encodedType);
        assert dims > 0;
        return dims;
    }

    /**
     * Returns the int constant denoting the type of the elements in an array of the given encoded type.
     */
    public static short decodeArrayElementType(int encodedType) {
        if (ColumnType.isNull(encodedType)) {
            return ColumnType.NULL;
        }
        assert ColumnType.isArray(encodedType) : "typeTag of encodedType is not ARRAY";
        return (short) ((encodedType >> ARRAY_ELEMTYPE_FIELD_POS) & ARRAY_ELEMTYPE_FIELD_MASK);
    }

    /**
     * Returns the number of dimensions for the given array type or -1 in case of an array with weak dimensionality,
     * e.g. array type of bind variable.
     */
    public static int decodeWeakArrayDimensionality(int encodedType) {
        if (ColumnType.isNull(encodedType)) {
            return 0;
        }
        assert ColumnType.isArray(encodedType) : "typeTag of encodedType is not ARRAY";
        if ((encodedType & TYPE_FLAG_ARRAY_WEAK_DIMS) != 0) {
            return -1;
        }
        return ((encodedType >> ARRAY_NDIMS_FIELD_POS) & ARRAY_NDIMS_FIELD_MASK) + 1;
    }

    public static int encodeArrayType(short elemType, int nDims) {
        return encodeArrayType(elemType, nDims, true);
    }

    /**
     * Encodes the array type tag from the element type tag and dimensionality.
     * <br>
     * The encoded type is laid out as follows:
     * <pre>
     *     31~20      19        18~14       13~8           7~0
     * +----------+----------+----------+-----------+------------------+
     * | Reserved | WeakDims |  nDims   | elemType  | ColumnType.ARRAY |
     * +----------+----------+----------+-----------+------------------+
     * |          |  1 bit   |  5 bits  |  6 bits   |      8 bits      |
     * +----------+----------+----------+-----------+------------------+
     * </pre>
     * <p>
     * WeakDims bit (19): When set, indicates the dimensionality is tentative and
     * can be updated based on actual data. This is useful for PostgreSQL wire
     * protocol where type information doesn't include array dimensions.
     *
     * @param elemType one of the supported array element type tags.
     * @param nDims    dimensionality, from 1 to {@value ARRAY_NDIMS_LIMIT}.
     */
    public static int encodeArrayType(int elemType, int nDims, boolean checkSupportedElementTypes) {
        assert nDims >= 1 && nDims <= ARRAY_NDIMS_LIMIT : "nDims out of range: " + nDims;
        assert !checkSupportedElementTypes || (isSupportedArrayElementType(elemType) || elemType == UNDEFINED)
                : "not supported as array element type: " + nameOf(elemType);

        nDims--; // 0 == one dimension
        return (nDims & ARRAY_NDIMS_FIELD_MASK) << ARRAY_NDIMS_FIELD_POS
                | (elemType & ARRAY_ELEMTYPE_FIELD_MASK) << ARRAY_ELEMTYPE_FIELD_POS
                | ARRAY;
    }

    /**
     * Encodes an array type with weak dimensionality. The dimensionality is still
     * encoded but marked as tentative and can be updated based on actual data.
     * This is useful for PostgreSQL wire protocol where type information doesn't
     * include array dimensions.
     * <p>
     * The number of dimensions of this type is undefined, so the decoded number on
     * dimensions for the returned column type will be -1.
     */
    public static int encodeArrayTypeWithWeakDims(short elemType, boolean checkSupportedElementTypes) {
        return encodeArrayType(elemType, 1, checkSupportedElementTypes) | TYPE_FLAG_ARRAY_WEAK_DIMS;
    }

    /**
     * Generate a decimal type from a given precision and scale.
     * It will choose the proper subtype (DECIMAL8, DECIMAL16, etc.) from the precision, depending on the amount
     * of storage needed to store a number with the given precision.
     *
     * @param precision to be encoded in the decimal type
     * @param scale     to be encoded in the decimal type
     * @return the generated type as an int
     */
    public static int getDecimalType(int precision, int scale) {
        assert precision > 0 && precision <= Decimals.MAX_PRECISION;
        assert scale >= 0 && scale <= Decimals.MAX_SCALE;
        int size = Decimals.getStorageSizePow2(precision);
        // Construct the type following the layout described earlier.
        // DECIMAL8-256 needs to be clustered together for this to work.
        return ((scale & 0xFF) << 18) | ((precision & 0xFF) << 8) | (DECIMAL8 + size);
    }

    public static int getGeoHashTypeWithBits(int bits) {
        assert bits > 0 && bits <= GEOLONG_MAX_BITS;
        // this logic relies on GeoHash type value to be clustered together
        return mkGeoHashType(bits, (short) (GEOBYTE + pow2SizeOfBits(bits)));
    }

    /**
     * Is an N-dimensional array type.
     */
    public static boolean isArray(int columnType) {
        return ColumnType.tagOf(columnType) == ColumnType.ARRAY;
    }

    public static boolean isNull(int columnType) {
        return columnType == NULL;
    }

    public static boolean isSupportedArrayElementType(int typeTag) {
        return arrayTypeSet.contains(typeTag);
    }

    public static String nameOf(int columnType) {
        final int index = typeNameMap.keyIndex(columnType);
        if (index > -1) {
            return "unknown";
        }
        return typeNameMap.valueAtQuick(index);
    }

    public static int pow2SizeOf(int columnType) {
        return TYPE_SIZE_POW2[tagOf(columnType)];
    }

    public static int pow2SizeOfBits(int bits) {
        assert bits <= GEOLONG_MAX_BITS;
        return GEO_TYPE_SIZE_POW2[bits];
    }

    public static int sizeOf(int columnType) {
        short tag = tagOf(columnType);
        if (tag < TYPE_SIZE.length) {
            return sizeOfTag(tag);
        }
        return -1;
    }

    public static int sizeOfTag(short tag) {
        return TYPE_SIZE[tag];
    }

    public static short tagOf(int type) {
        if (type == -1) {
            return (short) type;
        }
        return (short) (type & 0xFF);
    }

    public static int typeOf(CharSequence name) {
        return nameTypeMap.get(name);
    }

    private static void addArrayTypeName(StringSink sink, short type) {
        sink.clear();
        sink.put(nameOf(type));
        for (int d = 1; d <= ARRAY_NDIMS_LIMIT; d++) {
            sink.put("[]");
            int arrayType = encodeArrayType(type, d, false);
            String name = sink.toString();
            typeNameMap.put(arrayType, name);
            nameTypeMap.put(name, arrayType);
        }
    }

    private static int mkGeoHashType(int bits, short baseType) {
        return (baseType & ~(0xFF << BYTE_BITS)) | (bits << BYTE_BITS) | TYPE_FLAG_GEO_HASH; // bit 16 is GeoHash flag
    }

    static {
        assert MIGRATION_VERSION >= VERSION;
        GEO_TYPE_SIZE_POW2 = new int[GEOLONG_MAX_BITS + 1];
        for (int bits = 1; bits <= GEOLONG_MAX_BITS; bits++) {
            GEO_TYPE_SIZE_POW2[bits] = Numbers.msb(Numbers.ceilPow2(((bits + Byte.SIZE) & -Byte.SIZE)) >> 3);
        }

        typeNameMap.put(BOOLEAN, "BOOLEAN");
        typeNameMap.put(BYTE, "BYTE");
        typeNameMap.put(DOUBLE, "DOUBLE");
        typeNameMap.put(FLOAT, "FLOAT");
        typeNameMap.put(INT, "INT");
        typeNameMap.put(LONG, "LONG");
        typeNameMap.put(SHORT, "SHORT");
        typeNameMap.put(CHAR, "CHAR");
        typeNameMap.put(STRING, "STRING");
        typeNameMap.put(VARCHAR, "VARCHAR");
        typeNameMap.put(ARRAY, "ARRAY");
        typeNameMap.put(SYMBOL, "SYMBOL");
        typeNameMap.put(BINARY, "BINARY");
        typeNameMap.put(DATE, "DATE");
        typeNameMap.put(PARAMETER, "PARAMETER");
        typeNameMap.put(TIMESTAMP_MICRO, "TIMESTAMP");
        typeNameMap.put(TIMESTAMP_NANO, "TIMESTAMP_NS");
        typeNameMap.put(LONG256, "LONG256");
        typeNameMap.put(UUID, "UUID");
        typeNameMap.put(LONG128, "LONG128");
        typeNameMap.put(CURSOR, "CURSOR");
        typeNameMap.put(RECORD, "RECORD");
        typeNameMap.put(VAR_ARG, "VARARG");
        typeNameMap.put(GEOHASH, "GEOHASH");
        typeNameMap.put(REGCLASS, "regclass");
        typeNameMap.put(REGPROCEDURE, "regprocedure");
        typeNameMap.put(ARRAY_STRING, "text[]");
        typeNameMap.put(IPv4, "IPv4");
        typeNameMap.put(INTERVAL, "INTERVAL");
        typeNameMap.put(INTERVAL_RAW, "INTERVAL");
        typeNameMap.put(INTERVAL_TIMESTAMP_MICRO, "INTERVAL");
        typeNameMap.put(INTERVAL_TIMESTAMP_NANO, "INTERVAL");
        typeNameMap.put(DECIMAL, "DECIMAL");
        typeNameMap.put(NULL, "NULL");

        arrayTypeSet.add(DOUBLE);

        TYPE_SIZE_POW2[UNDEFINED] = -1;
        TYPE_SIZE_POW2[BOOLEAN] = 0;
        TYPE_SIZE_POW2[BYTE] = 0;
        TYPE_SIZE_POW2[SHORT] = 1;
        TYPE_SIZE_POW2[CHAR] = 1;
        TYPE_SIZE_POW2[FLOAT] = 2;
        TYPE_SIZE_POW2[INT] = 2;
        TYPE_SIZE_POW2[IPv4] = 2;
        TYPE_SIZE_POW2[SYMBOL] = 2;
        TYPE_SIZE_POW2[DOUBLE] = 3;
        TYPE_SIZE_POW2[STRING] = -1;
        TYPE_SIZE_POW2[VARCHAR] = -1;
        TYPE_SIZE_POW2[ARRAY] = -1;
        TYPE_SIZE_POW2[LONG] = 3;
        TYPE_SIZE_POW2[DATE] = 3;
        TYPE_SIZE_POW2[TIMESTAMP] = 3;
        TYPE_SIZE_POW2[LONG256] = 5;
        TYPE_SIZE_POW2[GEOBYTE] = 0;
        TYPE_SIZE_POW2[GEOSHORT] = 1;
        TYPE_SIZE_POW2[GEOINT] = 2;
        TYPE_SIZE_POW2[GEOLONG] = 3;
        TYPE_SIZE_POW2[BINARY] = -1;
        TYPE_SIZE_POW2[PARAMETER] = -1;
        TYPE_SIZE_POW2[CURSOR] = -1;
        TYPE_SIZE_POW2[VAR_ARG] = -1;
        TYPE_SIZE_POW2[RECORD] = -1;
        TYPE_SIZE_POW2[NULL] = -1;
        TYPE_SIZE_POW2[LONG128] = 4;
        TYPE_SIZE_POW2[UUID] = 4;
        TYPE_SIZE_POW2[DECIMAL8] = 0;
        TYPE_SIZE_POW2[DECIMAL16] = 1;
        TYPE_SIZE_POW2[DECIMAL32] = 2;
        TYPE_SIZE_POW2[DECIMAL64] = 3;
        TYPE_SIZE_POW2[DECIMAL128] = 4;
        TYPE_SIZE_POW2[DECIMAL256] = 5;
        TYPE_SIZE_POW2[INTERVAL] = 4;

        TYPE_SIZE[UNDEFINED] = -1;
        TYPE_SIZE[BOOLEAN] = Byte.BYTES;
        TYPE_SIZE[BYTE] = Byte.BYTES;
        TYPE_SIZE[SHORT] = Short.BYTES;
        TYPE_SIZE[CHAR] = Character.BYTES;
        TYPE_SIZE[FLOAT] = Float.BYTES;
        TYPE_SIZE[INT] = Integer.BYTES;
        TYPE_SIZE[IPv4] = Integer.BYTES;
        TYPE_SIZE[SYMBOL] = Integer.BYTES;
        TYPE_SIZE[STRING] = 0;
        TYPE_SIZE[VARCHAR] = 0;
        TYPE_SIZE[ARRAY] = 0;
        TYPE_SIZE[DOUBLE] = Double.BYTES;
        TYPE_SIZE[LONG] = Long.BYTES;
        TYPE_SIZE[DATE] = Long.BYTES;
        TYPE_SIZE[TIMESTAMP] = Long.BYTES;
        TYPE_SIZE[LONG256] = Long256.BYTES;
        TYPE_SIZE[GEOBYTE] = Byte.BYTES;
        TYPE_SIZE[GEOSHORT] = Short.BYTES;
        TYPE_SIZE[GEOINT] = Integer.BYTES;
        TYPE_SIZE[GEOLONG] = Long.BYTES;
        TYPE_SIZE[BINARY] = 0;
        TYPE_SIZE[PARAMETER] = -1;
        TYPE_SIZE[CURSOR] = -1;
        TYPE_SIZE[VAR_ARG] = -1;
        TYPE_SIZE[RECORD] = -1;
        TYPE_SIZE[UUID] = 2 * Long.BYTES;
        TYPE_SIZE[NULL] = 0;
        TYPE_SIZE[LONG128] = 2 * Long.BYTES;
        TYPE_SIZE[DECIMAL8] = Byte.BYTES;
        TYPE_SIZE[DECIMAL16] = Short.BYTES;
        TYPE_SIZE[DECIMAL32] = Integer.BYTES;
        TYPE_SIZE[DECIMAL64] = Long.BYTES;
        TYPE_SIZE[DECIMAL128] = 2 * Long.BYTES;
        TYPE_SIZE[DECIMAL256] = 4 * Long.BYTES;
        TYPE_SIZE[INTERVAL] = 2 * Long.BYTES;

        nameTypeMap.put("boolean", BOOLEAN);
        nameTypeMap.put("byte", BYTE);
        nameTypeMap.put("double", DOUBLE);
        nameTypeMap.put("float", FLOAT);
        nameTypeMap.put("int", INT);
        nameTypeMap.put("integer", INT);
        nameTypeMap.put("long", LONG);
        nameTypeMap.put("short", SHORT);
        nameTypeMap.put("char", CHAR);
        nameTypeMap.put("string", STRING);
        nameTypeMap.put("varchar", VARCHAR);
        nameTypeMap.put("array", ARRAY);
        nameTypeMap.put("symbol", SYMBOL);
        nameTypeMap.put("binary", BINARY);
        nameTypeMap.put("date", DATE);
        nameTypeMap.put("parameter", PARAMETER);
        nameTypeMap.put("timestamp", TIMESTAMP_MICRO);
        nameTypeMap.put("cursor", CURSOR);
        nameTypeMap.put("long256", LONG256);
        nameTypeMap.put("uuid", UUID);
        nameTypeMap.put("long128", LONG128);
        nameTypeMap.put("geohash", GEOHASH);
        nameTypeMap.put("text", STRING);
        nameTypeMap.put("smallint", SHORT);
        nameTypeMap.put("bigint", LONG);
        nameTypeMap.put("real", FLOAT);
        nameTypeMap.put("bytea", STRING);
        nameTypeMap.put("regclass", REGCLASS);
        nameTypeMap.put("regprocedure", REGPROCEDURE);
        nameTypeMap.put("text[]", ARRAY_STRING);
        nameTypeMap.put("IPv4", IPv4);
        nameTypeMap.put("interval", INTERVAL);
        nameTypeMap.put("interval", INTERVAL_TIMESTAMP_MICRO);
        nameTypeMap.put("timestamp_ns", TIMESTAMP_NANO);
        nameTypeMap.put("decimal", DECIMAL);

        StringSink sink = new StringSink();
        for (int b = 1; b <= GEOLONG_MAX_BITS; b++) {
            sink.clear();
            if (b % 5 != 0) {
                sink.put("GEOHASH(").put(b).put("b)");
            } else {
                sink.put("GEOHASH(").put(b / 5).put("c)");
            }
            String name = sink.toString();
            int type = getGeoHashTypeWithBits(b);
            typeNameMap.put(type, name);
            nameTypeMap.put(name, type);
        }

        addArrayTypeName(sink, ColumnType.BOOLEAN);
        addArrayTypeName(sink, ColumnType.BYTE);
        addArrayTypeName(sink, ColumnType.SHORT);
        addArrayTypeName(sink, ColumnType.INT);
        addArrayTypeName(sink, ColumnType.LONG);
        addArrayTypeName(sink, ColumnType.FLOAT);
        addArrayTypeName(sink, ColumnType.DOUBLE);
        addArrayTypeName(sink, ColumnType.LONG256);
        addArrayTypeName(sink, ColumnType.VARCHAR);
        addArrayTypeName(sink, ColumnType.STRING);
        addArrayTypeName(sink, ColumnType.IPv4);
        addArrayTypeName(sink, ColumnType.TIMESTAMP);
        addArrayTypeName(sink, ColumnType.UUID);
        addArrayTypeName(sink, ColumnType.DATE);

        // Stored decimals
        for (int precision = 1; precision <= Decimals.MAX_PRECISION; precision++) {
            for (int scale = 0; scale <= Decimals.MAX_SCALE; scale++) {
                int type = getDecimalType(precision, scale);
                sink.clear();
                sink.put("DECIMAL(").put(precision).put(',').put(scale).put(")");
                String name = sink.toString();
                typeNameMap.put(type, name);
                nameTypeMap.put(name, type);
            }
        }
    }
}