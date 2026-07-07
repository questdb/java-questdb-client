package com.example.query;

import io.questdb.client.Query;
import io.questdb.client.QueryException;
import io.questdb.client.QuestDB;
import io.questdb.client.cutlass.qwp.client.QwpColumnBatch;
import io.questdb.client.cutlass.qwp.client.QwpColumnBatchHandler;
import io.questdb.client.cutlass.qwp.protocol.QwpConstants;

import java.util.UUID;

/**
 * Tour of every typed bind setter {@code QwpBindValues} exposes. Each
 * placeholder is cast to the target SQL type in the projection so the server
 * tags the returned column identically to the bind.
 * <p>
 * Types covered: BOOLEAN, BYTE, SHORT, CHAR, INT, LONG, FLOAT, DOUBLE, DATE,
 * TIMESTAMP, TIMESTAMP_NANOS, VARCHAR, UUID, LONG256, GEOHASH, DECIMAL64,
 * DECIMAL128, DECIMAL256.
 * <p>
 * Each bind goes over the wire as {@code type_code(1B) | null_flag(1B) | value}
 * -- typed, not string-interpolated, so UUID / DECIMAL / TIMESTAMP_NANOS round
 * trip without escaping and the server's SQL-text factory cache stays warm
 * across calls that differ only in bind values.
 */
public class BindAllTypesExample {

    public static void main(String[] args) throws InterruptedException {
        String sql = "SELECT "
                + "$1::BOOLEAN          AS c_bool, "
                + "$2::BYTE             AS c_byte, "
                + "$3::SHORT            AS c_short, "
                + "$4::CHAR             AS c_char, "
                + "$5::INT              AS c_int, "
                + "$6::LONG             AS c_long, "
                + "$7::FLOAT            AS c_float, "
                + "$8::DOUBLE           AS c_double, "
                + "$9::DATE             AS c_date, "
                + "$10::TIMESTAMP       AS c_ts, "
                + "$11::TIMESTAMP_NS    AS c_ts_ns, "
                + "$12::VARCHAR         AS c_varchar, "
                + "$13::UUID            AS c_uuid, "
                + "$14::LONG256         AS c_long256, "
                + "cast($15 as GEOHASH(60b)) AS c_geohash, "
                + "$16::DECIMAL(18, 4)  AS c_dec64, "
                + "$17::DECIMAL(38, 6)  AS c_dec128, "
                + "$18::DECIMAL(76, 10) AS c_dec256 "
                + "FROM long_sequence(1)";

        try (QuestDB db = QuestDB.connect("ws::addr=localhost:9000;");
             Query q = db.borrowQuery()) {
            try {
                q.sql(sql)
                        .binds(binds -> binds
                                .setBoolean(0, true)
                                .setByte(1, (byte) 42)
                                .setShort(2, (short) 1234)
                                .setChar(3, 'Q')
                                .setInt(4, 2_000_000)
                                .setLong(5, 9_000_000_000L)
                                .setFloat(6, 3.14f)
                                .setDouble(7, 2.718281828)
                                .setDate(8, 1_700_000_000_000L)         // ms since epoch
                                .setTimestampMicros(9, 1_700_000_000_000_000L)   // us since epoch
                                .setTimestampNanos(10, 1_700_000_000_123_456_789L) // ns since epoch
                                .setVarchar(11, "café")
                                .setUuid(12, UUID.fromString("123e4567-e89b-12d3-a456-426614174000"))
                                .setLong256(13, 0x1111111111111111L, 0x2222222222222222L,
                                        0x3333333333333333L, 0x4444444444444444L)
                                .setGeohash(14, 60, 0x0FFF_FFFF_FFFF_FFFFL)
                                // Decimal unscaled values.
                                // 12345.6789  at scale 4  -> unscaled 123_456_789
                                // 123456789.123456 at scale 6 -> unscaled 123_456_789_123_456
                                // 42.0 at scale 10 -> unscaled 420_000_000_000
                                .setDecimal64(15, 4, 123_456_789L)
                                .setDecimal128(16, 6, 123_456_789_123_456L, 0L)
                                .setDecimal256(17, 10, 420_000_000_000L, 0L, 0L, 0L))
                        .handler(new QwpColumnBatchHandler() {
                            @Override
                            public void onBatch(QwpColumnBatch batch) {
                                System.out.println("row count: " + batch.getRowCount());
                                System.out.println("columns:   " + batch.getColumnCount());
                                for (int c = 0; c < batch.getColumnCount(); c++) {
                                    System.out.printf("  col %2d: wire type=0x%02X%n",
                                            c, batch.getColumnWireType(c) & 0xFF);
                                }
                                System.out.println("bool    : " + batch.getBoolValue(0, 0));
                                System.out.println("byte    : " + batch.getByteValue(1, 0));
                                System.out.println("short   : " + batch.getShortValue(2, 0));
                                System.out.println("char    : " + batch.getCharValue(3, 0));
                                System.out.println("int     : " + batch.getIntValue(4, 0));
                                System.out.println("long    : " + batch.getLongValue(5, 0));
                                System.out.println("float   : " + batch.getDoubleValue(6, 0));
                                System.out.println("double  : " + batch.getDoubleValue(7, 0));
                                System.out.println("date    : " + batch.getLongValue(8, 0));
                                System.out.println("ts_us   : " + batch.getLongValue(9, 0));
                                System.out.println("ts_ns   : " + batch.getLongValue(10, 0));
                                System.out.println("varchar : " + batch.getString(11, 0));
                                System.out.printf("uuid    : lo=%016x hi=%016x%n",
                                        batch.getUuidLo(12, 0), batch.getUuidHi(12, 0));
                                System.out.println("geohash wire type: 0x"
                                        + Integer.toHexString(batch.getColumnWireType(14) & 0xFF));
                                // Decimal128/Decimal256 surface via dedicated accessors:
                                System.out.printf("dec64   : scale=%d unscaled=%d%n",
                                        batch.getDecimalScale(15), batch.getLongValue(15, 0));
                                System.out.printf("dec128  : scale=%d lo=%d hi=%d%n",
                                        batch.getDecimalScale(16),
                                        batch.getDecimal128Low(16, 0),
                                        batch.getDecimal128High(16, 0));
                                System.out.println("dec256 wire type: 0x"
                                        + Integer.toHexString(batch.getColumnWireType(17) & 0xFF));
                                // Sanity -- dec256 wire type:
                                if (batch.getColumnWireType(17) != QwpConstants.TYPE_DECIMAL256) {
                                    throw new AssertionError("expected DECIMAL256");
                                }
                            }

                            @Override
                            public void onEnd(long totalRows) {
                            }

                            @Override
                            public void onError(byte status, String message) {
                            }
                        })
                        .submit()
                        .await();
            } catch (QueryException e) {
                System.err.printf("query failed: status=0x%02X %s%n", e.getStatus() & 0xFF, e.getMessage());
            }
        }
    }
}
