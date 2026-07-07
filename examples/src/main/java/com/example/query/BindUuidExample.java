package com.example.query;

import io.questdb.client.Query;
import io.questdb.client.QueryException;
import io.questdb.client.QuestDB;
import io.questdb.client.cutlass.qwp.client.QwpColumnBatch;
import io.questdb.client.cutlass.qwp.client.QwpColumnBatchHandler;

import java.util.UUID;

/**
 * Binding UUIDs. The encoder offers two overloads:
 * <p>
 * 1. {@code setUuid(index, lo, hi)} takes the two 64-bit limbs directly.
 *    The wire format is {@code lo | hi}, each little-endian. Use this when
 *    you carry UUIDs in long-pair form on the hot path and want zero
 *    allocation.
 * <p>
 * 2. {@code setUuid(index, java.util.UUID)} takes a boxed UUID. The client
 *    extracts {@code getLeastSignificantBits()} as {@code lo} and
 *    {@code getMostSignificantBits()} as {@code hi} -- the mapping that
 *    QuestDB's UUID type uses internally. A {@code null} UUID is encoded
 *    as an explicit NULL bind.
 * <p>
 * Assumes a table exists:
 * <pre>
 *   CREATE TABLE sessions (
 *       session_id UUID,
 *       user_name  VARCHAR,
 *       started_at TIMESTAMP
 *   ) TIMESTAMP(started_at) PARTITION BY DAY WAL;
 * </pre>
 */
public class BindUuidExample {

    public static void main(String[] args) throws InterruptedException {
        try (QuestDB db = QuestDB.connect("ws::addr=localhost:9000;");
             Query q = db.borrowQuery()) {
            UUID id = UUID.fromString("123e4567-e89b-12d3-a456-426614174000");

            // Convenience overload: pass java.util.UUID directly.
            System.out.println("lookup by java.util.UUID convenience overload");
            try {
                q.sql("SELECT user_name, started_at FROM sessions WHERE session_id = $1")
                        .binds(binds -> binds.setUuid(0, id))
                        .handler(new QwpColumnBatchHandler() {
                            @Override
                            public void onBatch(QwpColumnBatch batch) {
                                batch.forEachRow(row -> System.out.printf(
                                        "  user=%s started_at=%d%n",
                                        row.getString(0), row.getLongValue(1)
                                ));
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

            // Zero-alloc limb form: if you already carry the UUID as (lo, hi).
            long lo = id.getLeastSignificantBits();
            long hi = id.getMostSignificantBits();
            System.out.println("lookup by (lo, hi) limb overload");
            try {
                q.sql("SELECT user_name, started_at FROM sessions WHERE session_id = $1")
                        .binds(binds -> binds.setUuid(0, lo, hi))
                        .handler(new QwpColumnBatchHandler() {
                            @Override
                            public void onBatch(QwpColumnBatch batch) {
                                batch.forEachRow(row -> System.out.printf(
                                        "  user=%s started_at=%d%n",
                                        row.getString(0), row.getLongValue(1)
                                ));
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

            // Projecting a UUID back via the batch's UUID accessors.
            System.out.println("project a UUID back");
            try {
                q.sql("SELECT $1::UUID AS u FROM long_sequence(1)")
                        .binds(binds -> binds.setUuid(0, id))
                        .handler(new QwpColumnBatchHandler() {
                            @Override
                            public void onBatch(QwpColumnBatch batch) {
                                long gotLo = batch.getUuidLo(0, 0);
                                long gotHi = batch.getUuidHi(0, 0);
                                UUID got = new UUID(gotHi, gotLo);
                                System.out.println("  got: " + got);
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
