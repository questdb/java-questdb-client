package com.example.query;

import io.questdb.client.Query;
import io.questdb.client.QueryException;
import io.questdb.client.QuestDB;
import io.questdb.client.cutlass.qwp.client.QwpColumnBatch;
import io.questdb.client.cutlass.qwp.client.QwpColumnBatchHandler;
import io.questdb.client.cutlass.qwp.protocol.QwpConstants;

/**
 * Binding NULLs.
 * <p>
 * Two ways to bind an explicit NULL:
 * <ul>
 *   <li>{@code setNull(index, typeCode)} -- always works, takes the QWP wire
 *       type code from {@link QwpConstants}.</li>
 *   <li>{@code setVarchar(index, null)} and {@code setUuid(index, (UUID) null)}
 *       -- convenience shortcuts for the nullable reference types.</li>
 * </ul>
 * <p>
 * Note: SQL semantics mean you cannot normally match NULL with equality
 * (<code>col = NULL</code> is itself NULL and filters nothing). Use the
 * {@code IS NULL} / {@code IS NOT NULL} predicates when you want to find
 * rows with NULL columns. The bind side here is only about transmitting a
 * typed NULL value through the wire.
 * <p>
 * Assumes a table exists:
 * <pre>
 *   CREATE TABLE logs (
 *       id    LONG,
 *       note  VARCHAR,
 *       level INT,
 *       ts    TIMESTAMP
 *   ) TIMESTAMP(ts) PARTITION BY DAY WAL;
 * </pre>
 */
public class BindNullExample {

    public static void main(String[] args) throws InterruptedException {
        try (QuestDB db = QuestDB.connect("ws::addr=localhost:9000;");
             Query q = db.borrowQuery()) {

            // --- Convenience: setVarchar(index, null).
            // Use IS NULL / IS NOT NULL predicates since SQL equality with
            // NULL always yields NULL. $1 still needs a typed NULL so the
            // server can infer the intended type; the bind drives that.
            System.out.println("rows where note IS NOT NULL and level < some-null-placeholder:");
            try {
                q.sql("SELECT id, note FROM logs WHERE note IS NOT NULL AND level < COALESCE($1::INT, 100) LIMIT 10")
                        .binds(binds -> binds.setNull(0, QwpConstants.TYPE_INT))
                        .handler(printRowsHandler())
                        .submit()
                        .await();
            } catch (QueryException e) {
                System.err.printf("query failed: status=0x%02X %s%n", e.getStatus() & 0xFF, e.getMessage());
            }

            // --- Convenience: setUuid(index, (UUID) null).
            System.out.println("projection: NULL VARCHAR");
            try {
                q.sql("SELECT $1 AS v FROM long_sequence(1)")
                        .binds(binds -> binds.setVarchar(0, null))
                        .handler(new QwpColumnBatchHandler() {
                            @Override
                            public void onBatch(QwpColumnBatch batch) {
                                System.out.println("  v is null? " + batch.isNull(0, 0));
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

            // --- Explicit setNull for every scalar type (useful when you
            //     don't have a value-type overload, e.g. TIMESTAMP_NANOS).
            System.out.println("projection: NULL TIMESTAMP_NANOS");
            try {
                q.sql("SELECT CAST($1 AS TIMESTAMP_NS) AS ts FROM long_sequence(1)")
                        .binds(binds -> binds.setNull(0, QwpConstants.TYPE_TIMESTAMP_NANOS))
                        .handler(new QwpColumnBatchHandler() {
                            @Override
                            public void onBatch(QwpColumnBatch batch) {
                                System.out.println("  ts is null? " + batch.isNull(0, 0));
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

    private static QwpColumnBatchHandler printRowsHandler() {
        return new QwpColumnBatchHandler() {
            @Override
            public void onBatch(QwpColumnBatch batch) {
                batch.forEachRow(row -> System.out.printf(
                        "  id=%d note=%s%n", row.getLongValue(0), row.getString(1)
                ));
            }

            @Override
            public void onEnd(long totalRows) {
            }

            @Override
            public void onError(byte status, String message) {
            }
        };
    }
}
