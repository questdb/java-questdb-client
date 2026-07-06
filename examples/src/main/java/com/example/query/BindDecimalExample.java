package com.example.query;

import io.questdb.client.Query;
import io.questdb.client.QueryException;
import io.questdb.client.QuestDB;
import io.questdb.client.cutlass.qwp.client.QwpColumnBatch;
import io.questdb.client.cutlass.qwp.client.QwpColumnBatchHandler;
import io.questdb.client.std.Decimal128;
import io.questdb.client.std.Decimal256;

/**
 * Binding DECIMAL64 / DECIMAL128 / DECIMAL256.
 * <p>
 * QuestDB decimals are fixed-point: you ship a signed integer unscaled value
 * plus a scale. For DECIMAL(precision, scale), the stored form is
 * {@code unscaled / 10^scale}. DECIMAL64 holds up to 18 digits in a single
 * 64-bit signed integer; DECIMAL128 uses two 64-bit limbs; DECIMAL256 uses
 * four. The client's {@code setDecimal*} setters take the scale byte up
 * front and validate it against the server's {@code MAX_SCALE = 76}.
 * <p>
 * Two convenience overloads take an already-built {@code Decimal128} /
 * {@code Decimal256} value and extract the scale + limbs automatically;
 * useful when you already carry decimals as first-class objects.
 * <p>
 * Assumes a table exists:
 * <pre>
 *   CREATE TABLE invoices (
 *       id     LONG,
 *       amount DECIMAL(18, 2),   -- fits DECIMAL64
 *       tax    DECIMAL(38, 6),   -- requires DECIMAL128
 *       ts     TIMESTAMP
 *   ) TIMESTAMP(ts) PARTITION BY DAY WAL;
 * </pre>
 */
public class BindDecimalExample {

    public static void main(String[] args) throws InterruptedException {
        try (QuestDB db = QuestDB.connect("ws::addr=localhost:9000;");
             Query q = db.borrowQuery()) {

            // --- Example 1: DECIMAL64 amount, unscaled literal.
            // Looking up invoices where the total is exactly $1999.99 --
            // unscaled = 199_999 at scale 2.
            System.out.println("lookup by DECIMAL64 amount = 1999.99");
            try {
                q.sql("SELECT id FROM invoices WHERE amount = $1")
                        .binds(binds -> binds.setDecimal64(0, 2, 199_999L))
                        .handler(printIdHandler())
                        .submit()
                        .await();
            } catch (QueryException e) {
                System.err.printf("query failed: status=0x%02X %s%n", e.getStatus() & 0xFF, e.getMessage());
            }

            // --- Example 2: DECIMAL128 tax, via explicit scale + limbs.
            // Looking up invoices where tax equals 12.345678 --
            // unscaled = 12_345_678 at scale 6. Positive value fits the low
            // limb alone; high limb is zero.
            System.out.println("lookup by DECIMAL128 tax = 12.345678");
            try {
                q.sql("SELECT id FROM invoices WHERE tax = $1")
                        .binds(binds -> binds.setDecimal128(0, 6, 12_345_678L, 0L))
                        .handler(printIdHandler())
                        .submit()
                        .await();
            } catch (QueryException e) {
                System.err.printf("query failed: status=0x%02X %s%n", e.getStatus() & 0xFF, e.getMessage());
            }

            // --- Example 3: DECIMAL128 via Decimal128 convenience overload.
            // If you already carry the value as a Decimal128, skip the scale
            // + limb juggling -- the client reads them off the object.
            Decimal128 tax = Decimal128.fromLong(12_345_678L, 6);
            System.out.println("lookup by DECIMAL128 via Decimal128 convenience");
            try {
                q.sql("SELECT id FROM invoices WHERE tax = $1")
                        .binds(binds -> binds.setDecimal128(0, tax))
                        .handler(printIdHandler())
                        .submit()
                        .await();
            } catch (QueryException e) {
                System.err.printf("query failed: status=0x%02X %s%n", e.getStatus() & 0xFF, e.getMessage());
            }

            // --- Example 4: DECIMAL256 projection (no table needed).
            // 42.0 at scale 10 -> unscaled 420_000_000_000. Positive value so
            // only the lowest of the four limbs is set.
            System.out.println("project DECIMAL256 42.0 at scale 10");
            try {
                q.sql("SELECT $1::DECIMAL(76, 10) AS v FROM long_sequence(1)")
                        .binds(binds -> binds.setDecimal256(0, 10, 420_000_000_000L, 0L, 0L, 0L))
                        .handler(new QwpColumnBatchHandler() {
                            @Override
                            public void onBatch(QwpColumnBatch batch) {
                                System.out.println("  dec256 scale: " + batch.getDecimalScale(0));
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

            // --- Example 5: DECIMAL256 via Decimal256 convenience.
            Decimal256 big = new Decimal256(0L, 0L, 0L, 420_000_000_000L, 10);
            System.out.println("project DECIMAL256 via Decimal256 convenience");
            try {
                q.sql("SELECT $1::DECIMAL(76, 10) AS v FROM long_sequence(1)")
                        .binds(binds -> binds.setDecimal256(0, big))
                        .handler(new QwpColumnBatchHandler() {
                            @Override
                            public void onBatch(QwpColumnBatch batch) {
                                System.out.println("  dec256 scale: " + batch.getDecimalScale(0));
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

    private static QwpColumnBatchHandler printIdHandler() {
        return new QwpColumnBatchHandler() {
            @Override
            public void onBatch(QwpColumnBatch batch) {
                batch.forEachRow(row -> System.out.println("  id=" + row.getLongValue(0)));
            }

            @Override
            public void onEnd(long totalRows) {
                System.out.println("  total matched: " + totalRows);
            }

            @Override
            public void onError(byte status, String message) {
            }
        };
    }
}
