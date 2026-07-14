package com.example.query;

import io.questdb.client.Query;
import io.questdb.client.QueryException;
import io.questdb.client.QuestDB;
import io.questdb.client.cutlass.qwp.client.QwpColumnBatch;
import io.questdb.client.cutlass.qwp.client.QwpColumnBatchHandler;

/**
 * Minimal QWP egress query example.
 * <p>
 * Opens a pooled {@link QuestDB} handle over QWP (WebSocket), borrows a
 * {@link Query} to run a SELECT, and prints each row as the batches
 * arrive. {@code Completion.await()} blocks until the query finishes and
 * rethrows any server error as a {@link QueryException}.
 * <p>
 * Iterates rows via {@link QwpColumnBatch#forEachRow}, which hands a reusable
 * row-pinned view to the lambda. Single-arg accessors keep the read path
 * compact; the underlying batch is still column-major and the {@code (col, row)}
 * primitives remain available on {@code batch} for callers that prefer them.
 * <p>
 * Queries the {@code trades} table the ingest examples
 * ({@code com.example.sender.WsExample}, {@code com.example.QuestDBExample})
 * write -- auto-created on first ingest as:
 * <pre>
 *   CREATE TABLE trades (
 *       symbol SYMBOL, side SYMBOL, price DOUBLE, amount DOUBLE, timestamp TIMESTAMP
 *   ) TIMESTAMP(timestamp) PARTITION BY DAY WAL;
 * </pre>
 */
public class BasicQueryExample {

    public static void main(String[] args) throws InterruptedException {
        try (QuestDB db = QuestDB.connect("ws::addr=localhost:9000;")) {
            try (Query q = db.borrowQuery()) {
                q.sql(
                        "SELECT timestamp, symbol, side, price, amount FROM trades WHERE symbol = 'ETH-USD' LIMIT 1000")
                        .handler(new QwpColumnBatchHandler() {
                            @Override
                            public void onBatch(QwpColumnBatch batch) {
                                // The RowView handed to the lambda is reusable and pinned to the
                                // current row; copy values out before the callback returns if you
                                // need to retain them past the surrounding onBatch call.
                                batch.forEachRow(row -> {
                                    long timestamp = row.getLongValue(0);   // TIMESTAMP -> microseconds since epoch
                                    String symbol = row.getSymbol(1);       // SYMBOL -> String
                                    String side = row.getSymbol(2);         // SYMBOL -> String
                                    double price = row.getDoubleValue(3);   // DOUBLE
                                    double amount = row.getDoubleValue(4);  // DOUBLE

                                    System.out.printf(
                                            "timestamp=%d symbol=%s side=%s price=%.4f amount=%.5f%n",
                                            timestamp, symbol, side, price, amount
                                    );
                                });
                            }

                            @Override
                            public void onEnd(long totalRows) {
                                System.out.println("query finished");
                            }

                            @Override
                            public void onError(byte status, String message) {
                            }
                        }
                ).submit().await();
            } catch (QueryException e) {
                System.err.printf("query failed: status=0x%02X %s%n", e.getStatus() & 0xFF, e.getMessage());
            }
        }
    }
}
