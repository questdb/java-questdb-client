package com.example;

import io.questdb.client.QueryException;
import io.questdb.client.QuestDB;
import io.questdb.client.Sender;
import io.questdb.client.cutlass.qwp.client.QwpColumnBatch;
import io.questdb.client.cutlass.qwp.client.QwpColumnBatchHandler;

/**
 * Pointing ingest and query at different endpoints.
 * <p>
 * The single-string {@link QuestDB#connect(CharSequence)} configures both
 * directions from one {@code ws}/{@code wss} string -- the common case. When
 * ingest and egress differ (ingest goes to the primary while reads target a
 * read-replica or a separate load balancer), pass two strings to
 * {@link QuestDB#connect(CharSequence, CharSequence)}, or use the builder's
 * {@code ingestConfig(...)} / {@code queryConfig(...)}. Each string still
 * accepts keys owned by the other direction; the split just lets the two sides
 * point at different hosts or carry different tuning.
 */
public class QuestDBSeparateConfigExample {

    public static void main(String[] args) throws InterruptedException {
        // First string is the ingest (Sender) config, second is the query
        // (egress) config. Both must use the ws/wss schema.
        try (QuestDB db = QuestDB.connect(
                "ws::addr=ingest.cluster:9000;",
                "wss::addr=read-replica.cluster:9000;token=YOUR_TOKEN;")) {

            try (Sender sender = db.borrowSender()) {
                sender.table("trades")
                        .symbol("symbol", "ETH-USD")
                        .doubleColumn("price", 2615.54)
                        .atNow();
            }

            try {
                db.executeSql("SELECT count() FROM trades", new QwpColumnBatchHandler() {
                    @Override
                    public void onBatch(QwpColumnBatch batch) {
                        batch.forEachRow(row -> System.out.println("count = " + row.getLongValue(0)));
                    }

                    @Override
                    public void onEnd(long totalRows) {
                    }

                    @Override
                    public void onError(byte status, String message) {
                        System.err.printf("error: 0x%02X %s%n", status & 0xFF, message);
                    }
                }).await();
            } catch (QueryException e) {
                System.err.printf("query failed: status=0x%02X %s%n", e.getStatus() & 0xFF, e.getMessage());
            }
        }

        // The connect string can also come from the QDB_CLIENT_CONF environment
        // variable (export QDB_CLIENT_CONF="wss::addr=db:9000;token=...;"):
        //
        //   String cfg = System.getenv("QDB_CLIENT_CONF");
        //   try (QuestDB db = QuestDB.connect(cfg)) { ... }
    }
}
