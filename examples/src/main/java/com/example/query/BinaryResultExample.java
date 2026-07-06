package com.example.query;

import io.questdb.client.Query;
import io.questdb.client.QueryException;
import io.questdb.client.QuestDB;
import io.questdb.client.Sender;
import io.questdb.client.cutlass.qwp.client.QwpColumnBatch;
import io.questdb.client.cutlass.qwp.client.QwpColumnBatchHandler;

import java.util.Arrays;

/**
 * BINARY column round-trip: ingest a {@code byte[]} and read it back.
 * <p>
 * Ingest with {@code binaryColumn(name, byte[])} (overloads also take a native
 * {@code (ptr, len)} pair or a {@code DirectByteSlice} for zero-copy paths).
 * On egress, {@link QwpColumnBatch#getBinary(int, int)} returns a heap
 * {@code byte[]}; {@code getBinaryA}/{@code getBinaryB} give reusable native
 * views for allocation-free reads.
 */
public class BinaryResultExample {

    public static void main(String[] args) throws InterruptedException {
        byte[] payload = {0x00, 0x01, 0x02, (byte) 0xFF};

        try (QuestDB db = QuestDB.connect("ws::addr=localhost:9000;")) {

            try (Sender sender = db.borrowSender()) {
                sender.table("blobs")
                        .symbol("kind", "thumbnail")
                        .binaryColumn("payload", payload)
                        .atNow();
            }

            try (Query q = db.borrowQuery()) {
                q.sql(
                        "SELECT kind, payload FROM blobs LIMIT 10")
                        .handler(new QwpColumnBatchHandler() {
                            @Override
                            public void onBatch(QwpColumnBatch batch) {
                                for (int row = 0; row < batch.getRowCount(); row++) {
                                    String kind = batch.getSymbol(0, row);
                                    byte[] bytes = batch.isNull(1, row) ? null : batch.getBinary(1, row);
                                    System.out.println("kind=" + kind + " payload=" + Arrays.toString(bytes));
                                }
                            }

                            @Override
                            public void onEnd(long totalRows) {
                                System.out.println("done: " + totalRows + " rows");
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
