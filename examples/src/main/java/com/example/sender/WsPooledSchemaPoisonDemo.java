package com.example.sender;

import io.questdb.client.LineSenderServerException;
import io.questdb.client.QuestDB;
import io.questdb.client.Sender;
import io.questdb.client.SenderError;
import io.questdb.client.SenderErrorHandler;
import io.questdb.client.cutlass.qwp.client.QwpColumnBatch;
import io.questdb.client.cutlass.qwp.client.QwpColumnBatchHandler;
import io.questdb.client.cutlass.qwp.client.QwpQueryClient;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Shows that a schema rejection fails one pooled-sender borrow without
 * poisoning the underlying store-and-forward slot.
 *
 * See {@code examples/POOLED_SF_POISON_DEMO.md} for setup and invocation.
 */
public final class WsPooledSchemaPoisonDemo {

    private static final String SENDER_ID = "java-schema-poison-demo";
    private static final String TABLE = "java_sfa_poison_demo";
    private static final long WAIT_MILLIS = 10_000;

    private WsPooledSchemaPoisonDemo() {
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 1 || args.length > 2) {
            printUsageAndExit();
        }

        Path sfDir = Paths.get(args[0]).toAbsolutePath().normalize();
        String address = args.length == 2 ? args[1] : "localhost:9000";
        rejectConfigSeparators(sfDir, address);

        Path slotDir = sfDir.resolve(SENDER_ID + "-0");
        if (Files.exists(slotDir)) {
            throw new IllegalStateException(
                    "The demo slot already exists: " + slotDir + ". Use a new empty directory.");
        }

        resetTable(address);
        runDemo(address, sfDir);
    }

    private static void runDemo(String address, Path sfDir) throws Exception {
        CountDownLatch reportReady = new CountDownLatch(1);
        AtomicReference<SenderError> report = new AtomicReference<>();

        try (QuestDB db = createOneSlotPool(address, sfDir, error -> {
            if (error.getCategory() == SenderError.Category.SCHEMA_MISMATCH) {
                report.compareAndSet(null, error);
                reportReady.countDown();
            }
        })) {
            System.out.println("First borrow: sending a STRING into the LONG column.");
            LineSenderServerException rejection = sendRejectedRow(db);
            requireSchemaMismatch(rejection);
            printRejection("Owning borrow failed", rejection.getServerError());

            if (!reportReady.await(WAIT_MILLIS, TimeUnit.MILLISECONDS)) {
                throw new IllegalStateException("Timed out waiting for the schema-rejection report");
            }
            SenderError preserved = report.get();
            if (preserved == null || preserved.getRejectedPath() == null
                    || preserved.getAppliedPolicy() != SenderError.Policy.REJECT_AND_CONTINUE
                    || preserved.getFromFsn() != rejection.getServerError().getFromFsn()
                    || preserved.getToFsn() != rejection.getServerError().getToFsn()
                    || !Files.isDirectory(Paths.get(preserved.getRejectedPath()))) {
                throw new IllegalStateException("The rejected range was not preserved", rejection);
            }
            printRejection("Asynchronous preserved-copy report", preserved);
            System.out.println("Rejected bytes were preserved at " + preserved.getRejectedPath());

            System.out.println("Second borrow: sending a valid LONG row through the same one-slot pool.");
            try (Sender healthy = db.borrowSender()) {
                healthy.table(TABLE)
                        .longColumn("value", 42)
                        .symbol("marker", "good-after-rejection")
                        .atNow();
                long fsn = healthy.flushAndGetSequence();
                if (!healthy.awaitAckedFsn(fsn, WAIT_MILLIS)) {
                    throw new IllegalStateException("Timed out waiting for the valid row [fsn=" + fsn + ']');
                }
            }
        }

        long delivered = awaitGoodRows(address);
        if (delivered != 1) {
            throw new IllegalStateException("Expected one valid row [count=" + delivered + ']');
        }
        System.out.println("SUCCESS: returning the failed borrow kept the slot usable; the valid row was delivered.");
    }

    private static QuestDB createOneSlotPool(
            String address,
            Path sfDir,
            SenderErrorHandler errorHandler
    ) {
        String config = "ws::addr=" + address + ';'
                + "sf_dir=" + sfDir + ';'
                + "sender_id=" + SENDER_ID + ';'
                + "sf_durability=periodic;"
                + "sf_sync_interval_millis=1;"
                + "close_flush_timeout_millis=0;";

        return QuestDB.builder()
                .fromConfig(config)
                .senderPoolSize(1)
                .queryPoolMin(0)
                .queryPoolMax(1)
                .acquireTimeoutMillis(3_000)
                .errorHandler(errorHandler)
                .build();
    }

    private static long awaitGoodRows(String address) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(WAIT_MILLIS);
        long count;
        do {
            count = countGoodRows(address);
            if (count > 0) {
                return count;
            }
            Thread.sleep(50);
        } while (System.nanoTime() < deadline);
        return count;
    }

    private static LineSenderServerException sendRejectedRow(QuestDB db) {
        Sender sender = db.borrowSender();
        LineSenderServerException rejection = null;
        try {
            sender.table(TABLE)
                    .stringColumn("value", "not-a-long")
                    .symbol("marker", "bad")
                    .atNow();
            long fsn = sender.flushAndGetSequence();
            if (!sender.awaitAckedFsn(fsn, WAIT_MILLIS)) {
                throw new IllegalStateException("Timed out waiting for schema rejection [fsn=" + fsn + ']');
            }
            throw new IllegalStateException("The bad row was unexpectedly accepted");
        } catch (LineSenderServerException expected) {
            rejection = expected;
        } finally {
            try {
                sender.close();
            } catch (LineSenderServerException closeRejection) {
                if (rejection == null) {
                    rejection = closeRejection;
                }
            }
        }
        return rejection;
    }

    private static void resetTable(String address) {
        execute(address, "DROP TABLE IF EXISTS " + TABLE, new NoRowsHandler());
        execute(address,
                "CREATE TABLE " + TABLE
                        + " (value LONG, marker SYMBOL, ts TIMESTAMP)"
                        + " TIMESTAMP(ts) PARTITION BY DAY WAL",
                new NoRowsHandler());
    }

    private static long countGoodRows(String address) {
        final long[] count = {Long.MIN_VALUE};
        execute(address,
                "SELECT count() FROM " + TABLE + " WHERE marker = 'good-after-rejection'",
                new QwpColumnBatchHandler() {
                    @Override
                    public void onBatch(QwpColumnBatch batch) {
                        if (batch.getRowCount() > 0) {
                            count[0] = batch.getLongValue(0, 0);
                        }
                    }

                    @Override
                    public void onEnd(long totalRows) {
                    }

                    @Override
                    public void onError(byte status, String message) {
                        throw new IllegalStateException(
                                String.format("Verification query failed [status=0x%02X, message=%s]",
                                        status & 0xFF,
                                        message));
                    }
                });
        if (count[0] == Long.MIN_VALUE) {
            throw new IllegalStateException("The verification query returned no count");
        }
        return count[0];
    }

    private static void execute(String address, String sql, QwpColumnBatchHandler handler) {
        try (QwpQueryClient client = QwpQueryClient.fromConfig("ws::addr=" + address + ';')) {
            client.connect();
            client.execute(sql, handler);
        }
    }

    private static void requireSchemaMismatch(LineSenderServerException rejection) {
        if (rejection == null
                || rejection.getServerError().getCategory() != SenderError.Category.SCHEMA_MISMATCH
                || rejection.getServerError().getAppliedPolicy() != SenderError.Policy.REJECT_AND_CONTINUE) {
            throw new IllegalStateException("The first borrow did not fail with REJECT_AND_CONTINUE", rejection);
        }
    }

    private static void printRejection(String prefix, SenderError error) {
        System.out.printf(
                "%s: category=%s policy=%s fsn=[%d..%d] message=%s%n",
                prefix,
                error.getCategory(),
                error.getAppliedPolicy(),
                error.getFromFsn(),
                error.getToFsn(),
                error.getServerMessage());
    }

    private static void rejectConfigSeparators(Path sfDir, String address) {
        if (sfDir.toString().indexOf(';') >= 0 || address.indexOf(';') >= 0) {
            throw new IllegalArgumentException("The address and sf-dir must not contain ';'");
        }
    }

    private static void printUsageAndExit() {
        System.err.println("Usage: WsPooledSchemaPoisonDemo <sf-dir> [host:port]");
        System.exit(2);
    }

    private static final class NoRowsHandler implements QwpColumnBatchHandler {
        @Override
        public void onBatch(QwpColumnBatch batch) {
            throw new IllegalStateException("DDL unexpectedly returned rows");
        }

        @Override
        public void onEnd(long totalRows) {
        }

        @Override
        public void onError(byte status, String message) {
            throw new IllegalStateException(
                    String.format("DDL failed [status=0x%02X, message=%s]", status & 0xFF, message));
        }
    }
}
