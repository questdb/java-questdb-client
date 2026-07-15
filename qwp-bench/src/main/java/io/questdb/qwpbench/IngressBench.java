package io.questdb.qwpbench;

import io.questdb.client.Sender;

import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * Row-API ingress bench over QWP/WebSocket, mirroring the cadence of
 * examples/qwp_ingress_c.c in c-questdb-client: append rows per batch, flush
 * per {@code MAX_BATCH_ROWS}, ack checkpoint every {@link #CHECKPOINT_BATCHES}
 * batches, final ack wait. {@code RUN_MODE} selects {@code full} (default),
 * {@code e2e}, or {@code floor}. Java's {@link Sender} has no offline
 * wire-staging API, so the row-build floor is measured via
 * {@link #floorPass}: single-threaded, {@code reset()} per batch through the
 * connected Sender's own buffer -- unlike the Rust twin's standalone offline
 * {@code Buffer}, so this floor includes inline global-symbol-dict resolution
 * and requires a live server. Floor mode still creates the table and
 * connects all {@code SENDERS}.
 */
public final class IngressBench {
    static final int CHECKPOINT_BATCHES = (int) Env.zu("CHECKPOINT_BATCHES", 64);
    static final long ACK_TIMEOUT_MS = 120_000;
    // Precomputed column names for S2_WIDE's wide symbol/double columns --
    // avoids a per-row "s" + c / "d" + k allocation in the timed pass() loop.
    // Mirrors Rust's S_NAMES/D_NAMES in qwp_ingress_row.rs.
    private static final String[] S_NAMES = names("s", BenchSchema.N_WIDE_SYMS);
    private static final String[] D_NAMES = names("d", BenchSchema.N_WIDE_DOUBLES);

    private static String[] names(String prefix, int count) {
        String[] out = new String[count + 1];
        for (int i = 1; i <= count; i++) out[i] = prefix + i;
        return out;
    }

    /**
     * Sender {@code k} (0-based) of {@code senders} owns global rows
     * {@code [rows*k/senders, rows*(k+1)/senders)}. Multiply-first long math:
     * ranges tile [0, rows) exactly for any senders count (empty ranges when
     * senders > rows are legal no-ops). Same split as the Rust/C twins'
     * sender_range(s).
     */
    static long[] senderRange(long rows, int senders, int k) {
        return new long[]{rows * k / senders, rows * (k + 1) / senders};
    }

    public static int run() throws Exception {
        BenchSchema.Kind kind = BenchSchema.Kind.parse(Env.str("SCHEMA", "s1-narrow"));
        long rows = Env.zu("ROWS", 10_000_000);
        int symCard = (int) Env.zu("QUESTDB_COLUMN_BENCH_SYM_CARD", 8);
        int varcharLen = (int) Env.zu("QUESTDB_COLUMN_BENCH_VARCHAR_LEN", 16);
        int hiCard = (int) Env.zu("HI_SYM_CARD", 100_000);
        int iterations = (int) Env.zu("ITERATIONS", 5);
        int warmups = (int) Env.zu("WARMUPS", 2);
        long maxBatch = Env.zu("MAX_BATCH_ROWS", 10_000);
        int senders = (int) Env.zu("SENDERS", 1);
        if (senders < 1) senders = 1;
        String host = Env.str("QDB_HOST", "127.0.0.1");
        long port = Env.zu("QDB_PORT", 9000);
        String table = kind.tableName();
        String base = "http://" + host + ":" + port;
        // Store-and-forward stays default memory mode (no sf_dir) and
        // transactional stays default off: both are opt-in builder settings
        // with no config-string key set here, so the defaults already apply.
        String conf = ingestConf(host, port);

        System.err.printf("[qwp_ingress_java] schema=%s rows=%d it=%d wu=%d batch=%d host=%s:%d senders=%d%n",
                kind, rows, iterations, warmups, maxBatch, host, port, senders);

        BenchHttp http = new BenchHttp(base);
        http.execSql("DROP TABLE IF EXISTS " + table);
        http.execSql(kind.createSql());

        long[] wall = new long[iterations];
        long[] cpu = new long[iterations];
        long[] gc = new long[iterations];
        // note values cycle over min(rows, 1024) templates; precompute
        int tcount = BenchSchema.noteTemplateCount(rows);
        String[] notes = new String[tcount];
        for (int t = 0; t < tcount; t++) notes[t] = BenchSchema.noteTemplate(t, varcharLen);
        // Label pools -- built once, outside every timed region, shared
        // read-only across sender threads. The timed loop then does O(1)
        // array lookups only; per-row String.format for the up-to-6 symbol
        // labels would otherwise be roughly half the measured pass.
        String[] symPool = BenchSchema.symPool(symCard);
        String[][] hiPools = kind == BenchSchema.Kind.S2_WIDE ? BenchSchema.hiSymPools(hiCard) : null;

        String runMode = Env.str("RUN_MODE", "full");
        boolean doFloor = !"e2e".equals(runMode);
        boolean doE2e = !"floor".equals(runMode);

        Sender[] pool = new Sender[senders];
        long[][] ranges = new long[senders][];
        long[] wallF = new long[iterations];
        long[] cpuF = new long[iterations];
        long[] gcF = new long[iterations];
        ExecutorService exec = senders > 1 ? Executors.newFixedThreadPool(senders) : null;
        try {
            for (int k = 0; k < senders; k++) {
                pool[k] = Sender.fromConfig(conf);
                ranges[k] = senderRange(rows, senders, k);
            }
            if (doFloor) {
                System.err.println("[qwp_ingress_java] measuring row-build floor ...");
                for (int w = 0; w < warmups; w++) {
                    floorPass(pool[0], kind, rows, symPool, hiPools, notes, maxBatch);
                }
                for (int it = 0; it < iterations; it++) {
                    long g0 = BenchJson.gcMs(), c0 = BenchJson.processCpuNs(), t0 = BenchJson.nowNs();
                    floorPass(pool[0], kind, rows, symPool, hiPools, notes, maxBatch);
                    wallF[it] = BenchJson.nowNs() - t0;
                    cpuF[it] = BenchJson.processCpuNs() - c0;
                    gcF[it] = BenchJson.gcMs() - g0;
                }
            }
            if (doE2e) {
                for (int w = 0; w < warmups; w++) {
                    multiPass(exec, pool, ranges, kind, symPool, hiPools, notes, maxBatch);
                }
                for (int it = 0; it < iterations; it++) {
                    long g0 = BenchJson.gcMs(), c0 = BenchJson.processCpuNs(), t0 = BenchJson.nowNs();
                    multiPass(exec, pool, ranges, kind, symPool, hiPools, notes, maxBatch);
                    wall[it] = BenchJson.nowNs() - t0;
                    cpu[it] = BenchJson.processCpuNs() - c0;
                    gc[it] = BenchJson.gcMs() - g0;
                }
            }
        } finally {
            if (exec != null) exec.shutdownNow();
            for (Sender s : pool) {
                if (s != null) s.close();
            }
        }
        long count = doE2e ? http.waitForCount(table, rows) : 0;

        BenchJson.Obj report = new BenchJson.Obj();
        report.put("schema", kind == BenchSchema.Kind.S1_NARROW ? "s1-narrow" : "s2-wide");
        report.put("rows", rows);
        report.put("columns", (long) kind.columns());
        report.put("direction", "ingress");
        report.put("client", "java-row");
        report.put("run_mode", runMode);
        report.put("warmups", (long) warmups);
        report.put("wire_bytes", 0L);
        report.put("senders", (long) senders);
        BenchJson.Obj machine = new BenchJson.Obj();
        machine.put("platform", System.getProperty("os.name").toLowerCase().contains("linux") ? "linux" : "macos");
        machine.put("arch", System.getProperty("os.arch"));
        machine.put("jvm", System.getProperty("java.vm.version"));
        report.put("machine", machine);
        BenchJson.Obj commits = new BenchJson.Obj();
        String jc = System.getenv("JAVA_QUESTDB_CLIENT_COMMIT");
        if (jc != null) commits.put("java_questdb_client", jc); else commits.putNull("java_questdb_client");
        report.put("commits", commits);
        if (doE2e) {
            BenchJson.Obj rcc = new BenchJson.Obj();
            rcc.put("expected", rows);
            rcc.put("actual", count);
            rcc.put("ok", count == rows);
            rcc.put("inflated", count > rows);
            report.put("row_count_check", rcc);
        }
        double e2e = BenchJson.medianS(wall, iterations);
        BenchJson.Obj headline = new BenchJson.Obj();
        headline.put("row_flush_s", e2e);
        if (e2e != 0.0) headline.put("row_flush_rows_per_s", rows / e2e);
        report.put("headline", headline);
        report.put("real_conf", conf);
        report.put("http_base", base);
        BenchJson.Obj paths = new BenchJson.Obj();
        if (doE2e) {
            paths.put("row-flush", BenchJson.pathSummary(wall, cpu, gc, iterations, rows, kind.columns(), 0, "e2e", warmups > 0));
        }
        if (doFloor) {
            paths.put("row-build", BenchJson.pathSummary(wallF, cpuF, gcF, iterations, rows,
                    kind.columns(), 0, "floor", warmups > 0));
        }
        report.put("paths", paths);
        System.out.println(report.render());
        return (!doE2e || count == rows) ? 0 : 2;
    }

    /**
     * Ingest-mode {@code ws::} conf shared by {@link #run()} and
     * {@link EgressBench}'s populate step. The WS engine's
     * {@code validateParameters()} rejects fully disabling auto-flush --
     * {@code autoFlushIntervalMillis == Integer.MAX_VALUE} throws
     * "disabling auto-flush is not supported for WebSocket protocol"
     * (Sender.java), which is what {@code auto_flush=off} maps to internally
     * -- so auto-flush is instead pinned to the largest values the ws:: conf
     * parser accepts: {@code Integer.MAX_VALUE} rows and
     * {@code Integer.MAX_VALUE - 1} ms (exactly {@code MAX_VALUE} ms collides
     * with the "off" sentinel and would trip the same rejection). Both are
     * unreachable within a single batch, so the measured cadence stays
     * caller-driven via the explicit {@code flushAndGetSequence()} per batch
     * below, with an ack checkpoint every {@link #CHECKPOINT_BATCHES}
     * batches.
     */
    static String ingestConf(String host, long port) {
        return "ws::addr=" + host + ":" + port
                + ";auto_flush_rows=2147483647;auto_flush_interval=2147483646;";
    }

    /**
     * Appends one row's worth of columns to {@code sender} without flushing.
     * Shared by {@link #pass} (e2e) and {@link #floorPass} (floor) so the two
     * paths stay byte-identical in what per-row work they measure.
     */
    private static void appendRow(Sender sender, BenchSchema.Kind kind, long i,
                                  String[] symPool, String[][] hiPools, String[] notes) {
        sender.table(kind.tableName());
        sender.symbol("sym", symPool[(int) (i % symPool.length)]);
        if (kind == BenchSchema.Kind.S2_WIDE) {
            for (int c = 1; c <= BenchSchema.N_WIDE_SYMS; c++) {
                sender.symbol(S_NAMES[c], hiPools[c][(int) (i % hiPools[c].length)]);
            }
        }
        sender.longColumn("id", BenchSchema.id(i));
        sender.doubleColumn("price", BenchSchema.price(i));
        sender.stringColumn("note", notes[(int) (i % notes.length)]);
        if (kind == BenchSchema.Kind.S2_WIDE) {
            for (int k = 1; k <= BenchSchema.N_WIDE_DOUBLES; k++) {
                sender.doubleColumn(D_NAMES[k], BenchSchema.wideDouble(i, k));
            }
        }
        sender.at(BenchSchema.tsNanos(i), ChronoUnit.NANOS);
    }

    /** Floor "row-build": appendRow per row, reset() per batch, no flush/acks.
     *  Single-threaded over [0, rows) regardless of SENDERS (parity with rust
     *  measure_row_build). NOTE: unlike rust's standalone Buffer, this exercises
     *  a connected Sender's internal buffer; connection stays idle. */
    private static void floorPass(Sender sender, BenchSchema.Kind kind, long rows,
                                  String[] symPool, String[][] hiPools, String[] notes, long maxBatch) {
        for (long start = 0; start < rows; start += maxBatch) {
            long end = Math.min(start + maxBatch, rows);
            for (long i = start; i < end; i++) {
                appendRow(sender, kind, i, symPool, hiPools, notes);
            }
            sender.reset();
        }
    }

    // package-private: EgressBench reuses this for its populate step.
    // symPool/hiPools are read-only and safely shared across sender threads;
    // hiPools is null iff kind != S2_WIDE (never dereferenced then).
    static void pass(Sender sender, BenchSchema.Kind kind, long lo, long hi,
                      String[] symPool, String[][] hiPools, String[] notes, long maxBatch) throws Exception {
        long batchNo = 0;
        long lastFsn = -1;
        for (long start = lo; start < hi; start += maxBatch) {
            long end = Math.min(start + maxBatch, hi);
            for (long i = start; i < end; i++) {
                appendRow(sender, kind, i, symPool, hiPools, notes);
            }
            lastFsn = sender.flushAndGetSequence();
            if (++batchNo % CHECKPOINT_BATCHES == 0 && lastFsn >= 0) {
                if (!sender.awaitAckedFsn(lastFsn, ACK_TIMEOUT_MS)) {
                    throw new RuntimeException(
                            "[qwp_ingress_java] checkpoint ack timed out for fsn=" + lastFsn
                                    + " after " + ACK_TIMEOUT_MS + "ms");
                }
            }
        }
        if (lastFsn >= 0) {
            if (!sender.awaitAckedFsn(lastFsn, ACK_TIMEOUT_MS)) {
                throw new RuntimeException(
                        "[qwp_ingress_java] final ack timed out for fsn=" + lastFsn
                                + " after " + ACK_TIMEOUT_MS + "ms");
            }
        }
    }

    /**
     * One e2e pass over all senders. senders == 1 runs inline (no executor
     * hop in the timed region); otherwise each sender's range runs on the
     * shared pool. The completion loop awaits EVERY worker before returning
     * or throwing — no sender is still mid-flush when the caller's finally
     * closes the pool — then rethrows the first failure with any others
     * attached as suppressed exceptions.
     */
    private static void multiPass(ExecutorService exec, Sender[] pool, long[][] ranges,
                                  BenchSchema.Kind kind, String[] symPool, String[][] hiPools,
                                  String[] notes, long maxBatch) throws Exception {
        if (pool.length == 1) {
            pass(pool[0], kind, ranges[0][0], ranges[0][1], symPool, hiPools, notes, maxBatch);
            return;
        }
        List<Future<?>> futures = new ArrayList<>(pool.length);
        for (int k = 0; k < pool.length; k++) {
            final int kk = k;
            futures.add(exec.submit(() -> {
                pass(pool[kk], kind, ranges[kk][0], ranges[kk][1], symPool, hiPools, notes, maxBatch);
                return null;
            }));
        }
        Exception first = null;
        for (Future<?> f : futures) {
            try {
                f.get();
            } catch (Exception e) {
                if (first == null) {
                    first = e;
                } else {
                    first.addSuppressed(e);
                }
            }
        }
        if (first != null) {
            throw first;
        }
    }

    private IngressBench() {}
}
