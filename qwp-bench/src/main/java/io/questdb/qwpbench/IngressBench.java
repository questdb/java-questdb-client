package io.questdb.qwpbench;

import io.questdb.client.Sender;

import java.time.temporal.ChronoUnit;

/**
 * Row-API ingress bench over QWP/WebSocket, mirroring the cadence of
 * examples/qwp_ingress_c.c in c-questdb-client: append rows per batch, flush
 * per {@code MAX_BATCH_ROWS}, ack checkpoint every {@link #CHECKPOINT_BATCHES}
 * batches, final ack wait. Java's {@link Sender} has no offline wire-staging
 * API, so unlike the C twin this bench has a single e2e path ("row-flush");
 * the floor/e2e asymmetry versus C is documented via that single path in the
 * JSON report rather than measured directly.
 */
public final class IngressBench {
    static final int CHECKPOINT_BATCHES = 64;
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

    public static int run() throws Exception {
        BenchSchema.Kind kind = BenchSchema.Kind.parse(Env.str("SCHEMA", "s1-narrow"));
        long rows = Env.zu("ROWS", 10_000_000);
        int symCard = (int) Env.zu("QUESTDB_COLUMN_BENCH_SYM_CARD", 8);
        int varcharLen = (int) Env.zu("QUESTDB_COLUMN_BENCH_VARCHAR_LEN", 16);
        int hiCard = (int) Env.zu("HI_SYM_CARD", 100_000);
        int iterations = (int) Env.zu("ITERATIONS", 5);
        int warmups = (int) Env.zu("WARMUPS", 2);
        long maxBatch = Env.zu("MAX_BATCH_ROWS", 10_000);
        String host = Env.str("QDB_HOST", "127.0.0.1");
        long port = Env.zu("QDB_PORT", 9000);
        String table = kind.tableName();
        String base = "http://" + host + ":" + port;
        // Store-and-forward stays default memory mode (no sf_dir) and
        // transactional stays default off: both are opt-in builder settings
        // with no config-string key set here, so the defaults already apply.
        String conf = ingestConf(host, port);

        System.err.printf("[qwp_ingress_java] schema=%s rows=%d it=%d wu=%d batch=%d host=%s:%d%n",
                kind, rows, iterations, warmups, maxBatch, host, port);

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

        try (Sender sender = Sender.fromConfig(conf)) {
            for (int w = 0; w < warmups; w++) pass(sender, kind, rows, symCard, hiCard, notes, maxBatch);
            for (int it = 0; it < iterations; it++) {
                long g0 = BenchJson.gcMs(), c0 = BenchJson.processCpuNs(), t0 = BenchJson.nowNs();
                pass(sender, kind, rows, symCard, hiCard, notes, maxBatch);
                wall[it] = BenchJson.nowNs() - t0;
                cpu[it] = BenchJson.processCpuNs() - c0;
                gc[it] = BenchJson.gcMs() - g0;
            }
        }
        long count = http.waitForCount(table, rows);

        BenchJson.Obj report = new BenchJson.Obj();
        report.put("schema", kind == BenchSchema.Kind.S1_NARROW ? "s1-narrow" : "s2-wide");
        report.put("rows", rows);
        report.put("columns", (long) kind.columns());
        report.put("direction", "ingress");
        report.put("client", "java-row");
        report.put("run_mode", Env.str("RUN_MODE", "full"));
        report.put("warmups", (long) warmups);
        report.put("wire_bytes", 0L);
        BenchJson.Obj machine = new BenchJson.Obj();
        machine.put("platform", System.getProperty("os.name").toLowerCase().contains("linux") ? "linux" : "macos");
        machine.put("arch", System.getProperty("os.arch"));
        machine.put("jvm", System.getProperty("java.vm.version"));
        report.put("machine", machine);
        BenchJson.Obj commits = new BenchJson.Obj();
        String jc = System.getenv("JAVA_QUESTDB_CLIENT_COMMIT");
        if (jc != null) commits.put("java_questdb_client", jc); else commits.putNull("java_questdb_client");
        report.put("commits", commits);
        BenchJson.Obj rcc = new BenchJson.Obj();
        rcc.put("expected", rows);
        rcc.put("actual", count);
        rcc.put("ok", count == rows);
        rcc.put("inflated", count > rows);
        report.put("row_count_check", rcc);
        double e2e = BenchJson.medianS(wall, iterations);
        BenchJson.Obj headline = new BenchJson.Obj();
        headline.put("row_flush_s", e2e);
        if (e2e != 0.0) headline.put("row_flush_rows_per_s", rows / e2e);
        report.put("headline", headline);
        report.put("real_conf", conf);
        report.put("http_base", base);
        BenchJson.Obj paths = new BenchJson.Obj();
        paths.put("row-flush", BenchJson.pathSummary(wall, cpu, gc, iterations, rows, kind.columns(), 0, "e2e", warmups > 0));
        report.put("paths", paths);
        System.out.println(report.render());
        return count == rows ? 0 : 2;
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

    // package-private: EgressBench reuses this for its populate step (Task 5)
    static void pass(Sender sender, BenchSchema.Kind kind, long rows,
                      int symCard, int hiCard, String[] notes, long maxBatch) throws Exception {
        long batchNo = 0;
        long lastFsn = -1;
        for (long start = 0; start < rows; start += maxBatch) {
            long end = Math.min(start + maxBatch, rows);
            for (long i = start; i < end; i++) {
                sender.table(kind.tableName());
                // symbols must precede other columns (Sender contract)
                sender.symbol("sym", BenchSchema.sym(i, symCard));
                if (kind == BenchSchema.Kind.S2_WIDE) {
                    for (int c = 1; c <= BenchSchema.N_WIDE_SYMS; c++) {
                        sender.symbol(S_NAMES[c], BenchSchema.hiSym(c, i, hiCard));
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

    private IngressBench() {}
}
