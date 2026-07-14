package io.questdb.qwpbench;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Golden-value contract test. Every expected literal here is transcribed from
 * examples/bench_schema_c.c / bench_schema_c.h in c-questdb-client, and cross-checked
 * against the golden asserts in examples/qwp_bench_selftest.c. The C source is
 * normative — if this test and the C source ever disagree, the C source wins.
 */
class BenchSchemaTest {

    @Test
    void parseAndNames() {
        assertEquals(BenchSchema.Kind.S1_NARROW, BenchSchema.Kind.parse("s1-narrow"));
        assertEquals(BenchSchema.Kind.S2_WIDE, BenchSchema.Kind.parse("s2-wide"));
        assertEquals("bench_s1_narrow", BenchSchema.Kind.S1_NARROW.tableName());
        assertEquals("bench_s2_wide", BenchSchema.Kind.S2_WIDE.tableName());
        assertEquals(5, BenchSchema.Kind.S1_NARROW.columns());
        assertEquals(15, BenchSchema.Kind.S2_WIDE.columns());
        assertThrows(IllegalArgumentException.class, () -> BenchSchema.Kind.parse("bogus"));
    }

    @Test
    void ddlMatchesCContract() {
        // Byte-for-byte from schema_create_sql() / schema_select_sql() in
        // examples/bench_schema_c.c.
        assertEquals(
                "CREATE TABLE bench_s1_narrow (id LONG, price DOUBLE, sym SYMBOL, note VARCHAR, ts TIMESTAMP)" +
                        " TIMESTAMP(ts) PARTITION BY HOUR WAL DEDUP UPSERT KEYS(ts)",
                BenchSchema.Kind.S1_NARROW.createSql());
        assertEquals(
                "CREATE TABLE bench_s2_wide (id LONG, price DOUBLE, sym SYMBOL, note VARCHAR, " +
                        "d1 DOUBLE, d2 DOUBLE, d3 DOUBLE, d4 DOUBLE, d5 DOUBLE, " +
                        "s1 SYMBOL CAPACITY 200000, s2 SYMBOL CAPACITY 200000, " +
                        "s3 SYMBOL CAPACITY 200000, s4 SYMBOL CAPACITY 200000, " +
                        "s5 SYMBOL CAPACITY 200000, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY HOUR WAL DEDUP UPSERT KEYS(ts)",
                BenchSchema.Kind.S2_WIDE.createSql());
        assertEquals(
                "SELECT ts, id, price, sym, note FROM bench_s1_narrow",
                BenchSchema.Kind.S1_NARROW.selectSql());
        assertEquals(
                "SELECT ts, id, price, sym, note, d1, d2, d3, d4, d5, s1, s2, s3, s4, s5 FROM bench_s2_wide",
                BenchSchema.Kind.S2_WIDE.selectSql());
    }

    @Test
    void deterministicValues() {
        // ts_nanos: TS_BASE_NANOS + i * TS_STEP_NANOS
        assertEquals(1704067200000000000L, BenchSchema.tsNanos(0));
        assertEquals(1704067200000001000L, BenchSchema.tsNanos(1));

        // price: i * 0.25
        assertEquals(0.0, BenchSchema.price(0));
        assertEquals(1.0, BenchSchema.price(4));

        // sym: dictionary label(i % card), "sym_%04d"
        assertEquals("sym_0003", BenchSchema.sym(3, 8));
        assertEquals("sym_0001", BenchSchema.sym(9, 8)); // 9 % 8 == 1

        // note templates: clamp(rows, 1, 1024); char-cycled "note_%03d_" pattern
        assertEquals(1, BenchSchema.noteTemplateCount(1));
        assertEquals(1024, BenchSchema.noteTemplateCount(10_000_000));
        assertEquals(1024, BenchSchema.noteTemplateCount(50_000));
        assertEquals(8, BenchSchema.noteTemplateCount(8));
        assertEquals("note_000_note_00", BenchSchema.noteTemplate(0, 16));
        assertEquals("note_007_", BenchSchema.noteTemplate(7, 9));
        assertEquals("note_1023_no", BenchSchema.noteTemplate(1023, 12));
        assertEquals("note_1023_note_1", BenchSchema.noteTemplate(1023, 16));

        // wide_double: i * (0.5 + k)
        assertEquals(3.0, BenchSchema.wideDouble(2, 1));
        assertEquals(17.5, BenchSchema.wideDouble(5, 3));

        // hi_sym: column s1 -> label prefix s0_, wraps at hi_sym_card
        assertEquals("s0_000007", BenchSchema.hiSym(1, 7, 100000));
        assertEquals("s4_000001", BenchSchema.hiSym(5, 100001, 100000)); // 100001 % 100000 == 1
        assertEquals("s0_000005", BenchSchema.hiSym(1, 5, 1000));
        assertEquals("s4_099999", BenchSchema.hiSym(5, 99999, 1000000));
    }

    @Test
    void labelPoolsMatchPerRowGenerators() {
        // symPool: pool[v] == sym(v, card); pool[(int) (i % card)] must
        // reproduce the old per-row sym(i, card) call for any row index i.
        String[] symPool = BenchSchema.symPool(8);
        assertEquals(8, symPool.length);
        assertEquals("sym_0000", symPool[0]);
        assertEquals("sym_0007", symPool[7]);
        for (long i : new long[]{0, 3, 9, 100_001, 9_999_999L, 3_000_000_007L}) {
            assertEquals(BenchSchema.sym(i, 8), symPool[(int) (i % 8)]);
        }

        // hiSymPools: 1-based outer index like IngressBench.S_NAMES;
        // pools[c][v] == hiSym(c, v, card).
        String[][] hiPools = BenchSchema.hiSymPools(1000);
        assertEquals(BenchSchema.N_WIDE_SYMS + 1, hiPools.length);
        assertNull(hiPools[0]);
        assertEquals("s0_000000", hiPools[1][0]);
        assertEquals("s4_000999", hiPools[5][999]);
        for (int c = 1; c <= BenchSchema.N_WIDE_SYMS; c++) {
            assertEquals(1000, hiPools[c].length);
            for (long i : new long[]{0, 7, 999, 1000, 123_456, 9_999_999L}) {
                assertEquals(BenchSchema.hiSym(c, i, 1000), hiPools[c][(int) (i % 1000)]);
            }
        }
    }
}
