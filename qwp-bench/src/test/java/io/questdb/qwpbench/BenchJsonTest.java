package io.questdb.qwpbench;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class BenchJsonTest {
    @Test void f64TrimsLikeC() {
        assertEquals("0.333333333333", BenchJson.f64(1.0 / 3.0));
        assertEquals("2.5", BenchJson.f64(2.5));
        assertEquals("1", BenchJson.f64(1.0));
        assertEquals("0.516397779494", BenchJson.f64(0.5163977794943222));
    }
    @Test void f64RoundsExactlyLikeCPrintf() {
        // String.format("%.12f") is not correctly rounded in rare cases; C printf is.
        // Reviewer counterexample: the exact binary value rounds to ...709, not ...710.
        assertEquals("4287.413072708709", BenchJson.f64(4287.4130727087095));
        // C printf preserves the sign when a negative value rounds to zero -> "-0".
        assertEquals("-0", BenchJson.f64(-1e-13));
    }
    @Test void mibPerSBranchesLikeCPathSummary() {
        long[] wall = {2_000_000_000L};
        long[] cpu = {1_000_000_000L};
        long[] gc = {0};
        // Floor phase: mib_per_s is JSON null regardless of wireBytes (C passes NULL rate ptr).
        String floor = BenchJson.pathSummary(wall, cpu, gc, 1, 10, 5, 2097152L, "floor", true).render();
        assertTrue(floor.contains("\"mib_per_s\":null"), floor);
        // E2e phase with wireBytes == 0: literal 0, not null (C passes a valid ptr to a 0 value).
        String e2e = BenchJson.pathSummary(wall, cpu, gc, 1, 10, 5, 0, "e2e", true).render();
        assertTrue(e2e.contains("\"mib_per_s\":0,"), e2e);
        assertFalse(e2e.contains("\"mib_per_s\":null"), e2e);
        // E2e phase with wireBytes > 0: computed rate (2 MiB over 2 s -> 1 MiB/s).
        String e2eRate = BenchJson.pathSummary(wall, cpu, gc, 1, 10, 5, 2097152L, "e2e", true).render();
        assertTrue(e2eRate.contains("\"mib_per_s\":1,"), e2eRate);
    }
    @Test void sortedKeysAndEscapes() {
        BenchJson.Obj o = new BenchJson.Obj();
        o.put("b", 1L);
        o.put("a", "x");
        o.put("d", 1.0 / 3.0);
        o.put("c", 2.5);
        assertEquals("{\"a\":\"x\",\"b\":1,\"c\":2.5,\"d\":0.333333333333}", o.render());
        BenchJson.Obj e = new BenchJson.Obj();
        e.put("s", "a\"b\\c\nd\re\tfg");
        assertEquals("{\"s\":\"a\\\"b\\\\c\\nd\\re\\tf\\u0001g\"}", e.render());
    }
    @Test void statsMatchC() {
        // 5 samples in ns; medians/stdev per bench_json_c.c: midpoint median,
        // Bessel stdev, p95 index round((n-1)*0.95).
        long[] wall = {100_000_000L, 200_000_000L, 300_000_000L, 400_000_000L, 500_000_000L};
        long[] cpu  = {100_000_000L, 100_000_000L, 100_000_000L, 100_000_000L, 100_000_000L};
        long[] gc   = {0, 0, 0, 0, 0};
        BenchJson.Obj s = BenchJson.pathSummary(wall, cpu, gc, 5, 3_000_000L, 15, 0, "floor", true);
        String j = s.render();
        assertTrue(j.contains("\"median_s\":0.3"));
        assertTrue(j.contains("\"p95_s\":0.5"));
        assertTrue(j.contains("\"rows_per_s_median\":10000000"));
        assertTrue(j.contains("\"iterations\":5"));
        assertTrue(j.contains("\"warm\":true"));
        assertTrue(j.contains("\"gc_ms_median\":0"));
    }
}
