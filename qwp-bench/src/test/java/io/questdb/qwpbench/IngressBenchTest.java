package io.questdb.qwpbench;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class IngressBenchTest {

    @Test
    void senderRangesTileExactly() {
        long[][] cases = {{10, 1}, {10, 3}, {1000003, 7}, {5, 8}};
        for (long[] c : cases) {
            long rows = c[0];
            int n = (int) c[1];
            long prev = 0;
            for (int k = 0; k < n; k++) {
                long[] r = IngressBench.senderRange(rows, n, k);
                assertEquals(prev, r[0], "lo of sender " + k + " for rows=" + rows + " n=" + n);
                assertTrue(r[1] >= r[0], "hi >= lo");
                prev = r[1];
            }
            assertEquals(rows, prev, "ranges must end at rows for rows=" + rows + " n=" + n);
        }
    }
}
