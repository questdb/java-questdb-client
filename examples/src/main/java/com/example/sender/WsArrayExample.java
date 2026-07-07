package com.example.sender;

import io.questdb.client.QuestDB;
import io.questdb.client.Sender;
import io.questdb.client.cutlass.line.array.DoubleArray;

import java.time.temporal.ChronoUnit;

/**
 * Ingesting DOUBLE array columns over QWP.
 * <p>
 * Only DOUBLE arrays are supported for ingestion today. LONG arrays
 * ({@code longArray} / {@code LongArray}) are <b>not</b> yet accepted by the
 * server -- attempting one is rejected with "long arrays are not supported,
 * only double arrays" -- so this example sticks to doubles.
 * <p>
 * For 1D and 2D arrays, pass a Java array straight to {@code doubleArray}. For
 * higher dimensions or hot paths, reuse a {@link DoubleArray} instance across
 * rows to avoid per-row allocation: {@code clear()} resets the write position
 * and reuses the native memory, {@code append()} fills in row-major order (last
 * dimension varies fastest), and {@code reshape(...)} changes the shape without
 * reallocating.
 */
public class WsArrayExample {

    private static final int ROW_COUNT = 10;

    public static void main(String[] args) {
        try (QuestDB db = QuestDB.connect("ws::addr=localhost:9000;")) {
            try (Sender sender = db.borrowSender()) {

                // 1D: pass a Java array directly (2D double[][] and 3D
                // double[][][] overloads exist too).
                double[] levels = {1.0842, 1.0843, 1.0841};
                sender.table("book")
                        .doubleArray("levels", levels)
                        .atNow();

                // Higher-dimensional / hot path: reuse one DoubleArray across
                // rows. It owns native memory, so close it (try-with-resources).
                try (DoubleArray cube = new DoubleArray(3, 3, 3)) {
                    for (int row = 0; row < ROW_COUNT; row++) {
                        cube.clear();                 // reset write position, reuse memory
                        for (int v = 0; v < 27; v++) {
                            cube.append(v + row);     // row-major fill
                        }
                        sender.table("tensors")
                                .doubleArray("cube", cube)
                                .at(System.currentTimeMillis() * 1000, ChronoUnit.MICROS);
                    }
                }

                sender.flush();
            }
        }
    }
}
