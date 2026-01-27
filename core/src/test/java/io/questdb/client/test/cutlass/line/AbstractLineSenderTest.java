package io.questdb.client.test.cutlass.line;

import io.questdb.client.cutlass.line.AbstractLineSender;
import io.questdb.client.test.AbstractQdbTest;
import org.junit.Assume;
import org.junit.BeforeClass;

import java.lang.reflect.Array;

public class AbstractLineSenderTest extends AbstractQdbTest {
    private static final int DEFAULT_ILP_TCP_PORT = 9009;
    private static final int DEFAULT_ILP_UDP_PORT = 9009;

    public static <T> T createDoubleArray(int... shape) {
        int[] indices = new int[shape.length];
        return buildNestedArray(ArrayDataType.DOUBLE, shape, 0, indices);
    }

    @BeforeClass
    public static void setUpStatic() {
        AbstractQdbTest.setUpStatic();
        Assume.assumeTrue(getQuestDBRunning());
    }

    @SuppressWarnings("unchecked")
    private static <T> T buildNestedArray(ArrayDataType dataType, int[] shape, int currentDim, int[] indices) {
        if (currentDim == shape.length - 1) {
            Object arr = dataType.createArray(shape[currentDim]);
            for (int i = 0; i < Array.getLength(arr); i++) {
                indices[currentDim] = i;
                dataType.setElement(arr, i, indices);
            }
            return (T) arr;
        } else {
            Class<?> componentType = dataType.getComponentType(shape.length - currentDim - 1);
            Object arr = Array.newInstance(componentType, shape[currentDim]);
            for (int i = 0; i < shape[currentDim]; i++) {
                indices[currentDim] = i;
                Object subArr = buildNestedArray(dataType, shape, currentDim + 1, indices);
                Array.set(arr, i, subArr);
            }
            return (T) arr;
        }
    }

    /**
     * Get ILP TCP port.
     */
    protected static int getIlpTcpPort() {
        return getConfigInt("QUESTDB_ILP_TCP_PORT", "questdb.ilp.tcp.port", DEFAULT_ILP_TCP_PORT);
    }

    /**
     * Get ILP UDP port.
     */
    protected static int getIlpUdpPort() {
        return getConfigInt("QUESTDB_ILP_UDP_PORT", "questdb.ilp.udp.port", DEFAULT_ILP_UDP_PORT);
    }

    /**
     * Send data using the sender and assert the expected row count.
     * This method flushes the sender, waits for UDP to settle, and polls for the expected row count.
     * <p>
     * UDP is fire-and-forget, so we need extra delay to ensure the server has processed the data.
     *
     * @param sender           the sender to flush
     * @param tableName        the table to check
     * @param expectedRowCount the expected number of rows
     */
    protected void flushAndAssertRowCount(AbstractLineSender sender, String tableName, int expectedRowCount) throws Exception {
        sender.flush();
        assertTableSizeEventually(tableName, expectedRowCount);
    }

    private enum ArrayDataType {
        DOUBLE(double.class) {
            @Override
            public Object createArray(int length) {
                return new double[length];
            }

            @Override
            public void setElement(Object array, int index, int[] indices) {
                double[] arr = (double[]) array;
                double product = 1.0;
                for (int idx : indices) {
                    product *= (idx + 1);
                }
                arr[index] = product;
            }
        },
        LONG(long.class) {
            @Override
            public Object createArray(int length) {
                return new long[length];
            }

            @Override
            public void setElement(Object array, int index, int[] indices) {
                long[] arr = (long[]) array;
                long product = 1L;
                for (int idx : indices) {
                    product *= (idx + 1);
                }
                arr[index] = product;
            }
        };

        private final Class<?> baseType;
        private final Class<?>[] componentTypes = new Class<?>[17]; // 支持最多16维

        ArrayDataType(Class<?> baseType) {
            this.baseType = baseType;
            initComponentTypes();
        }

        public abstract Object createArray(int length);

        public Class<?> getComponentType(int dimsRemaining) {
            if (dimsRemaining < 0 || dimsRemaining > 16) {
                throw new RuntimeException("Array dimension too large");
            }
            return componentTypes[dimsRemaining];
        }

        public abstract void setElement(Object array, int index, int[] indices);

        private void initComponentTypes() {
            componentTypes[0] = baseType;
            for (int dim = 1; dim <= 16; dim++) {
                componentTypes[dim] = Array.newInstance(componentTypes[dim - 1], 0).getClass();
            }
        }
    }
}
