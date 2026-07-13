package io.questdb.qwpbench;

public final class Main {
    public static void main(String[] args) throws Exception {
        if (args.length != 1) {
            System.err.println("usage: qwp-bench-java <ingress|egress>");
            System.exit(1);
            return;
        }
        int rc = switch (args[0]) {
            case "ingress" -> IngressBench.run();
            case "egress" -> EgressBench.run();
            default -> {
                System.err.println("unknown mode " + args[0]);
                yield 1;
            }
        };
        System.exit(rc);
    }

    private Main() {}
}
