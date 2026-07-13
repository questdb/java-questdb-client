package io.questdb.qwpbench;

/**
 * Environment-variable readers shared by the ingress/egress bench entry points.
 */
public final class Env {
    public static long zu(String name, long dflt) {
        String v = System.getenv(name);
        if (v == null || v.isEmpty()) return dflt;
        return Long.parseLong(v);
    }

    public static String str(String name, String dflt) {
        String v = System.getenv(name);
        return (v == null || v.isEmpty()) ? dflt : v;
    }

    private Env() {}
}
