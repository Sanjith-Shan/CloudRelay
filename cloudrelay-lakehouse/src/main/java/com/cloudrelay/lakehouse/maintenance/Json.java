package com.cloudrelay.lakehouse.maintenance;

import java.util.Collection;
import java.util.Map;

/**
 * Just enough JSON to write a benchmark report.
 *
 * <p>Hand rolled rather than pulling Jackson onto the Spark classpath, where a
 * version that disagrees with the one Spark shades is a genuinely unpleasant
 * class of bug for the sake of one output file.
 */
final class Json {

    private Json() {
    }

    static String object(Map<String, Object> map) {
        StringBuilder sb = new StringBuilder("{\n");
        int i = 0;
        for (Map.Entry<String, Object> e : map.entrySet()) {
            sb.append("  ").append(quote(e.getKey())).append(": ").append(value(e.getValue()));
            if (++i < map.size()) {
                sb.append(',');
            }
            sb.append('\n');
        }
        return sb.append("}\n").toString();
    }

    @SuppressWarnings("unchecked")
    private static String value(Object v) {
        if (v == null) {
            return "null";
        }
        if (v instanceof Number || v instanceof Boolean) {
            return v.toString();
        }
        if (v instanceof Map<?, ?> m) {
            return object((Map<String, Object>) m).replace("\n", "\n  ").stripTrailing();
        }
        if (v instanceof Collection<?> c) {
            StringBuilder sb = new StringBuilder("[");
            int i = 0;
            for (Object o : c) {
                sb.append(value(o));
                if (++i < c.size()) {
                    sb.append(", ");
                }
            }
            return sb.append(']').toString();
        }
        return quote(v.toString());
    }

    private static String quote(String s) {
        return '"' + s.replace("\\", "\\\\").replace("\"", "\\\"")
                .replace("\n", "\\n").replace("\r", "\\r").replace("\t", "\\t") + '"';
    }
}
