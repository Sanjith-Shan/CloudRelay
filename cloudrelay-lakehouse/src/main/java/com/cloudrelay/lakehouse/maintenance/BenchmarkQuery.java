package com.cloudrelay.lakehouse.maintenance;

/**
 * One named query that gets timed before and after compaction.
 *
 * @param name what it measures, used as the row label in the report
 * @param sql  a query over {@code {table}}, substituted with the table path
 */
public record BenchmarkQuery(String name, String sql) {

    public String resolve(String tablePath) {
        return sql.replace("{table}", "delta.`" + tablePath + "`");
    }
}
