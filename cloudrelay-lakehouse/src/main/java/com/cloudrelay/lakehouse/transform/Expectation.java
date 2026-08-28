package com.cloudrelay.lakehouse.transform;

import org.apache.spark.sql.Column;

/**
 * One named rule a silver row has to satisfy.
 *
 * <p>The name matters as much as the condition. When a row is quarantined the
 * names of the rules it broke are written next to it, so triage starts from
 * "1,400 rows failed {@code event_time_parses} at 04:10" rather than from a
 * pile of JSON somebody has to read.
 *
 * @param name      stable identifier, written into {@code quality_failures}
 * @param condition must evaluate true for the row to reach silver
 */
public record Expectation(String name, Column condition) {
}
