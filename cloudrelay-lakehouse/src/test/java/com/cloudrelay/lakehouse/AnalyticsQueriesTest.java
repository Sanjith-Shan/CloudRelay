package com.cloudrelay.lakehouse;

import com.cloudrelay.lakehouse.aggregate.GoldAggregates;
import com.cloudrelay.lakehouse.ingest.BronzeIngestion;
import com.cloudrelay.lakehouse.ingest.FileEventSource;
import com.cloudrelay.lakehouse.query.AnalyticsQueries;
import com.cloudrelay.lakehouse.transform.SilverTransform;
import org.apache.spark.sql.Row;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.nio.file.Path;
import java.time.Instant;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The operator-facing SQL, run against a pipeline that has never rejected
 * anything.
 *
 * <p>That is the case worth testing rather than the interesting one. The
 * quarantine table is only created the first time a malformed event arrives, so
 * on a healthy pipeline it does not exist — and a query pack that falls over
 * when nothing has gone wrong is unavailable exactly when somebody is checking
 * that nothing has gone wrong.
 */
class AnalyticsQueriesTest extends SparkTestBase {

    private static final int SESSIONS = 60;
    private static LakehouseOptions options;
    private static int totalEvents;

    @BeforeAll
    static void runCleanPipeline() throws Exception {
        options = freshLakehouse("analytics-queries");
        List<String> lines =
                TestEvents.lines(SESSIONS, Instant.parse("2026-08-27T09:00:00Z"), 31L);
        totalEvents = lines.size();
        TestEvents.writeLanding(Path.of(options.landingPath()), lines, 6);

        new BronzeIngestion(options, new FileEventSource(options, 3)).runOnce(spark);
        new SilverTransform(options).runOnce(spark);
        new GoldAggregates(options).runOnce(spark);
    }

    @Test
    void aHealthyPipelineHasNoQuarantineTable() {
        // The precondition for everything below. If this ever starts being
        // true, the rest of this class is testing the easy path by accident.
        assertThat(DeltaTables.exists(spark, options.quarantinePath())).isFalse();
    }

    @Test
    void everyQueryInThePackRunsWithoutAQuarantineTable() {
        List<String> rendered = new AnalyticsQueries(options).runAll(spark);

        assertThat(rendered).hasSize(AnalyticsQueries.catalog().size());
        assertThat(rendered).noneMatch(output -> output.contains("unavailable"));
    }

    @Test
    void theThroughputSummaryReconcilesBronzeAgainstSilver() {
        AnalyticsQueries queries = new AnalyticsQueries(options);
        queries.registerViews(spark);

        Row summary = spark.sql(AnalyticsQueries.catalog()
                .get("Pipeline throughput: bronze in, silver out")).first();

        assertThat(summary.getLong(0)).as("bronze rows").isEqualTo(totalEvents);
        assertThat(summary.getLong(1)).as("silver rows").isEqualTo(totalEvents);
        assertThat(summary.getLong(2)).as("quarantined rows").isZero();
        assertThat(summary.getLong(3)).as("distinct event ids").isEqualTo(totalEvents);
    }

    @Test
    void theDataQualityQueryReturnsNoRowsRatherThanFailing() {
        AnalyticsQueries queries = new AnalyticsQueries(options);
        queries.registerViews(spark);

        assertThat(spark.sql(AnalyticsQueries.catalog()
                .get("Data quality: what is being rejected")).count())
                .isZero();
    }

    @Test
    void theFunnelQueryReportsEveryGameAndRegionPairSeen() {
        AnalyticsQueries queries = new AnalyticsQueries(options);
        queries.registerViews(spark);

        long pairs = spark.sql(AnalyticsQueries.catalog()
                .get("Session length distribution by game")).count();

        assertThat(pairs).isPositive()
                .isLessThanOrEqualTo((long) TestEvents.GAMES.size() * TestEvents.REGIONS.size());
    }
}
