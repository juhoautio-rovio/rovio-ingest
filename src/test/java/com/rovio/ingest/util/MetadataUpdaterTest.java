package com.rovio.ingest.util;

import com.rovio.ingest.DruidSourceBaseTest;
import com.rovio.ingest.WriterContext;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.partition.LinearShardSpec;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.joda.time.DateTime;
import org.joda.time.DateTimeUtils;
import org.joda.time.Interval;
import org.joda.time.chrono.ISOChronology;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.skife.jdbi.v2.DBI;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.shaded.com.google.common.collect.ImmutableMap;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static com.rovio.ingest.DruidSourceBaseTest.DATA_SOURCE;
import static com.rovio.ingest.DruidSourceBaseTest.VERSION_TIME_MILLIS;
import static com.rovio.ingest.DruidSourceBaseTest.dbPass;
import static com.rovio.ingest.DruidSourceBaseTest.dbUser;
import static com.rovio.ingest.DruidSourceBaseTest.getDataSourceOptions;
import static com.rovio.ingest.DruidSourceBaseTest.getPostgreSQLContainer;
import static com.rovio.ingest.DruidSourceBaseTest.prepareDatabase;
import static com.rovio.ingest.DruidSourceBaseTest.segmentsTable;
import static com.rovio.ingest.DruidSourceBaseTest.setUpDb;
import static org.junit.Assert.assertEquals;

@Testcontainers
class MetadataUpdaterTest {

    // TODO cover MySQLContainer as well
    @Container
    public static PostgreSQLContainer POSTGRES = getPostgreSQLContainer();

    @BeforeAll
    public static void beforeClass() throws Exception {
        prepareDatabase(POSTGRES);
    }

    @BeforeEach
    public void setUp() throws SQLException {
        DateTimeUtils.setCurrentMillisFixed(VERSION_TIME_MILLIS);
        setUpDb(POSTGRES);
    }

    @AfterEach
    public void after() {
        DateTimeUtils.setCurrentMillisSystem();
    }

    @Test
    void publishSegments() {
        String version = DateTime.now(ISOChronology.getInstanceUTC()).toString();
        Interval interval = new Interval(DateTime.parse("2019-10-16T00:00:00Z"), DateTime.parse("2019-10-18T00:00:00Z"));

        MetadataUpdater updater = new MetadataUpdater(WriterContext.from(new CaseInsensitiveStringMap(
                getDataSourceOptions(POSTGRES)), version));

        List<DataSegment> segments = Arrays.asList(
                DataSegment.builder()
                        .dataSource(DATA_SOURCE)
                        .loadSpec(ImmutableMap.of("type", "local", "path", "tmp/2019-10-17T00:00:00.000Z_2019-10-18T00:00:00.000Z/2019-10-01T20:29:31.384Z/0/index.zip"))
                        .shardSpec(new LinearShardSpec(0))
                        .size(2218)
                        .binaryVersion(9)
                        .interval(interval)
                        .version(version)
                        .build());
        updater.publishSegments(segments);

        List<Map<String, Object>> result = DBI.open(DruidSourceBaseTest.connectionString, dbUser, dbPass)
                .createQuery("select * from " + segmentsTable).list();
        Map<String, Object> row = result.get(0);
        assertEquals("temp", row.get("datasource"));
        assertEquals("2019-10-16T00:00:00.000Z", row.get("start"));
        assertEquals(true, row.get("partitioned"));
        assertEquals("temp_2019-10-16T00:00:00.000Z_2019-10-18T00:00:00.000Z_2019-10-01T20:29:31.384Z", row.get("id"));
        assertEquals("2019-10-18T00:00:00.000Z", row.get("end"));
        assertEquals("2019-10-01T20:29:31.384Z", row.get("created_date"));
        assertEquals("2019-10-01T20:29:31.384Z", row.get("version"));
    }

}
