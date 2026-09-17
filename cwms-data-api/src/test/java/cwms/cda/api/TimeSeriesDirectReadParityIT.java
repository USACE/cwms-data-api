package cwms.cda.api;

import static cwms.cda.data.dao.JooqDao.REQUIRE_NEW_LRTS_ID_FORMAT;
import static cwms.cda.data.dao.JooqDao.SESSION_USE_LRTS_ID_FORMAT;
import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import cwms.cda.ApiServlet;
import cwms.cda.api.enums.VersionType;
import cwms.cda.data.dto.TimeSeries;
import cwms.cda.formatters.Formats;
import cwms.cda.formatters.json.JsonV2;
import fixtures.CwmsDataApiSetupCallback;
import io.restassured.filter.log.LogDetail;
import io.restassured.response.ExtractableResponse;
import io.restassured.response.Response;
import io.restassured.specification.RequestSpecification;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import javax.servlet.http.HttpServletResponse;
import mil.army.usace.hec.test.database.CwmsDatabaseContainer;
import org.jooq.impl.DSL;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import usace.cwms.db.jooq.codegen.packages.CWMS_TS_PACKAGE;
import usace.cwms.db.jooq.codegen.packages.CWMS_UTIL_PACKAGE;

@Tag("integration")
final class TimeSeriesDirectReadParityIT extends DataApiTestIT {
    private static final ObjectMapper OBJECT_MAPPER = JsonV2.buildObjectMapper();
    private static final String OFFICE = "SPK";
    private static final double DOUBLE_TOLERANCE = 1e-9;

    @Test
    void databasePagingTransfersOnlyPageAndLookaheadInOneStatement() throws Exception {
        String property = cwms.cda.data.dao.TimeSeriesDaoImpl.DATABASE_PAGING_PROPERTY;
        String previous = System.getProperty(property);
        try {
            System.clearProperty(property);
            verifyDatabasePagingOptIn();
        } finally {
            if (previous == null) {
                System.clearProperty(property);
            } else {
                System.setProperty(property, previous);
            }
        }
    }

    private void verifyDatabasePagingOptIn() throws Exception {
        String series = "ITSQLBOUND.Stage.Inst.1Minute.0.BENCH";
        Instant begin = Instant.parse("2024-01-01T00:00:00Z");
        seedTimeSeries("ITSQLBOUND", series, regularRows(begin, 2500, 1.0, Duration.ofDays(1)), false);
        connectionAsWebUser(connection -> {
            org.jooq.DSLContext context = cwms.cda.data.dao.JooqDao.getDslContext(connection, OFFICE);
            java.util.concurrent.atomic.AtomicInteger queries = new java.util.concurrent.atomic.AtomicInteger();
            java.util.concurrent.atomic.AtomicInteger fetched = new java.util.concurrent.atomic.AtomicInteger();
            org.jooq.impl.DefaultExecuteListener listener = new org.jooq.impl.DefaultExecuteListener() {
                @Override
                public void executeStart(org.jooq.ExecuteContext event) {
                    if (event.sql() != null && event.sql().contains("window_summary")) {
                        queries.incrementAndGet();
                    }
                }

                @Override
                public void recordEnd(org.jooq.ExecuteContext event) {
                    if (event.record() != null && event.sql() != null && event.sql().contains("window_summary")) {
                        fetched.incrementAndGet();
                    }
                }
            };
            org.jooq.ExecuteListenerProvider[] existing = context.configuration().executeListenerProviders();
            org.jooq.ExecuteListenerProvider[] providers = java.util.Arrays.copyOf(existing, existing.length + 1);
            providers[existing.length] = new org.jooq.impl.DefaultExecuteListenerProvider(listener);
            context.configuration().set(providers);
            cwms.cda.data.dao.TimeSeriesDaoImpl dao = new cwms.cda.data.dao.TimeSeriesDaoImpl(context,
                    new com.codahale.metrics.MetricRegistry());
            cwms.cda.data.dao.TimeSeriesRequestParameters parameters =
                    new cwms.cda.data.dao.TimeSeriesRequestParameters.Builder().withOffice(OFFICE)
                            .withNames(series).withUnits("ft").withBeginTime(begin.atZone(java.time.ZoneOffset.UTC))
                            .withEndTime(begin.plusSeconds(2499 * 60L).atZone(java.time.ZoneOffset.UTC)).build();
            TimeSeries defaultPage = dao.getTimeseries(null, 3, parameters);
            assertEquals(2500, defaultPage.getTotal());
            assertEquals(3, defaultPage.getValues().size());
            assertEquals(0, queries.get());
            System.setProperty(cwms.cda.data.dao.TimeSeriesDaoImpl.DATABASE_PAGING_PROPERTY, "true");
            TimeSeries first = dao.getTimeseries(null, 3, parameters);
            assertEquals(2500, first.getTotal());
            assertEquals(3, first.getValues().size());
            assertEquals(1, queries.get());
            assertEquals(4, fetched.get());
            assertNotNull(first.getNextPage());
            queries.set(0);
            fetched.set(0);
            TimeSeries second = dao.getTimeseries(first.getNextPage(), 3, parameters);
            assertEquals(2500, second.getTotal());
            assertEquals(begin.plusSeconds(180), second.getValues().get(0).getDateTime().toInstant());
            assertEquals(1, queries.get());
            assertEquals(4, fetched.get());
            queries.set(0);
            fetched.set(0);
            TimeSeries metadata = dao.getTimeseries(null, 0, parameters);
            assertEquals(2500, metadata.getTotal());
            assertEquals(0, metadata.getValues().size());
            assertEquals(1, queries.get());
            assertEquals(1, fetched.get());
        });
    }

    @Test
    void databasePagesMatchFullMergeAtFractionalCursorsAndEmptyWindows() throws Exception {
        String series = "ITSQLPAGE.Stage.Inst.1Minute.0.BENCH";
        seedTimeSeries("ITSQLPAGE", series, List.of(
                row("2024-01-01T00:00:00Z", 1.0, 0, "2024-01-02T00:00:00Z", null),
                row("2024-01-01T00:00:30Z", null, 5, "2024-01-02T00:00:01Z", null),
                row("2024-01-01T00:02:00Z", 2.0, 0, "2024-01-02T00:00:02Z", null),
                row("2024-01-01T00:05:15Z", 3.0, 0, "2024-01-02T00:00:03Z", null)), false);
        Instant begin = Instant.parse("2024-01-01T00:00:00Z");
        assertDatabasePagesMatchMerge(series, begin, begin.plusSeconds(360));
        assertDatabasePagesMatchMerge(series, begin.plusMillis(500), begin.plusSeconds(315).plusMillis(500));
        assertDatabasePagesMatchMerge(series, begin.plusSeconds(86400), begin.plusSeconds(86700));
    }

    @Test
    void databasePagesPreserveIrregularAndMaximumVersionSelection() throws Exception {
        String irregular = "ITSQLIRR.Stage.Inst.0.0.BENCH";
        seedTimeSeries("ITSQLIRR", irregular, irregularRows(), false);
        assertDatabasePagesMatchMerge(irregular, Instant.parse("2024-01-05T12:00:00Z"),
                Instant.parse("2024-01-05T12:40:00Z"));
        String versioned = "ITSQLVER.Stage.Inst.1Hour.0.BENCH";
        seedTimeSeries("ITSQLVER", versioned, versionedRows(), true);
        assertDatabasePagesMatchMerge(versioned, Instant.parse("2024-05-01T15:00:00Z"),
                Instant.parse("2024-05-01T19:00:00Z"));
    }

    @Test
    void databasePagesPreserveNonzeroIntervalOffsets() throws Exception {
        String series = "ITSQLOFF.Stage.Inst.5Minutes.0.BENCH";
        seedTimeSeries("ITSQLOFF", series, List.of(
                row("2024-01-01T00:02:00Z", 1.0, 0, "2024-01-02T00:00:00Z", null),
                row("2024-01-01T00:03:30Z", 2.0, 0, "2024-01-02T00:00:01Z", null),
                row("2024-01-01T00:12:00Z", 3.0, 0, "2024-01-02T00:00:02Z", null)), false, 2);
        assertDatabasePagesMatchMerge(series, Instant.parse("2024-01-01T00:00:00Z"),
                Instant.parse("2024-01-01T00:20:00Z"));
    }

    private static void assertDatabasePagesMatchMerge(String series, Instant begin, Instant end) throws Exception {
        String property = cwms.cda.data.dao.TimeSeriesDaoImpl.DATABASE_PAGING_PROPERTY;
        String previous = System.getProperty(property);
        try {
            for (boolean trim : new boolean[]{false, true}) {
                for (int size : new int[]{0, 1, 3}) {
                    for (Instant cursor : new Instant[]{null, begin.plusMillis(500),
                            begin.plusSeconds(61).plusMillis(500), end.plusSeconds(1)}) {
                        System.setProperty(property, "false");
                        JsonNode reference = fetchPageTree(series, begin, end, trim, size, cursor);
                        System.setProperty(property, "true");
                        JsonNode actual = fetchPageTree(series, begin, end, trim, size, cursor);
                        assertEquals(reference, actual, series + " trim=" + trim + " size=" + size
                                + " begin=" + begin + " cursor=" + cursor);
                    }
                }
            }
        } finally {
            if (previous == null) {
                System.clearProperty(property);
            } else {
                System.setProperty(property, previous);
            }
        }
    }

    private static JsonNode fetchPageTree(String series, Instant begin, Instant end, boolean trim,
                                          int size, Instant cursor) throws Exception {
        RequestSpecification request = given().accept(Formats.JSONV2)
                .queryParam(Controllers.OFFICE, OFFICE).queryParam(Controllers.NAME, series)
                .queryParam(Controllers.UNIT, "ft").queryParam(Controllers.BEGIN, begin.toString())
                .queryParam(Controllers.END, end.toString()).queryParam(Controllers.TRIM, trim)
                .queryParam(Controllers.PAGE_SIZE, size).queryParam(Controllers.INCLUDE_ENTRY_DATE, true);
        if (cursor != null) {
            request.queryParam(Controllers.PAGE, cwms.cda.data.dto.CwmsDTOPaginated.encodeCursor(
                    Long.toString(cursor.toEpochMilli()), size));
        }
        return OBJECT_MAPPER.readTree(request.get("/timeseries/").then().statusCode(200).extract().asString());
    }

    @Test
    void readLimitsRejectLargeWindowsPagesAndCursorOverrides() throws Exception {
        String seriesId = "ITPARCAP.Stage.Inst.1Minute.0.BENCH";
        seedTimeSeries("ITPARCAP", seriesId, denseRows(), false);
        Instant begin = Instant.parse("2024-01-01T00:00:00Z");
        for (String accept : new String[]{Formats.JSONV2, Formats.CSV}) {
            given().accept(accept).queryParam(Controllers.OFFICE, OFFICE)
                    .queryParam(Controllers.NAME, seriesId).queryParam(Controllers.UNIT, "ft")
                    .queryParam(Controllers.BEGIN, begin.toString())
                    .queryParam(Controllers.END, begin.plusSeconds(5_000_000L * 60).toString())
                    .queryParam(Controllers.PAGE_SIZE, 500)
                    .get("/timeseries/").then().statusCode(413);
        }
        given().accept(Formats.JSONV2).queryParam(Controllers.OFFICE, OFFICE)
                .queryParam(Controllers.NAME, seriesId).queryParam(Controllers.PAGE_SIZE, 100_001)
                .get("/timeseries/").then().statusCode(413);
        String cursor = cwms.cda.data.dto.CwmsDTOPaginated.encodeCursor(
                Long.toString(begin.toEpochMilli()), 100_001);
        given().accept(Formats.JSONV2).queryParam(Controllers.OFFICE, OFFICE)
                .queryParam(Controllers.NAME, seriesId).queryParam(Controllers.PAGE_SIZE, 1)
                .queryParam(Controllers.PAGE, cursor).queryParam(Controllers.UNIT, "ft")
                .queryParam(Controllers.BEGIN, begin.toString())
                .queryParam(Controllers.END, begin.plusSeconds(300).toString())
                .get("/timeseries/").then().statusCode(413);
    }

    @Test
    void unlimitedGapFilledResultIsRejectedAndSmallPageStillWorks() throws Exception {
        String seriesId = "ITPARCAPG.Stage.Inst.1Minute.0.BENCH";
        seedTimeSeries("ITPARCAPG", seriesId, denseRows(), false);
        Instant begin = Instant.parse("2024-01-01T00:00:00Z");
        Instant end = begin.plusSeconds(100_000L * 60);
        given().accept(Formats.JSONV2).queryParam(Controllers.OFFICE, OFFICE)
                .queryParam(Controllers.NAME, seriesId).queryParam(Controllers.UNIT, "ft")
                .queryParam(Controllers.BEGIN, begin.toString()).queryParam(Controllers.END, end.toString())
                .queryParam(Controllers.TRIM, false).queryParam(Controllers.PAGE_SIZE, -1)
                .get("/timeseries/").then().statusCode(413);
        given().accept(Formats.JSONV2).queryParam(Controllers.OFFICE, OFFICE)
                .queryParam(Controllers.NAME, seriesId).queryParam(Controllers.UNIT, "ft")
                .queryParam(Controllers.BEGIN, begin.toString()).queryParam(Controllers.END, end.toString())
                .queryParam(Controllers.TRIM, false).queryParam(Controllers.PAGE_SIZE, 10)
                .get("/timeseries/").then().statusCode(200).body("total", is(100_001))
                .body("values.size()", is(10));
    }

    @Test
    void denseRegularReadMatchesRetrieveTs() throws Exception {
        assertDirectReadMatchesOracle(
            "ITPARREG",
            "ITPARREG.Stage.Inst.1Minute.0.BENCH",
            "ft",
            Instant.parse("2024-01-01T00:00:00Z"),
            Instant.parse("2024-01-01T00:05:00Z"),
            denseRows(),
            false,
            false,
            VersionType.UNVERSIONED,
            Duration.ofMinutes(1),
            0L,
            null
        );
    }

    @Test
    void convertedUnitReadMatchesRetrieveTs() throws Exception {
        assertDirectReadMatchesOracle(
            "ITPARUNIT",
            "ITPARUNIT.Stage.Inst.1Minute.0.BENCH",
            "m",
            Instant.parse("2024-01-01T00:00:00Z"),
            Instant.parse("2024-01-01T00:05:00Z"),
            denseRows(),
            false,
            false,
            VersionType.UNVERSIONED,
            Duration.ofMinutes(1),
            0L,
            null
        );
    }

    @Test
    void denseRegularEntryDateReadMatchesRetrieveTs() throws Exception {
        assertDirectReadMatchesOracle(
            "ITPARREG",
            "ITPARREG.Stage.Inst.1Minute.0.BENCH",
            "ft",
            Instant.parse("2024-01-01T00:00:00Z"),
            Instant.parse("2024-01-01T00:05:00Z"),
            denseRows(),
            false,
            true,
            VersionType.UNVERSIONED,
            Duration.ofMinutes(1),
            0L,
            null
        );
    }

    @Test
    void gapFilledRegularReadMatchesRetrieveTs() throws Exception {
        assertDirectReadMatchesOracle(
            "ITPARGAP",
            "ITPARGAP.Stage.Inst.1Minute.0.BENCH",
            "ft",
            Instant.parse("2024-01-01T00:00:00Z"),
            Instant.parse("2024-01-01T00:09:00Z"),
            gapRows(),
            false,
            false,
            VersionType.UNVERSIONED,
            Duration.ofMinutes(1),
            0L,
            null
        );
    }

    @Test
    void maxVersionReadMatchesRetrieveTs() throws Exception {
        assertDirectReadMatchesOracle(
            "ITPARVER",
            "ITPARVER.Flow.Inst.1Hour.0.BENCH",
            "cfs",
            Instant.parse("2024-05-01T15:00:00Z"),
            Instant.parse("2024-05-01T18:00:00Z"),
            versionedRows(),
            true,
            false,
            VersionType.MAX_AGGREGATE,
            Duration.ofHours(1),
            0L,
            null
        );
    }

    @Test
    void specificVersionReadMatchesRetrieveTs() throws Exception {
        Instant newerVersion = Instant.parse("2024-06-21T08:00:00Z");
        assertDirectReadMatchesOracle(
            "ITPARVER",
            "ITPARVER.Flow.Inst.1Hour.0.BENCH",
            "cfs",
            Instant.parse("2024-05-01T15:00:00Z"),
            Instant.parse("2024-05-01T18:00:00Z"),
            versionedRows(),
            true,
            false,
            VersionType.SINGLE_VERSION,
            Duration.ofHours(1),
            0L,
            newerVersion
        );
    }

    @Test
    void irregularReadMatchesRetrieveTs() throws Exception {
        assertDirectReadMatchesOracle(
            "ITPARIRR",
            "ITPARIRR.Flow.Inst.0.0.BENCH",
            "cfs",
            Instant.parse("2024-01-05T12:00:00Z"),
            Instant.parse("2024-01-05T12:33:10Z"),
            irregularRows(),
            false,
            false,
            VersionType.UNVERSIONED,
            Duration.ZERO,
            (long) Integer.MIN_VALUE,
            null
        );
    }

    @Test
    void pseudoIrregularReadWithLrtsHeaderMatchesRetrieveTs() throws Exception {
        String seriesId = "ITPARPIRR.Flow.Inst.~15Minutes.0.BENCH";
        Instant beginTime = Instant.parse("2024-01-05T12:00:00Z");
        Instant endTime = Instant.parse("2024-01-05T13:00:00Z");
        List<SeedRow> rows = List.of(
            row("2024-01-05T12:00:00Z", 10.0, 0, "2024-01-06T00:00:00Z", null),
            row("2024-01-05T12:17:00Z", 20.0, 0, "2024-01-06T00:01:00Z", null),
            row("2024-01-05T12:45:00Z", 30.0, 0, "2024-01-06T00:02:00Z", null)
        );
        seedTimeSeries("ITPARPIRR", seriesId, rows, false, null);

        List<TimeSeries.Record> expectedRows = fetchOracleRows(seriesId, "cfs", beginTime, endTime,
            false, null);
        TimeSeries actualResponse = fetchCdaRowsWithPageSize(seriesId, "cfs", beginTime, endTime,
            1000, false, null, true, true);

        assertEquals(expectedRows.size(), actualResponse.getTotal(), "total");
        assertEquals(expectedRows.size(), actualResponse.getValues().size(), "values size");
        assertEquals(Duration.ZERO, actualResponse.getInterval(), "interval");
        assertEquals((long) Integer.MIN_VALUE, actualResponse.getIntervalOffset(), "interval offset");
        for (int index = 0; index < expectedRows.size(); index++) {
            assertRecordsEqual(expectedRows.get(index), actualResponse.getValues().get(index), index);
        }
    }

    @Test
    void dstWindowRegularReadMatchesRetrieveTs() throws Exception {
        Instant dstStart = Instant.parse("2024-03-09T00:00:00Z");
        assertDirectReadMatchesOracle(
            "ITPARDST",
            "ITPARDST.Stage.Inst.1Minute.0.BENCH",
            "ft",
            dstStart,
            dstStart.plus(Duration.ofMinutes(4999)),
            regularRows(dstStart, 5000, 1.0, Duration.ofDays(1)),
            false,
            false,
            VersionType.UNVERSIONED,
            Duration.ofMinutes(1),
            0L,
            null
        );
    }

    @Test
    void localRegularGapReadMatchesRetrieveTs() throws Exception {
        assertDirectReadMatchesOracle(
            "ITPARLCL",
            "ITPARLCL.Flow.Inst.~1Day.0.BENCH",
            "cfs",
            Instant.parse("2024-01-01T06:00:00Z"),
            Instant.parse("2024-01-05T06:00:00Z"),
            localRegularGapRows(),
            false,
            false,
            VersionType.UNVERSIONED,
            Duration.ofDays(1),
            0L,
            null
        );
    }

    @Test
    void localRegularGapReadWithNewLrtsIntervalMatchesRetrieveTs() throws Exception {
        assertDirectReadMatchesOracle(
            "ITPARLCLNEW",
            "ITPARLCLNEW.Flow.Inst.1DayLocal.0.BENCH",
            "cfs",
            Instant.parse("2024-01-01T06:00:00Z"),
            Instant.parse("2024-01-05T06:00:00Z"),
            localRegularGapRows(),
            false,
            false,
            VersionType.UNVERSIONED,
            Duration.ofDays(1),
            0L,
            null,
            true
        );
    }

    @Test
    void pageSizeZeroReturnsEmptyValuesArray() throws Exception {
        List<SeedRow> rows = denseRows();
        Instant beginTime = Instant.parse("2024-01-01T00:00:00Z");
        Instant endTime = Instant.parse("2024-01-01T00:05:00Z");
        seedTimeSeries("ITPARPZ0", "ITPARPZ0.Stage.Inst.1Minute.0.BENCH", rows, false);

        TimeSeries response = fetchCdaRowsWithPageSize(
            "ITPARPZ0.Stage.Inst.1Minute.0.BENCH",
            "ft",
            beginTime,
            endTime,
            0,
            false,
            null,
            true
        );

        assertEquals(0, response.getPageSize(), "page-size");
        assertNotNull(response.getValues(), "values");
        assertEquals(0, response.getValues().size(), "values size");
        assertEquals(rows.size(), response.getTotal(), "total");
        assertNull(response.getPage(), "page");
        assertNull(response.getNextPage(), "next-page");
    }

    @Test
    void pageSizeNegativeOneReturnsWholeWindowWithoutPagination() throws Exception {
        List<SeedRow> rows = denseRows();
        Instant beginTime = Instant.parse("2024-01-01T00:00:00Z");
        Instant endTime = Instant.parse("2024-01-01T00:05:00Z");
        seedTimeSeries("ITPARALL", "ITPARALL.Stage.Inst.1Minute.0.BENCH", rows, false);

        TimeSeries response = fetchCdaRowsWithPageSize(
            "ITPARALL.Stage.Inst.1Minute.0.BENCH",
            "ft",
            beginTime,
            endTime,
            -1,
            false,
            null,
            true
        );

        assertEquals(-1, response.getPageSize(), "page-size");
        assertEquals(rows.size(), response.getValues().size(), "values size");
        assertEquals(rows.size(), response.getTotal(), "total");
        assertNull(response.getPage(), "page");
        assertNull(response.getNextPage(), "next-page");
    }

    @Test
    void trimmedResponseWindowMatchesReturnedValues() throws Exception {
        List<SeedRow> rows = gapRows();
        seedTimeSeries("ITPARTRM", "ITPARTRM.Stage.Inst.1Minute.0.BENCH", rows, false);

        TimeSeries response = fetchCdaRowsWithPageSize(
            "ITPARTRM.Stage.Inst.1Minute.0.BENCH",
            "ft",
            Instant.parse("2023-12-31T23:59:00Z"),
            Instant.parse("2024-01-01T00:10:00Z"),
            1000,
            false,
            null,
            true
        );

        assertNotNull(response.getBegin(), "begin");
        assertNotNull(response.getEnd(), "end");
        assertEquals(response.getValues().get(0).getDateTime().toInstant(), response.getBegin().toInstant(), "begin");
        assertEquals(response.getValues().get(response.getValues().size() - 1).getDateTime().toInstant(),
            response.getEnd().toInstant(), "end");
    }

    @Test
    void cursorPagesPreserveGapRowsAndTotal() throws Exception {
        String seriesId = "ITPARCUR.Stage.Inst.1Minute.0.BENCH";
        seedTimeSeries("ITPARCUR", seriesId, gapRows(), false);
        Instant begin = Instant.parse("2024-01-01T00:00:00Z");
        Instant end = Instant.parse("2024-01-01T00:09:00Z");
        TimeSeries whole = fetchCdaRowsWithPageSize(seriesId, "ft", begin, end, -1, false, null, false);
        List<Long> timestamps = new ArrayList<>();
        List<Double> values = new ArrayList<>();
        String cursor = null;
        int pages = 0;
        do {
            RequestSpecification request = given().accept(Formats.JSONV2)
                    .queryParam(Controllers.OFFICE, OFFICE).queryParam(Controllers.NAME, seriesId)
                    .queryParam(Controllers.UNIT, "ft").queryParam(Controllers.BEGIN, begin.toString())
                    .queryParam(Controllers.END, end.toString()).queryParam(Controllers.PAGE_SIZE, 3)
                    .queryParam(Controllers.TRIM, false);
            if (cursor != null) {
                request.queryParam(Controllers.PAGE, cursor);
            }
            String body = request.get("/timeseries/").then().statusCode(200).extract().asString();
            TimeSeries page = OBJECT_MAPPER.readValue(body, TimeSeries.class);
            assertEquals(whole.getTotal(), page.getTotal());
            page.getValues().forEach(row -> {
                timestamps.add(row.getDateTime().getTime());
                values.add(row.getValue());
            });
            cursor = page.getNextPage();
            org.junit.jupiter.api.Assertions.assertTrue(++pages <= 4, "Cursor must advance");
        } while (cursor != null);
        assertEquals(whole.getValues().stream().map(row -> row.getDateTime().getTime())
                .collect(Collectors.toList()), timestamps);
        assertEquals(whole.getValues().stream().map(TimeSeries.Record::getValue).collect(Collectors.toList()), values);
    }

    private static void assertDirectReadMatchesOracle(String locationId, String seriesId, String units,
                                                      Instant beginTime, Instant endTime, List<SeedRow> rows,
                                                      boolean versioned, boolean includeEntryDate,
                                                      VersionType expectedDateVersionType,
                                                      Duration expectedInterval, long expectedIntervalOffset,
                                                      Instant versionDate) throws Exception {
        assertDirectReadMatchesOracle(locationId, seriesId, units, beginTime, endTime, rows, versioned,
            includeEntryDate, expectedDateVersionType, expectedInterval, expectedIntervalOffset, versionDate, false);
    }

    private static void assertDirectReadMatchesOracle(String locationId, String seriesId, String units,
                                                      Instant beginTime, Instant endTime, List<SeedRow> rows,
                                                      boolean versioned, boolean includeEntryDate,
                                                      VersionType expectedDateVersionType,
                                                      Duration expectedInterval, long expectedIntervalOffset,
                                                      Instant versionDate, boolean useNewLrtsInterval)
            throws Exception {
        seedTimeSeries(locationId, seriesId, rows, versioned, useNewLrtsInterval);

        List<TimeSeries.Record> expectedRows = fetchOracleRows(seriesId, units, beginTime, endTime,
            includeEntryDate, versionDate, useNewLrtsInterval);
        TimeSeries actualResponse = fetchCdaRows(seriesId, units, beginTime, endTime, rows.size(),
            includeEntryDate, versionDate, useNewLrtsInterval ? Boolean.TRUE : null);
        String mismatchSummary = buildMismatchSummary(expectedRows, actualResponse);

        assertNotNull(actualResponse.getTotal(), "Reported total " + mismatchSummary);
        assertEquals(expectedRows.size(), actualResponse.getTotal(), "Reported total " + mismatchSummary);
        assertEquals(expectedDateVersionType, actualResponse.getDateVersionType(), "Date version type");
        assertEquals(expectedInterval, actualResponse.getInterval(), "Interval");
        assertEquals(expectedIntervalOffset, actualResponse.getIntervalOffset(), "Interval offset");

        if (versionDate != null) {
            assertNotNull(actualResponse.getVersionDate(), "Version date");
            assertEquals(versionDate, actualResponse.getVersionDate().toInstant(), "Version date");
        } else {
            assertNull(actualResponse.getVersionDate(), "Version date");
        }

        assertNotNull(actualResponse.getValues(), "Values " + mismatchSummary);
        assertEquals(expectedRows.size(), actualResponse.getValues().size(), "Row count " + mismatchSummary);
        for (int index = 0; index < expectedRows.size(); index++) {
            assertRecordsEqual(expectedRows.get(index), actualResponse.getValues().get(index), index);
        }
    }

    private static List<SeedRow> denseRows() {
        return List.of(
            row("2024-01-01T00:00:00Z", 1.0, 0, "2024-01-02T00:00:00Z", null),
            row("2024-01-01T00:01:00Z", 2.0, 0, "2024-01-02T00:01:00Z", null),
            row("2024-01-01T00:02:00Z", 3.0, 0, "2024-01-02T00:02:00Z", null),
            row("2024-01-01T00:03:00Z", 4.0, 0, "2024-01-02T00:03:00Z", null),
            row("2024-01-01T00:04:00Z", 5.0, 0, "2024-01-02T00:04:00Z", null),
            row("2024-01-01T00:05:00Z", 6.0, 0, "2024-01-02T00:05:00Z", null)
        );
    }

    private static List<SeedRow> localRegularGapRows() {
        return List.of(
            row("2024-01-01T06:00:00Z", 1.0, 0, "2024-01-06T00:00:00Z", null),
            row("2024-01-02T06:00:00Z", 2.0, 0, "2024-01-06T00:00:00Z", null),
            row("2024-01-04T06:00:00Z", 4.0, 0, "2024-01-06T00:00:00Z", null),
            row("2024-01-05T06:00:00Z", 5.0, 0, "2024-01-06T00:00:00Z", null)
        );
    }

    private static List<SeedRow> gapRows() {
        return List.of(
            row("2024-01-01T00:00:00Z", 1.0, 0, "2024-01-03T00:00:00Z", null),
            row("2024-01-01T00:01:00Z", 2.0, 0, "2024-01-03T00:01:00Z", null),
            row("2024-01-01T00:02:00Z", 3.0, 0, "2024-01-03T00:02:00Z", null),
            row("2024-01-01T00:05:00Z", 6.0, 0, "2024-01-03T00:05:00Z", null),
            row("2024-01-01T00:06:00Z", 7.0, 0, "2024-01-03T00:06:00Z", null),
            row("2024-01-01T00:07:00Z", 8.0, 0, "2024-01-03T00:07:00Z", null),
            row("2024-01-01T00:08:00Z", 9.0, 0, "2024-01-03T00:08:00Z", null),
            row("2024-01-01T00:09:00Z", 10.0, 0, "2024-01-03T00:09:00Z", null)
        );
    }

    private static List<SeedRow> versionedRows() {
        Instant olderVersion = Instant.parse("2024-06-20T08:00:00Z");
        Instant newerVersion = Instant.parse("2024-06-21T08:00:00Z");
        return List.of(
            row("2024-05-01T15:00:00Z", 4.0, 0, "2024-06-20T09:00:00Z", olderVersion),
            row("2024-05-01T16:00:00Z", 4.0, 0, "2024-06-20T09:01:00Z", olderVersion),
            row("2024-05-01T17:00:00Z", 4.0, 0, "2024-06-20T09:02:00Z", olderVersion),
            row("2024-05-01T18:00:00Z", 3.0, 0, "2024-06-20T09:03:00Z", olderVersion),
            row("2024-05-01T15:00:00Z", 1.0, 0, "2024-06-21T09:00:00Z", newerVersion),
            row("2024-05-01T16:00:00Z", 1.0, 0, "2024-06-21T09:01:00Z", newerVersion),
            row("2024-05-01T17:00:00Z", 1.0, 0, "2024-06-21T09:02:00Z", newerVersion)
        );
    }

    private static List<SeedRow> irregularRows() {
        return List.of(
            row("2024-01-05T12:00:00Z", 10.0, 0, "2024-01-06T00:00:00Z", null),
            row("2024-01-05T12:07:20Z", 20.0, 0, "2024-01-06T00:01:00Z", null),
            row("2024-01-05T12:19:45Z", 30.0, 0, "2024-01-06T00:02:00Z", null),
            row("2024-01-05T12:33:10Z", 40.0, 0, "2024-01-06T00:03:00Z", null)
        );
    }

    private static SeedRow row(String dateTime, Double value, int qualityCode, String dataEntryDate,
                               Instant versionDate) {
        return new SeedRow(
            Instant.parse(dateTime),
            value,
            qualityCode,
            Instant.parse(dataEntryDate),
            versionDate
        );
    }

    private static List<SeedRow> regularRows(Instant start, int count, double firstValue,
                                             Duration entryDateOffset) {
        return IntStream.range(0, count)
            .mapToObj(index -> new SeedRow(
                start.plusSeconds(index * 60L),
                firstValue + index,
                0,
                start.plus(entryDateOffset).plusSeconds(index * 60L),
                null
            ))
            .collect(Collectors.toList());
    }

    private static String buildMismatchSummary(List<TimeSeries.Record> expectedRows, TimeSeries actualResponse) {
        return "expectedRows=" + summarizeRows(expectedRows)
            + " actualRows=" + summarizeRows(actualResponse.getValues())
            + " actualTotal=" + actualResponse.getTotal();
    }

    private static String summarizeRows(List<TimeSeries.Record> rows) {
        if (rows == null) {
            return "null";
        }

        return rows.stream()
            .limit(12)
            .map(row -> "{t=" + toMillis(row.getDateTime())
                + ",v=" + row.getValue()
                + ",q=" + row.getQualityCode()
                + ",e=" + toMillis(row.getDataEntryDate())
                + "}")
            .collect(Collectors.joining(", ", "[", rows.size() > 12 ? ", ...]" : "]"));
    }

    private static long toMillis(Timestamp timestamp) {
        return timestamp != null ? timestamp.getTime() : Long.MIN_VALUE;
    }

    private static void assertRecordsEqual(TimeSeries.Record expected, TimeSeries.Record actual, int index) {
        assertEquals(expected.getDateTime(), actual.getDateTime(), "Row " + index + " timestamp");
        assertEquals(expected.getQualityCode(), actual.getQualityCode(), "Row " + index + " quality");

        if (expected.getValue() == null) {
            assertNull(actual.getValue(), "Row " + index + " value");
        } else {
            assertNotNull(actual.getValue(), "Row " + index + " value");
            assertEquals(expected.getValue(), actual.getValue(), DOUBLE_TOLERANCE, "Row " + index + " value");
        }

        assertEquals(expected.getDataEntryDate(), actual.getDataEntryDate(), "Row " + index + " entry date");
    }

    private static void seedTimeSeries(String locationId, String seriesId, List<SeedRow> rows,
                                       boolean versioned) throws SQLException {
        seedTimeSeries(locationId, seriesId, rows, versioned, false);
    }

    private static void seedTimeSeries(String locationId, String seriesId, List<SeedRow> rows,
                                       boolean versioned, boolean useNewLrtsInterval) throws SQLException {
        seedTimeSeries(locationId, seriesId, rows, versioned, 0, useNewLrtsInterval);
    }

    private static void seedTimeSeries(String locationId, String seriesId, List<SeedRow> rows,
                                       boolean versioned, Integer intervalOffset) throws SQLException {
        seedTimeSeries(locationId, seriesId, rows, versioned, intervalOffset, false);
    }

    private static void seedTimeSeries(String locationId, String seriesId, List<SeedRow> rows,
                                       boolean versioned, Integer intervalOffset,
                                       boolean useNewLrtsInterval) throws SQLException {
        createLocation(locationId, true, OFFICE);
        if (useNewLrtsInterval) {
            createTimeseriesWithNewLRTSInterval(OFFICE, seriesId, intervalOffset != null ? intervalOffset : 0);
        } else if (intervalOffset != null) {
            createTimeseries(OFFICE, seriesId, intervalOffset);
        } else {
            createTimeseries(OFFICE, seriesId);
        }

        CwmsDatabaseContainer<?> database = CwmsDataApiSetupCallback.getDatabaseLink();
        database.connection(connection -> {
            try {
                if (useNewLrtsInterval) {
                    useNewLrtsIdFormat(connection);
                }

                if (versioned) {
                    CWMS_TS_PACKAGE.call_SET_TSID_VERSIONED(DSL.using(connection).configuration(),
                        seriesId,
                        "T",
                        OFFICE);
                }

                long tsCode = useNewLrtsInterval
                    ? findTsCode(connection, seriesId, toLegacyLrtsId(seriesId))
                    : findTsCode(connection, seriesId);
                List<Integer> years = rows.stream()
                    .map(seedRow -> storageYear(seedRow.dateTime))
                    .distinct()
                    .collect(Collectors.toList());

                clearScenarioRows(connection, tsCode, years);
                insertScenarioRows(connection, tsCode, rows);
                updateScenarioExtents(connection, tsCode, rows);
            } catch (SQLException e) {
                throw new RuntimeException("Unable to seed time series " + seriesId, e);
            }
        }, "cwms_20");
    }

    private static long findTsCode(Connection connection, String seriesId) throws SQLException {
        return findTsCode(connection, seriesId, seriesId);
    }

    private static long findTsCode(Connection connection, String seriesId, String fallbackSeriesId)
            throws SQLException {
        Long tsCode = fetchTsCode(connection, seriesId);
        if (tsCode == null && !seriesId.equals(fallbackSeriesId)) {
            tsCode = fetchTsCode(connection, fallbackSeriesId);
        }
        if (tsCode == null) {
            throw new IllegalStateException("Unable to find ts_code for " + seriesId);
        }
        return tsCode;
    }

    private static Long fetchTsCode(Connection connection, String seriesId) throws SQLException {
        String sql = "select ts_code from at_cwms_ts_id where db_office_id = ? and cwms_ts_id = ?";
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setString(1, OFFICE);
            statement.setString(2, seriesId);
            try (ResultSet resultSet = statement.executeQuery()) {
                if (!resultSet.next()) {
                    return null;
                }
                return resultSet.getLong(1);
            }
        }
    }

    private static String toLegacyLrtsId(String seriesId) {
        String[] parts = seriesId.split("\\.", -1);
        String interval = parts[3];
        if (interval.endsWith("Local")) {
            parts[3] = "~" + interval.substring(0, interval.length() - "Local".length());
        }
        return String.join(".", parts);
    }

    private static void useNewLrtsIdFormat(Connection connection) {
        CWMS_UTIL_PACKAGE.call_SET_SESSION_INFO(DSL.using(connection).configuration(),
            SESSION_USE_LRTS_ID_FORMAT, "T", REQUIRE_NEW_LRTS_ID_FORMAT);
    }

    private static void clearScenarioRows(Connection connection, long tsCode, List<Integer> years) throws SQLException {
        for (Integer year : years) {
            try (PreparedStatement statement = connection.prepareStatement(
                "delete from at_tsv_" + year + " where ts_code = ?")) {
                statement.setLong(1, tsCode);
                statement.executeUpdate();
            }
        }

        try (PreparedStatement statement = connection.prepareStatement(
            "delete from at_ts_extents where ts_code = ?")) {
            statement.setLong(1, tsCode);
            statement.executeUpdate();
        }
    }

    private static void insertScenarioRows(Connection connection, long tsCode, List<SeedRow> rows)
        throws SQLException {
        List<SeedRow> sortedRows = new ArrayList<>(rows);
        sortedRows.sort(Comparator.comparing(seedRow -> seedRow.dateTime));

        Map<Integer, List<SeedRow>> rowsByYear = new LinkedHashMap<>();
        for (SeedRow row: sortedRows) {
            int year = storageYear(row.dateTime);
            rowsByYear.computeIfAbsent(year, ignored -> new ArrayList<>()).add(row);
        }

        for (Map.Entry<Integer, List<SeedRow>> entry: rowsByYear.entrySet()) {
            String sql = "insert into at_tsv_" + entry.getKey()
                + " (ts_code, date_time, version_date, data_entry_date, value, quality_code, dest_flag)"
                + " values (?, ?, ?, ?, ?, ?, 0)";
            try (PreparedStatement statement = connection.prepareStatement(sql)) {
                int batchCount = 0;
                for (SeedRow row: entry.getValue()) {
                    bindScenarioInsert(statement, tsCode, row);
                    statement.addBatch();
                    batchCount++;
                    if (batchCount % 1000 == 0) {
                        statement.executeBatch();
                    }
                }
                statement.executeBatch();
            }
        }
    }

    private static void bindScenarioInsert(PreparedStatement statement, long tsCode, SeedRow row)
        throws SQLException {
        statement.setLong(1, tsCode);
        statement.setTimestamp(2, Timestamp.from(row.dateTime));
        statement.setTimestamp(3, Timestamp.from(row.versionDate != null
            ? row.versionDate
            : Instant.parse("1111-11-11T00:00:00Z")));
        if (row.dataEntryDate != null) {
            statement.setTimestamp(4, Timestamp.from(row.dataEntryDate));
        } else {
            statement.setNull(4, Types.TIMESTAMP);
        }
        if (row.value != null) {
            statement.setDouble(5, row.value);
        } else {
            statement.setNull(5, Types.DOUBLE);
        }
        statement.setInt(6, row.qualityCode);
    }

    private static int storageYear(Instant instant) {
        return Timestamp.from(instant).toLocalDateTime().getYear();
    }

    private static void updateScenarioExtents(Connection connection, long tsCode, List<SeedRow> rows)
        throws SQLException {
        Set<Instant> distinctVersionDates = rows.stream()
            .map(seedRow -> seedRow.versionDate)
            .filter(Objects::nonNull)
            .collect(Collectors.toCollection(LinkedHashSet::new));

        if (distinctVersionDates.isEmpty()) {
            updateTsExtents(connection, tsCode, Instant.parse("1111-11-11T00:00:00Z"));
            return;
        }

        for (Instant versionDate : distinctVersionDates) {
            updateTsExtents(connection, tsCode, versionDate);
        }
    }

    private static void updateTsExtents(Connection connection, long tsCode, Instant versionDate)
        throws SQLException {
        String sql = "begin cwms_ts.update_ts_extents(?, ?); end;";
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setLong(1, tsCode);
            statement.setTimestamp(2, Timestamp.from(versionDate));
            statement.execute();
        }
    }

    private static List<TimeSeries.Record> fetchOracleRows(String seriesId, String units, Instant beginTime,
                                                           Instant endTime, boolean includeEntryDate,
                                                           Instant versionDate) throws SQLException {
        return fetchOracleRows(seriesId, units, beginTime, endTime, includeEntryDate, versionDate, false);
    }

    private static List<TimeSeries.Record> fetchOracleRows(String seriesId, String units, Instant beginTime,
                                                           Instant endTime, boolean includeEntryDate,
                                                           Instant versionDate, boolean useNewLrtsInterval)
            throws SQLException {
        CwmsDatabaseContainer<?> database = CwmsDataApiSetupCallback.getDatabaseLink();
        return database.connection(connection -> {
            try {
                if (useNewLrtsInterval) {
                    useNewLrtsIdFormat(connection);
                }

                String functionName = includeEntryDate
                    ? "cwms_20.cwms_ts.retrieve_ts_entry_out_tab"
                    : "cwms_20.cwms_ts.retrieve_ts_out_tab";
                String rowProjection = includeEntryDate
                    ? ", case when data_entry_date is null then null else round((cast(data_entry_date as date) - date '1970-01-01') * 86400000) end as data_entry_date_ms"
                    : "";
                String maxVersionFlag = versionDate != null ? "F" : "T";
                String sql = "select round((date_time - date '1970-01-01') * 86400000) as date_time_ms,"
                    + " value,"
                    + " quality_code"
                    + rowProjection
                    + " from table(" + functionName + "("
                    + "?, "
                    + "?, "
                    + "?, "
                    + "?, "
                    + "'UTC', 'T', 'T', 'T', 'F', 'F', "
                    + "?, "
                    + "?, "
                    + "?"
                    + "))"
                    + " order by date_time";

                try (PreparedStatement statement = connection.prepareStatement(sql)) {
                    statement.setString(1, seriesId);
                    statement.setString(2, units);
                    statement.setTimestamp(3, Timestamp.from(beginTime));
                    statement.setTimestamp(4, Timestamp.from(endTime));
                    if (versionDate != null) {
                        statement.setTimestamp(5, Timestamp.from(versionDate));
                    } else {
                        statement.setNull(5, Types.TIMESTAMP);
                    }
                    statement.setString(6, maxVersionFlag);
                    statement.setString(7, OFFICE);
                    try (ResultSet resultSet = statement.executeQuery()) {
                        List<TimeSeries.Record> rows = new ArrayList<>();
                        while (resultSet.next()) {
                            Double value = resultSet.getDouble("value");
                            if (resultSet.wasNull()) {
                                value = null;
                            }

                            Long dataEntryDateMillis = null;
                            if (includeEntryDate) {
                                long entryMillis = resultSet.getLong("data_entry_date_ms");
                                if (!resultSet.wasNull()) {
                                    dataEntryDateMillis = entryMillis;
                                }
                            }

                            rows.add(toRecord(
                                resultSet.getLong("date_time_ms"),
                                value,
                                resultSet.getInt("quality_code"),
                                dataEntryDateMillis
                            ));
                        }
                        return rows;
                    }
                }
            } catch (SQLException e) {
                throw new RuntimeException("Unable to fetch Oracle rows for " + seriesId, e);
            }
        }, "cwms_20");
    }

    private static TimeSeries.Record toRecord(long dateTimeMillis, Double value, int qualityCode,
                                              Long dataEntryDateMillis) {
        Timestamp dateTime = Timestamp.from(Instant.ofEpochMilli(dateTimeMillis));
        if (dataEntryDateMillis != null) {
            return new TimeSeries.Record(dateTime, value, qualityCode,
                Timestamp.from(Instant.ofEpochMilli(dataEntryDateMillis)));
        }
        return new TimeSeries.Record(dateTime, value, qualityCode);
    }

    private static TimeSeries fetchCdaRows(String seriesId, String units, Instant beginTime, Instant endTime,
                                           int seedRowCount, boolean includeEntryDate, Instant versionDate)
        throws Exception {
        return fetchCdaRows(seriesId, units, beginTime, endTime, seedRowCount, includeEntryDate, versionDate, null);
    }

    private static TimeSeries fetchCdaRows(String seriesId, String units, Instant beginTime, Instant endTime,
                                           int seedRowCount, boolean includeEntryDate, Instant versionDate,
                                           Boolean lrtsFormatting)
        throws Exception {
        int pageSize = Math.max(1000, seedRowCount * 2);
        return fetchCdaRowsWithPageSize(seriesId, units, beginTime, endTime, pageSize, includeEntryDate,
            versionDate, true, lrtsFormatting);
    }

    private static TimeSeries fetchCdaRowsWithPageSize(String seriesId, String units, Instant beginTime,
                                                       Instant endTime, int pageSize, boolean includeEntryDate,
                                                       Instant versionDate, boolean trim)
        throws Exception {
        return fetchCdaRowsWithPageSize(seriesId, units, beginTime, endTime, pageSize, includeEntryDate,
            versionDate, trim, null);
    }

    private static TimeSeries fetchCdaRowsWithPageSize(String seriesId, String units, Instant beginTime,
                                                       Instant endTime, int pageSize, boolean includeEntryDate,
                                                       Instant versionDate, boolean trim, Boolean lrtsFormatting)
        throws Exception {
        RequestSpecification request = given()
            .log().ifValidationFails(LogDetail.ALL, true)
            .accept(Formats.JSONV2)
            .queryParam(Controllers.OFFICE, OFFICE)
            .queryParam(Controllers.NAME, seriesId)
            .queryParam(Controllers.UNIT, units)
            .queryParam(Controllers.BEGIN, beginTime.toString())
            .queryParam(Controllers.END, endTime.toString())
            .queryParam(Controllers.PAGE_SIZE, pageSize)
            .queryParam(Controllers.TRIM, trim)
            .queryParam(Controllers.INCLUDE_ENTRY_DATE, includeEntryDate);
        if (lrtsFormatting != null) {
            request = request.header(ApiServlet.IS_NEW_LRTS, lrtsFormatting);
        }
        if (versionDate != null) {
            request = request.queryParam(Controllers.VERSION_DATE, versionDate.toString());
        }

        ExtractableResponse<Response> response = request.when()
            .redirects().follow(true)
            .redirects().max(3)
            .get("/timeseries/")
        .then()
            .log().ifValidationFails(LogDetail.ALL, true)
            .assertThat()
            .statusCode(is(HttpServletResponse.SC_OK))
            .extract();

        String responseBody = response.asString();
        TimeSeries timeSeries = OBJECT_MAPPER.readValue(responseBody, TimeSeries.class);
        if (!includeEntryDate) {
            return timeSeries;
        }

        JsonNode payload = OBJECT_MAPPER.readTree(responseBody);
        List<TimeSeries.Record> values = new ArrayList<>();
        for (JsonNode entry : payload.get("values")) {
            Long dataEntryDateMillis = null;
            if (entry.size() > 3 && !entry.get(3).isNull()) {
                dataEntryDateMillis = entry.get(3).asLong();
            }
            values.add(toRecord(
                entry.get(0).asLong(),
                entry.get(1).isNull() ? null : entry.get(1).asDouble(),
                entry.get(2).asInt(),
                dataEntryDateMillis
            ));
        }
        return timeSeries.withValues(values);
    }

    private static final class SeedRow {
        private final Instant dateTime;
        private final Double value;
        private final int qualityCode;
        private final Instant dataEntryDate;
        private final Instant versionDate;

        private SeedRow(Instant dateTime, Double value, int qualityCode, Instant dataEntryDate,
                        Instant versionDate) {
            this.dateTime = dateTime;
            this.value = value;
            this.qualityCode = qualityCode;
            this.dataEntryDate = dataEntryDate;
            this.versionDate = versionDate;
        }
    }
}
