package cwms.cda.api;

import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import cwms.cda.formatters.Formats;
import fixtures.CwmsDataApiSetupCallback;
import io.restassured.filter.log.LogDetail;
import io.restassured.response.ExtractableResponse;
import io.restassured.response.Response;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.IntStream;
import javax.servlet.http.HttpServletResponse;
import mil.army.usace.hec.test.database.CwmsDatabaseContainer;
import org.jooq.impl.DSL;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import usace.cwms.db.jooq.codegen.packages.CWMS_TS_PACKAGE;
import io.restassured.specification.RequestSpecification;

@Tag("integration")
final class TimeSeriesDirectReadParityIT extends DataApiTestIT {
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final String OFFICE = "SPK";
    private static final double DOUBLE_TOLERANCE = 1e-9;

    @ParameterizedTest(name = "{0}")
    @MethodSource("scenarios")
    void directReadMatchesOracleRetrieveTs(Scenario scenario) throws Exception {
        seedScenario(scenario);

        List<RetrievedRow> expectedRows = fetchOracleRows(scenario);
        TimeSeriesResponse actualResponse = fetchCdaRows(scenario);
        String mismatchSummary = buildMismatchSummary(expectedRows, actualResponse);

        assertEquals(expectedRows.size(), actualResponse.total, "Reported total " + mismatchSummary);
        assertEquals(scenario.expectedDateVersionType, actualResponse.dateVersionType, "Date version type");
        assertEquals(scenario.expectedInterval, actualResponse.interval, "Interval");
        assertEquals(scenario.expectedIntervalOffset, actualResponse.intervalOffset, "Interval offset");

        if (scenario.versionDate != null) {
            assertNotNull(actualResponse.versionDate, "Version date");
            assertEquals(scenario.versionDate, actualResponse.versionDate, "Version date");
        } else {
            assertNull(actualResponse.versionDate, "Version date");
        }

        assertEquals(expectedRows.size(), actualResponse.rows.size(), "Row count " + mismatchSummary);
        for (int i = 0; i < expectedRows.size(); i++) {
            assertRowsEqual(expectedRows.get(i), actualResponse.rows.get(i), i);
        }
    }

    private static String buildMismatchSummary(List<RetrievedRow> expectedRows, TimeSeriesResponse actualResponse) {
        return "expectedRows=" + summarizeRows(expectedRows)
            + " actualRows=" + summarizeRows(actualResponse.rows)
            + " actualTotal=" + actualResponse.total;
    }

    private static String summarizeRows(List<RetrievedRow> rows) {
        return rows.stream()
            .limit(12)
            .map(row -> "{t=" + row.dateTimeMillis
                + ",v=" + row.value
                + ",q=" + row.qualityCode
                + ",e=" + row.dataEntryDateMillis
                + "}")
            .collect(Collectors.joining(", ", "[", rows.size() > 12 ? ", ...]" : "]"));
    }

    private static Stream<Scenario> scenarios() {
        Instant olderVersion = Instant.parse("2024-06-20T08:00:00Z");
        Instant newerVersion = Instant.parse("2024-06-21T08:00:00Z");

        List<SeedRow> denseRows = List.of(
            row("2024-01-01T00:00:00Z", 1.0, 0, "2024-01-02T00:00:00Z", null),
            row("2024-01-01T00:01:00Z", 2.0, 0, "2024-01-02T00:01:00Z", null),
            row("2024-01-01T00:02:00Z", 3.0, 0, "2024-01-02T00:02:00Z", null),
            row("2024-01-01T00:03:00Z", 4.0, 0, "2024-01-02T00:03:00Z", null),
            row("2024-01-01T00:04:00Z", 5.0, 0, "2024-01-02T00:04:00Z", null),
            row("2024-01-01T00:05:00Z", 6.0, 0, "2024-01-02T00:05:00Z", null)
        );

        List<SeedRow> gapRows = List.of(
            row("2024-01-01T00:00:00Z", 1.0, 0, "2024-01-03T00:00:00Z", null),
            row("2024-01-01T00:01:00Z", 2.0, 0, "2024-01-03T00:01:00Z", null),
            row("2024-01-01T00:02:00Z", 3.0, 0, "2024-01-03T00:02:00Z", null),
            row("2024-01-01T00:05:00Z", 6.0, 0, "2024-01-03T00:05:00Z", null),
            row("2024-01-01T00:06:00Z", 7.0, 0, "2024-01-03T00:06:00Z", null),
            row("2024-01-01T00:07:00Z", 8.0, 0, "2024-01-03T00:07:00Z", null),
            row("2024-01-01T00:08:00Z", 9.0, 0, "2024-01-03T00:08:00Z", null),
            row("2024-01-01T00:09:00Z", 10.0, 0, "2024-01-03T00:09:00Z", null)
        );

        List<SeedRow> versionedRows = List.of(
            row("2024-05-01T15:00:00Z", 4.0, 0, "2024-06-20T09:00:00Z", olderVersion),
            row("2024-05-01T16:00:00Z", 4.0, 0, "2024-06-20T09:01:00Z", olderVersion),
            row("2024-05-01T17:00:00Z", 4.0, 0, "2024-06-20T09:02:00Z", olderVersion),
            row("2024-05-01T18:00:00Z", 3.0, 0, "2024-06-20T09:03:00Z", olderVersion),
            row("2024-05-01T15:00:00Z", 1.0, 0, "2024-06-21T09:00:00Z", newerVersion),
            row("2024-05-01T16:00:00Z", 1.0, 0, "2024-06-21T09:01:00Z", newerVersion),
            row("2024-05-01T17:00:00Z", 1.0, 0, "2024-06-21T09:02:00Z", newerVersion)
        );

        List<SeedRow> irregularRows = List.of(
            row("2024-01-05T12:00:00Z", 10.0, 0, "2024-01-06T00:00:00Z", null),
            row("2024-01-05T12:07:20Z", 20.0, 0, "2024-01-06T00:01:00Z", null),
            row("2024-01-05T12:19:45Z", 30.0, 0, "2024-01-06T00:02:00Z", null),
            row("2024-01-05T12:33:10Z", 40.0, 0, "2024-01-06T00:03:00Z", null)
        );

        Instant dstStart = Instant.parse("2024-03-09T00:00:00Z");
        List<SeedRow> dstRows = regularRows(dstStart, 5000, 1.0, Duration.ofDays(1));

        return Stream.of(
            new Scenario("dense-regular",
                "ITPARREG",
                "ITPARREG.Stage.Inst.1Minute.0.BENCH",
                "ft",
                Instant.parse("2024-01-01T00:00:00Z"),
                Instant.parse("2024-01-01T00:05:00Z"),
                denseRows,
                false,
                false,
                "UNVERSIONED",
                "PT1M",
                0L,
                null),
            new Scenario("dense-regular-entry-date",
                "ITPARREG",
                "ITPARREG.Stage.Inst.1Minute.0.BENCH",
                "ft",
                Instant.parse("2024-01-01T00:00:00Z"),
                Instant.parse("2024-01-01T00:05:00Z"),
                denseRows,
                false,
                true,
                "UNVERSIONED",
                "PT1M",
                0L,
                null),
            new Scenario("gap-regular",
                "ITPARGAP",
                "ITPARGAP.Stage.Inst.1Minute.0.BENCH",
                "ft",
                Instant.parse("2024-01-01T00:00:00Z"),
                Instant.parse("2024-01-01T00:09:00Z"),
                gapRows,
                false,
                false,
                "UNVERSIONED",
                "PT1M",
                0L,
                null),
            new Scenario("versioned-max",
                "ITPARVER",
                "ITPARVER.Flow.Inst.1Hour.0.BENCH",
                "cfs",
                Instant.parse("2024-05-01T15:00:00Z"),
                Instant.parse("2024-05-01T18:00:00Z"),
                versionedRows,
                true,
                false,
                "MAX_AGGREGATE",
                "PT1H",
                0L,
                null),
            new Scenario("versioned-single",
                "ITPARVER",
                "ITPARVER.Flow.Inst.1Hour.0.BENCH",
                "cfs",
                Instant.parse("2024-05-01T15:00:00Z"),
                Instant.parse("2024-05-01T18:00:00Z"),
                versionedRows,
                true,
                false,
                "SINGLE_VERSION",
                "PT1H",
                0L,
                newerVersion),
            new Scenario("irregular",
                "ITPARIRR",
                "ITPARIRR.Flow.Inst.0.0.BENCH",
                "cfs",
                Instant.parse("2024-01-05T12:00:00Z"),
                Instant.parse("2024-01-05T12:33:10Z"),
                irregularRows,
                false,
                false,
                "UNVERSIONED",
                "PT0S",
                Integer.MIN_VALUE,
                null),
            new Scenario("dense-regular-dst-window",
                "ITPARDST",
                "ITPARDST.Stage.Inst.1Minute.0.BENCH",
                "ft",
                dstStart,
                dstStart.plus(Duration.ofMinutes(4999)),
                dstRows,
                false,
                false,
                "UNVERSIONED",
                "PT1M",
                0L,
                null)
        );
    }

    private static SeedRow row(String dateTime, Double value, int qualityCode, String dataEntryDate, Instant versionDate) {
        return new SeedRow(
            Instant.parse(dateTime),
            value,
            qualityCode,
            Instant.parse(dataEntryDate),
            versionDate
        );
    }

    private static List<SeedRow> regularRows(Instant start, int count, double firstValue, Duration entryDateOffset) {
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

    private static void assertRowsEqual(RetrievedRow expected, RetrievedRow actual, int index) {
        assertEquals(expected.dateTimeMillis, actual.dateTimeMillis, "Row " + index + " timestamp");
        assertEquals(expected.qualityCode, actual.qualityCode, "Row " + index + " quality");

        if (expected.value == null) {
            assertNull(actual.value, "Row " + index + " value");
        } else {
            assertNotNull(actual.value, "Row " + index + " value");
            assertEquals(expected.value, actual.value, DOUBLE_TOLERANCE, "Row " + index + " value");
        }

        if (expected.dataEntryDateMillis == null) {
            assertNull(actual.dataEntryDateMillis, "Row " + index + " entry date");
        } else {
            assertEquals(expected.dataEntryDateMillis, actual.dataEntryDateMillis, "Row " + index + " entry date");
        }
    }

    private static void seedScenario(Scenario scenario) throws SQLException {
        createLocation(scenario.locationId, true, OFFICE);
        createTimeseries(OFFICE, scenario.seriesId, 0);

        CwmsDatabaseContainer<?> database = CwmsDataApiSetupCallback.getDatabaseLink();
        database.connection(connection -> {
            try {
                CWMS_TS_PACKAGE.call_SET_TSID_VERSIONED(DSL.using(connection).configuration(),
                    scenario.seriesId,
                    scenario.versioned ? "T" : "F",
                    OFFICE);

                long tsCode = findTsCode(connection, scenario.seriesId);
                List<Integer> years = scenario.rows.stream()
                    .map(row -> OffsetDateTime.ofInstant(row.dateTime, ZoneOffset.UTC).getYear())
                    .distinct()
                    .collect(Collectors.toList());

                clearScenarioRows(connection, tsCode, years);
                insertScenarioRows(connection, tsCode, scenario.rows);
                updateScenarioExtents(connection, tsCode, scenario.rows);
            } catch (SQLException e) {
                throw new RuntimeException("Unable to seed scenario " + scenario.name, e);
            }
        }, "cwms_20");
    }

    private static long findTsCode(Connection connection, String seriesId) throws SQLException {
        String sql = "select ts_code from at_cwms_ts_id where db_office_id = ? and cwms_ts_id = ?";
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setString(1, OFFICE);
            statement.setString(2, seriesId);
            try (ResultSet resultSet = statement.executeQuery()) {
                if (!resultSet.next()) {
                    throw new IllegalStateException("Unable to find ts_code for " + seriesId);
                }
                return resultSet.getLong(1);
            }
        }
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

    private static void insertScenarioRows(Connection connection, long tsCode, List<SeedRow> rows) throws SQLException {
        List<SeedRow> sortedRows = new ArrayList<>(rows);
        sortedRows.sort(Comparator.comparing(seedRow -> seedRow.dateTime));

        for (SeedRow row : sortedRows) {
            int year = OffsetDateTime.ofInstant(row.dateTime, ZoneOffset.UTC).getYear();
            String sql = "insert into at_tsv_" + year
                + " (ts_code, date_time, version_date, data_entry_date, value, quality_code, dest_flag)"
                + " values ("
                + tsCode + ", "
                + toOracleDateExpression(row.dateTime) + ", "
                + (row.versionDate != null ? toOracleDateExpression(row.versionDate) : "date '1111-11-11'") + ", "
                + (row.dataEntryDate != null ? toOracleTimestampExpression(row.dataEntryDate) : "null") + ", "
                + (row.value != null ? Double.toString(row.value) : "null") + ", "
                + row.qualityCode
                + ", 0)";
            try (PreparedStatement statement = connection.prepareStatement(sql)) {
                statement.executeUpdate();
            }
        }
    }

    private static void updateScenarioExtents(Connection connection, long tsCode, List<SeedRow> rows) throws SQLException {
        Set<Instant> distinctVersionDates = rows.stream()
            .map(seedRow -> seedRow.versionDate)
            .filter(Objects::nonNull)
            .collect(Collectors.toCollection(LinkedHashSet::new));

        if (distinctVersionDates.isEmpty()) {
            updateTsExtents(connection, tsCode, "date '1111-11-11'");
            return;
        }

        for (Instant versionDate : distinctVersionDates) {
            updateTsExtents(connection, tsCode, toOracleDateExpression(versionDate));
        }
    }

    private static void updateTsExtents(Connection connection, long tsCode, String versionDateExpression) throws SQLException {
        String sql = "begin cwms_ts.update_ts_extents(" + tsCode + ", " + versionDateExpression + "); end;";
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.execute();
        }
    }

    private static List<RetrievedRow> fetchOracleRows(Scenario scenario) throws SQLException {
        CwmsDatabaseContainer<?> database = CwmsDataApiSetupCallback.getDatabaseLink();
        return database.connection(connection -> {
            try {
                String functionName = scenario.includeEntryDate
                    ? "cwms_20.cwms_ts.retrieve_ts_entry_out_tab"
                    : "cwms_20.cwms_ts.retrieve_ts_out_tab";
                String rowProjection = scenario.includeEntryDate
                    ? ", case when data_entry_date is null then null else round((cast(data_entry_date as date) - date '1970-01-01') * 86400000) end as data_entry_date_ms"
                    : "";
                String versionDateExpression = scenario.versionDate != null
                    ? toOracleDateExpression(scenario.versionDate)
                    : "null";
                String maxVersionFlag = scenario.versionDate != null ? "'F'" : "'T'";
                String sql = "select round((date_time - date '1970-01-01') * 86400000) as date_time_ms,"
                    + " value,"
                    + " quality_code"
                    + rowProjection
                    + " from table(" + functionName + "("
                    + toSqlStringLiteral(scenario.seriesId) + ", "
                    + toSqlStringLiteral(scenario.units) + ", "
                    + toOracleDateExpression(scenario.beginTime) + ", "
                    + toOracleDateExpression(scenario.endTime) + ", "
                    + "'UTC', 'T', 'T', 'T', 'F', 'F', "
                    + versionDateExpression + ", "
                    + maxVersionFlag + ", "
                    + toSqlStringLiteral(OFFICE)
                    + "))"
                    + " order by date_time";

                try (PreparedStatement statement = connection.prepareStatement(sql)) {
                    try (ResultSet resultSet = statement.executeQuery()) {
                        List<RetrievedRow> rows = new ArrayList<>();
                        while (resultSet.next()) {
                            Double value = resultSet.getDouble("value");
                            if (resultSet.wasNull()) {
                                value = null;
                            }

                            Long dataEntryDateMillis = null;
                            if (scenario.includeEntryDate) {
                                long entryMillis = resultSet.getLong("data_entry_date_ms");
                                if (!resultSet.wasNull()) {
                                    dataEntryDateMillis = entryMillis;
                                }
                            }

                            rows.add(new RetrievedRow(
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
                throw new RuntimeException("Unable to fetch Oracle rows for " + scenario.name, e);
            }
        }, "cwms_20");
    }

    private static TimeSeriesResponse fetchCdaRows(Scenario scenario) throws Exception {
        int pageSize = Math.max(1000, scenario.rows.size() * 2);
        RequestSpecification request = given()
            .log().ifValidationFails(LogDetail.ALL, true)
            .accept(Formats.JSONV2)
            .queryParam(Controllers.OFFICE, OFFICE)
            .queryParam(Controllers.NAME, scenario.seriesId)
            .queryParam(Controllers.UNIT, scenario.units)
            .queryParam(Controllers.BEGIN, scenario.beginTime.toString())
            .queryParam(Controllers.END, scenario.endTime.toString())
            .queryParam("page-size", pageSize)
            .queryParam(Controllers.INCLUDE_ENTRY_DATE, scenario.includeEntryDate);
        if (scenario.versionDate != null) {
            request = request.queryParam(Controllers.VERSION_DATE, scenario.versionDate.toString());
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

        JsonNode payload = OBJECT_MAPPER.readTree(response.asString());
        List<RetrievedRow> rows = new ArrayList<>();
        for (JsonNode entry : payload.get("values")) {
            Double value = entry.get(1).isNull() ? null : entry.get(1).asDouble();
            Long dataEntryDateMillis = null;
            if (scenario.includeEntryDate && entry.size() > 3 && !entry.get(3).isNull()) {
                dataEntryDateMillis = entry.get(3).asLong();
            }
            rows.add(new RetrievedRow(
                entry.get(0).asLong(),
                value,
                entry.get(2).asInt(),
                dataEntryDateMillis
            ));
        }

        Instant versionDate = null;
        JsonNode versionDateNode = payload.get("version-date");
        if (versionDateNode != null && !versionDateNode.isNull()) {
            versionDate = OffsetDateTime.parse(versionDateNode.asText()).toInstant();
        }

        return new TimeSeriesResponse(
            rows,
            payload.get("total").asInt(),
            payload.get("date-version-type").asText(),
            payload.get("interval").asText(),
            payload.get("interval-offset").asLong(),
            versionDate
        );
    }

    private static String toSqlStringLiteral(String value) {
        return "'" + value.replace("'", "''") + "'";
    }

    private static String toOracleDateExpression(Instant instant) {
        LocalDateTime utc = LocalDateTime.ofInstant(instant, ZoneOffset.UTC);
        return "to_date('" + utc.format(java.time.format.DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"))
            + "', 'yyyy-mm-dd hh24:mi:ss')";
    }

    private static String toOracleTimestampExpression(Instant instant) {
        LocalDateTime utc = LocalDateTime.ofInstant(instant, ZoneOffset.UTC);
        return "to_timestamp('" + utc.format(java.time.format.DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"))
            + "', 'yyyy-mm-dd hh24:mi:ss')";
    }

    private static final class Scenario {
        private final String name;
        private final String locationId;
        private final String seriesId;
        private final String units;
        private final Instant beginTime;
        private final Instant endTime;
        private final List<SeedRow> rows;
        private final boolean versioned;
        private final boolean includeEntryDate;
        private final String expectedDateVersionType;
        private final String expectedInterval;
        private final long expectedIntervalOffset;
        private final Instant versionDate;

        private Scenario(String name, String locationId, String seriesId, String units, Instant beginTime,
                         Instant endTime, List<SeedRow> rows, boolean versioned, boolean includeEntryDate,
                         String expectedDateVersionType, String expectedInterval, long expectedIntervalOffset,
                         Instant versionDate) {
            this.name = name;
            this.locationId = locationId;
            this.seriesId = seriesId;
            this.units = units;
            this.beginTime = beginTime;
            this.endTime = endTime;
            this.rows = rows;
            this.versioned = versioned;
            this.includeEntryDate = includeEntryDate;
            this.expectedDateVersionType = expectedDateVersionType;
            this.expectedInterval = expectedInterval;
            this.expectedIntervalOffset = expectedIntervalOffset;
            this.versionDate = versionDate;
        }

        @Override
        public String toString() {
            return name;
        }
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

    private static final class RetrievedRow {
        private final long dateTimeMillis;
        private final Double value;
        private final int qualityCode;
        private final Long dataEntryDateMillis;

        private RetrievedRow(long dateTimeMillis, Double value, int qualityCode, Long dataEntryDateMillis) {
            this.dateTimeMillis = dateTimeMillis;
            this.value = value;
            this.qualityCode = qualityCode;
            this.dataEntryDateMillis = dataEntryDateMillis;
        }
    }

    private static final class TimeSeriesResponse {
        private final List<RetrievedRow> rows;
        private final int total;
        private final String dateVersionType;
        private final String interval;
        private final long intervalOffset;
        private final Instant versionDate;

        private TimeSeriesResponse(List<RetrievedRow> rows, int total, String dateVersionType,
                                   String interval, long intervalOffset, Instant versionDate) {
            this.rows = rows;
            this.total = total;
            this.dateVersionType = dateVersionType;
            this.interval = interval;
            this.intervalOffset = intervalOffset;
            this.versionDate = versionDate;
        }
    }
}
