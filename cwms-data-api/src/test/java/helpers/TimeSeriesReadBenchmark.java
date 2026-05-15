package helpers;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.ObjectMapper;
import fixtures.CwmsDataApiSetupCallback;
import fixtures.KeyCloakExtension;
import fixtures.MinIOExtension;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import mil.army.usace.hec.test.database.CwmsDatabaseContainer;
import org.jooq.impl.DSL;
import usace.cwms.db.jooq.codegen.packages.CWMS_TS_PACKAGE;

public final class TimeSeriesReadBenchmark {
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final JsonFactory JSON_FACTORY = new JsonFactory();
    private static final DateTimeFormatter REQUEST_TIME_FORMAT =
            DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss'Z'").withZone(ZoneOffset.UTC);
    private static final DateTimeFormatter ORACLE_DATE_TIME_FORMAT =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss").withZone(ZoneOffset.UTC);
    private static final String ACCEPT_JSON_V2 = "application/json;version=2";
    private static final Instant NON_VERSIONED_DATE = Instant.parse("1111-11-11T00:00:00Z");

    private TimeSeriesReadBenchmark() {
    }

    public static void main(String[] args) throws Exception {
        BenchmarkConfig config = BenchmarkConfig.fromSystemProperties();
        System.out.println("Starting benchmark fixtures...");

        try {
            new KeyCloakExtension().beforeAll(null);
            new MinIOExtension().beforeAll(null);
            new CwmsDataApiSetupCallback().beforeAll(null);

            System.out.println("Running benchmark...");
            BenchmarkReport report = runBenchmark(config);

            Files.createDirectories(config.resultsDir);
            Path resultFile = config.resultsDir.resolve("timeseries-read-benchmark-"
                    + DateTimeFormatter.ofPattern("yyyyMMdd-HHmmss").withZone(ZoneOffset.UTC).format(Instant.now())
                    + ".json");

            OBJECT_MAPPER.writerWithDefaultPrettyPrinter().writeValue(resultFile.toFile(), report);
            OBJECT_MAPPER.writerWithDefaultPrettyPrinter().writeValue(System.out, report);
            System.out.println();
            System.out.println("Benchmark report written to " + resultFile);

            for (BenchmarkRun run: report.runs) {
                if (run.httpCode != 200) {
                    throw new IllegalStateException(
                            "Benchmark completed with HTTP failures. Results saved to " + resultFile);
                }
            }
        } finally {
            System.out.println("Shutting down benchmark fixtures...");
            shutdownFixtures();
        }
    }

    private static BenchmarkReport runBenchmark(BenchmarkConfig config) throws Exception {
        Files.createDirectories(config.resultsDir);
        Files.createDirectories(config.responsesDir);

        SeedInfo seed = ensureBenchmarkSeed(config);
        if (seed.pointCount != config.pointCount) {
            throw new IllegalStateException("Expected " + config.pointCount + " seeded points but found "
                    + seed.pointCount);
        }

        waitForCdaReady(config);
        if (config.warmup) {
            Path warmupFile = config.responsesDir.resolve("warmup.json");
            executeRequest(config, warmupFile);
            if (!config.keepResponses) {
                Files.deleteIfExists(warmupFile);
            }
        }

        List<BenchmarkRun> runs = new ArrayList<>();
        for (int runIndex = 1; runIndex <= config.runs; runIndex++) {
            runs.add(executeRun(config, runIndex));
        }

        return new BenchmarkReport(
                "timeseries-read",
                Instant.now().toString(),
                resolveGitValue("git", "branch", "--show-current"),
                resolveGitValue("git", "rev-parse", "HEAD"),
                config.office,
                config.locationId,
                config.seriesId,
                config.units,
                config.startTime.toString(),
                config.endTime.toString(),
                config.pointCount,
                config.pageSize,
                config.requestUrl().toString(),
                seed,
                BenchmarkSummary.fromRuns(runs),
                runs
        );
    }

    private static SeedInfo ensureBenchmarkSeed(BenchmarkConfig config) throws SQLException {
        long existingCount = getSeededPointCount(config);
        if (config.skipSeed) {
            return new SeedInfo(false, existingCount);
        }
        if (!config.forceReseed && existingCount == config.pointCount) {
            return new SeedInfo(false, existingCount);
        }

        CwmsDatabaseContainer<?> database = CwmsDataApiSetupCallback.getDatabaseLink();
        database.connection(connection -> {
            try {
                ensureLocationExists(connection, config);
                ensureTimeSeriesExists(connection, config);
                CWMS_TS_PACKAGE.call_SET_TSID_VERSIONED(
                        DSL.using(connection).configuration(), config.seriesId, "F", config.office);

                long tsCode = findTsCode(connection, config.office, config.seriesId);
                List<YearSegment> segments = buildYearSegments(config.startTime, config.pointCount);
                clearSeededRows(connection, tsCode, segments);
                insertSeededRows(connection, tsCode, segments);
                updateTsExtents(connection, tsCode);
                if (!connection.getAutoCommit()) {
                    connection.commit();
                }
            } catch (SQLException e) {
                throw new RuntimeException("Unable to seed benchmark series " + config.seriesId, e);
            }
        }, "cwms_20");

        return new SeedInfo(true, getSeededPointCount(config));
    }

    private static void ensureLocationExists(Connection connection, BenchmarkConfig config) throws SQLException {
        String sql = "declare "
                + "location_exists exception; "
                + "pragma exception_init(location_exists, -20026); "
                + "begin "
                + "cwms_loc.create_location(?, ?, null, null, null, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?); "
                + "exception when location_exists then null; "
                + "end;";
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setString(1, config.locationId);
            statement.setString(2, "SITE");
            statement.setDouble(3, 38.0d);
            statement.setDouble(4, -90.0d);
            statement.setString(5, "NAD83");
            statement.setString(6, config.locationId);
            statement.setString(7, config.locationId + " Benchmark Location");
            statement.setString(8, "Performance benchmark location");
            statement.setString(9, "UTC");
            statement.setString(10, null);
            statement.setString(11, null);
            statement.setString(12, "T");
            statement.setString(13, config.office);
            statement.execute();
        }
    }

    private static void ensureTimeSeriesExists(Connection connection, BenchmarkConfig config) throws SQLException {
        String sql = "declare "
                + "ts_exists exception; "
                + "pragma exception_init(ts_exists, -20003); "
                + "begin "
                + "cwms_ts.create_ts(?, ?, 0); "
                + "exception when ts_exists then null; "
                + "end;";
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setString(1, config.office);
            statement.setString(2, config.seriesId);
            statement.execute();
        }
    }

    private static long findTsCode(Connection connection, String office, String seriesId) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(
                "select ts_code from at_cwms_ts_id where db_office_id = ? and cwms_ts_id = ?")) {
            statement.setString(1, office);
            statement.setString(2, seriesId);
            try (ResultSet resultSet = statement.executeQuery()) {
                if (!resultSet.next()) {
                    throw new IllegalStateException("Unable to find ts_code for " + seriesId);
                }
                return resultSet.getLong(1);
            }
        }
    }

    private static void clearSeededRows(Connection connection, long tsCode, List<YearSegment> segments) throws SQLException {
        for (YearSegment segment : segments) {
            try (PreparedStatement statement = connection.prepareStatement(
                    "delete from at_tsv_" + segment.year + " where ts_code = ?")) {
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

    private static void insertSeededRows(Connection connection, long tsCode, List<YearSegment> segments) throws SQLException {
        for (YearSegment segment : segments) {
            String sql = "insert /*+ APPEND */ into at_tsv_" + segment.year
                    + " (ts_code, date_time, version_date, data_entry_date, value, quality_code, dest_flag) "
                    + "select ?, to_date(?, 'yyyy-mm-dd hh24:mi:ss') + numtodsinterval(level - 1, 'MINUTE'), "
                    + "?, systimestamp, ? + level - 1, 0, 0 "
                    + "from dual connect by level <= ?";
            try (PreparedStatement statement = connection.prepareStatement(sql)) {
                statement.setLong(1, tsCode);
                statement.setString(2, ORACLE_DATE_TIME_FORMAT.format(segment.startTime));
                statement.setTimestamp(3, Timestamp.from(NON_VERSIONED_DATE));
                statement.setLong(4, segment.valueStart);
                statement.setInt(5, segment.count);
                statement.executeUpdate();
            }
        }
    }

    private static void updateTsExtents(Connection connection, long tsCode) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(
                "begin cwms_ts.update_ts_extents(?, ?); end;")) {
            statement.setLong(1, tsCode);
            statement.setTimestamp(2, Timestamp.from(NON_VERSIONED_DATE));
            statement.execute();
        }
    }

    private static long getSeededPointCount(BenchmarkConfig config) throws SQLException {
        CwmsDatabaseContainer<?> database = CwmsDataApiSetupCallback.getDatabaseLink();
        return database.connection(connection -> {
            try (PreparedStatement statement = connection.prepareStatement(
                    "select count(*) from av_tsv v "
                            + "join at_cwms_ts_id t on t.ts_code = v.ts_code "
                            + "where t.db_office_id = ? and t.cwms_ts_id = ?")) {
                statement.setString(1, config.office);
                statement.setString(2, config.seriesId);
                try (ResultSet resultSet = statement.executeQuery()) {
                    resultSet.next();
                    return resultSet.getLong(1);
                }
            } catch (SQLException e) {
                throw new RuntimeException("Unable to count seeded rows for " + config.seriesId, e);
            }
        }, "cwms_20");
    }

    private static List<YearSegment> buildYearSegments(Instant startTime, int pointCount) {
        List<YearSegment> segments = new ArrayList<>();
        Instant cursor = startTime;
        int remaining = pointCount;
        long valueStart = 1L;
        while (remaining > 0) {
            Instant nextYear = cursor.atOffset(ZoneOffset.UTC)
                    .withDayOfYear(1)
                    .withHour(0)
                    .withMinute(0)
                    .withSecond(0)
                    .withNano(0)
                    .plusYears(1)
                    .toInstant();
            long minutesUntilNextYear = Math.max(1L, Duration.between(cursor, nextYear).toMinutes());
            int segmentCount = (int) Math.min(remaining, minutesUntilNextYear);
            segments.add(new YearSegment(cursor.atOffset(ZoneOffset.UTC).getYear(), cursor, segmentCount, valueStart));
            cursor = cursor.plusSeconds(segmentCount * 60L);
            valueStart += segmentCount;
            remaining -= segmentCount;
        }
        return segments;
    }

    private static void waitForCdaReady(BenchmarkConfig config) throws Exception {
        HttpClient client = HttpClient.newHttpClient();
        URI readinessUri = URI.create(config.resolvedBaseUrl() + "/offices/" + urlEncode(config.office));
        for (int attempt = 0; attempt < 30; attempt++) {
            HttpRequest request = HttpRequest.newBuilder(readinessUri)
                    .header("Accept", ACCEPT_JSON_V2)
                    .GET()
                    .build();
            HttpResponse<InputStream> response = client.send(request, HttpResponse.BodyHandlers.ofInputStream());
            try (InputStream ignored = response.body()) {
                if (response.statusCode() == 200) {
                    return;
                }
            }
            Thread.sleep(1000L);
        }
        throw new IllegalStateException("CDA did not become ready at " + readinessUri);
    }

    private static BenchmarkRun executeRun(BenchmarkConfig config, int runIndex) throws Exception {
        Path responseFile = config.responsesDir.resolve("timeseries-read-run-" + runIndex + ".json");
        RequestResult requestResult = executeRequest(config, responseFile);
        ResponseSummary responseSummary = summarizeResponse(responseFile);
        String responseFileValue = responseFile.toAbsolutePath().toString();
        if (!config.keepResponses && requestResult.httpCode == 200) {
            Files.deleteIfExists(responseFile);
            responseFileValue = null;
        }
        return new BenchmarkRun(
                runIndex,
                requestResult.httpCode,
                roundSeconds(requestResult.timeTotalNanos),
                responseSummary.responseBytes,
                responseSummary.reportedTotal,
                responseSummary.reportedPageSize,
                responseSummary.firstTimestamp,
                responseSummary.lastTimestamp,
                requestResult.httpCode == 200 ? null : Files.readString(responseFile),
                responseFileValue
        );
    }

    private static RequestResult executeRequest(BenchmarkConfig config, Path responseFile) throws Exception {
        HttpClient client = HttpClient.newHttpClient();
        HttpRequest request = HttpRequest.newBuilder(config.requestUrl())
                .header("Accept", ACCEPT_JSON_V2)
                .GET()
                .build();
        long startNanos = System.nanoTime();
        HttpResponse<Path> response = client.send(request, HttpResponse.BodyHandlers.ofFile(responseFile));
        long endNanos = System.nanoTime();
        return new RequestResult(response.statusCode(), endNanos - startNanos);
    }

    private static ResponseSummary summarizeResponse(Path responseFile) throws IOException {
        Integer reportedTotal = null;
        Integer reportedPageSize = null;
        Long firstTimestamp = null;
        Long lastTimestamp = null;

        try (InputStream inputStream = Files.newInputStream(responseFile);
             JsonParser parser = JSON_FACTORY.createParser(inputStream)) {
            while (parser.nextToken() != null) {
                if (parser.currentToken() != JsonToken.FIELD_NAME) {
                    continue;
                }
                String fieldName = parser.currentName();
                JsonToken valueToken = parser.nextToken();
                if ("total".equals(fieldName) && valueToken != JsonToken.VALUE_NULL) {
                    reportedTotal = parser.getIntValue();
                } else if ("page-size".equals(fieldName) && valueToken != JsonToken.VALUE_NULL) {
                    reportedPageSize = parser.getIntValue();
                } else if ("values".equals(fieldName) && valueToken == JsonToken.START_ARRAY) {
                    while (parser.nextToken() != JsonToken.END_ARRAY) {
                        if (parser.currentToken() != JsonToken.START_ARRAY) {
                            parser.skipChildren();
                            continue;
                        }
                        parser.nextToken();
                        long timestamp = parser.getLongValue();
                        if (firstTimestamp == null) {
                            firstTimestamp = timestamp;
                        }
                        lastTimestamp = timestamp;
                        while (parser.nextToken() != JsonToken.END_ARRAY) {
                            parser.skipChildren();
                        }
                    }
                } else {
                    parser.skipChildren();
                }
            }
        }

        return new ResponseSummary(
                Files.size(responseFile),
                reportedTotal,
                reportedPageSize,
                firstTimestamp,
                lastTimestamp
        );
    }

    private static String resolveGitValue(String... command) {
        ProcessBuilder processBuilder = new ProcessBuilder(command);
        processBuilder.redirectErrorStream(true);
        try {
            Process process = processBuilder.start();
            byte[] outputBytes = process.getInputStream().readAllBytes();
            int exitCode = process.waitFor();
            if (exitCode != 0) {
                return null;
            }
            String value = new String(outputBytes, StandardCharsets.UTF_8).trim();
            return value.isEmpty() ? null : value;
        } catch (IOException e) {
            return null;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return null;
        }
    }

    private static double roundSeconds(long nanos) {
        return Math.round((nanos / 1_000_000_000.0d) * 1_000_000.0d) / 1_000_000.0d;
    }

    private static String urlEncode(String value) {
        return URLEncoder.encode(value, StandardCharsets.UTF_8);
    }

    private static void shutdownFixtures() throws Exception {
        Exception failure = null;
        try {
            CwmsDataApiSetupCallback.shutdown();
        } catch (Exception e) {
            failure = e;
        }

        try {
            MinIOExtension.shutdown();
        } catch (Exception e) {
            if (failure == null) {
                failure = e;
            } else {
                failure.addSuppressed(e);
            }
        }

        try {
            KeyCloakExtension.shutdown();
        } catch (Exception e) {
            if (failure == null) {
                failure = e;
            } else {
                failure.addSuppressed(e);
            }
        }

        if (failure != null) {
            throw failure;
        }
    }

    private static final class BenchmarkConfig {
        private final String office;
        private final String locationId;
        private final String seriesId;
        private final String units;
        private final String baseUrl;
        private final Instant startTime;
        private final Instant endTime;
        private final int pointCount;
        private final int pageSize;
        private final int runs;
        private final boolean warmup;
        private final boolean skipSeed;
        private final boolean forceReseed;
        private final boolean keepResponses;
        private final Path resultsDir;
        private final Path responsesDir;

        private BenchmarkConfig(String office, String locationId, String seriesId, String units, String baseUrl,
                                Instant startTime, int pointCount, int pageSize, int runs, boolean warmup,
                                boolean skipSeed, boolean forceReseed, boolean keepResponses, Path resultsDir,
                                Path responsesDir) {
            this.office = office;
            this.locationId = locationId;
            this.seriesId = seriesId;
            this.units = units;
            this.baseUrl = baseUrl;
            this.startTime = startTime;
            this.endTime = startTime.plusSeconds(Math.max(0L, pointCount - 1L) * 60L);
            this.pointCount = pointCount;
            this.pageSize = pageSize;
            this.runs = runs;
            this.warmup = warmup;
            this.skipSeed = skipSeed;
            this.forceReseed = forceReseed;
            this.keepResponses = keepResponses;
            this.resultsDir = resultsDir;
            this.responsesDir = responsesDir;
        }

        private static BenchmarkConfig fromSystemProperties() {
            String office = System.getProperty("benchmark.office", "SPK");
            String locationId = System.getProperty("benchmark.locationId", "PERF1MREAD");
            String seriesId = System.getProperty("benchmark.seriesId", "PERF1MREAD.Stage.Inst.1Minute.0.BENCH");
            String units = System.getProperty("benchmark.units", "ft");
            String baseUrl = System.getProperty("benchmark.baseUrl");
            Instant startTime = Instant.parse(System.getProperty("benchmark.startTime", "2024-01-01T00:00:00Z"));
            int pointCount = Integer.parseInt(System.getProperty("benchmark.pointCount", "1000000"));
            int pageSize = Integer.parseInt(System.getProperty("benchmark.pageSize", String.valueOf(pointCount)));
            int runs = Integer.parseInt(System.getProperty("benchmark.runs", "1"));
            boolean warmup = Boolean.parseBoolean(System.getProperty("benchmark.warmup", "false"));
            boolean skipSeed = Boolean.parseBoolean(System.getProperty("benchmark.skipSeed", "false"));
            boolean forceReseed = Boolean.parseBoolean(System.getProperty("benchmark.forceReseed", "false"));
            boolean keepResponses = Boolean.parseBoolean(System.getProperty("benchmark.keepResponses", "false"));
            Path resultsDir = Paths.get(System.getProperty("benchmark.resultsDir",
                    "..\\load_data\\performance\\results")).normalize().toAbsolutePath();
            Path responsesDir = Paths.get(System.getProperty("benchmark.responsesDir",
                    "..\\load_data\\performance\\responses")).normalize().toAbsolutePath();
            return new BenchmarkConfig(office, locationId, seriesId, units, baseUrl, startTime, pointCount,
                    pageSize, runs, warmup, skipSeed, forceReseed, keepResponses, resultsDir, responsesDir);
        }

        private URI requestUrl() {
            StringBuilder builder = new StringBuilder(resolvedBaseUrl());
            builder.append("/timeseries?office=").append(urlEncode(office));
            builder.append("&name=").append(urlEncode(seriesId));
            builder.append("&units=").append(urlEncode(units));
            builder.append("&begin=").append(urlEncode(REQUEST_TIME_FORMAT.format(startTime)));
            builder.append("&end=").append(urlEncode(REQUEST_TIME_FORMAT.format(endTime)));
            builder.append("&page-size=").append(pageSize);
            return URI.create(builder.toString());
        }

        private String resolvedBaseUrl() {
            if (baseUrl != null && !baseUrl.isBlank()) {
                return baseUrl;
            }
            return CwmsDataApiSetupCallback.httpUrl() + ":" + CwmsDataApiSetupCallback.httpPort()
                    + System.getProperty("warContext");
        }
    }

    private static final class YearSegment {
        private final int year;
        private final Instant startTime;
        private final int count;
        private final long valueStart;

        private YearSegment(int year, Instant startTime, int count, long valueStart) {
            this.year = year;
            this.startTime = startTime;
            this.count = count;
            this.valueStart = valueStart;
        }
    }

    private static final class RequestResult {
        private final int httpCode;
        private final long timeTotalNanos;

        private RequestResult(int httpCode, long timeTotalNanos) {
            this.httpCode = httpCode;
            this.timeTotalNanos = timeTotalNanos;
        }
    }

    private static final class ResponseSummary {
        private final long responseBytes;
        private final Integer reportedTotal;
        private final Integer reportedPageSize;
        private final Long firstTimestamp;
        private final Long lastTimestamp;

        private ResponseSummary(long responseBytes, Integer reportedTotal, Integer reportedPageSize,
                                Long firstTimestamp, Long lastTimestamp) {
            this.responseBytes = responseBytes;
            this.reportedTotal = reportedTotal;
            this.reportedPageSize = reportedPageSize;
            this.firstTimestamp = firstTimestamp;
            this.lastTimestamp = lastTimestamp;
        }
    }

    public static final class SeedInfo {
        public final boolean seeded;
        public final long pointCount;

        private SeedInfo(boolean seeded, long pointCount) {
            this.seeded = seeded;
            this.pointCount = pointCount;
        }
    }

    public static final class BenchmarkSummary {
        public final int successfulRuns;
        public final Double averageTimeTotalSeconds;
        public final Double minTimeTotalSeconds;
        public final Double maxTimeTotalSeconds;

        private BenchmarkSummary(int successfulRuns, Double averageTimeTotalSeconds,
                                 Double minTimeTotalSeconds, Double maxTimeTotalSeconds) {
            this.successfulRuns = successfulRuns;
            this.averageTimeTotalSeconds = averageTimeTotalSeconds;
            this.minTimeTotalSeconds = minTimeTotalSeconds;
            this.maxTimeTotalSeconds = maxTimeTotalSeconds;
        }

        private static BenchmarkSummary fromRuns(List<BenchmarkRun> runs) {
            List<BenchmarkRun> successfulRuns = new ArrayList<>();
            for (BenchmarkRun run : runs) {
                if (run.httpCode == 200) {
                    successfulRuns.add(run);
                }
            }
            if (successfulRuns.isEmpty()) {
                return new BenchmarkSummary(0, null, null, null);
            }

            double total = 0.0d;
            double min = Double.MAX_VALUE;
            double max = Double.MIN_VALUE;
            for (BenchmarkRun run : successfulRuns) {
                total += run.timeTotalSeconds;
                min = Math.min(min, run.timeTotalSeconds);
                max = Math.max(max, run.timeTotalSeconds);
            }
            return new BenchmarkSummary(
                    successfulRuns.size(),
                    Math.round((total / successfulRuns.size()) * 1_000_000.0d) / 1_000_000.0d,
                    Math.round(min * 1_000_000.0d) / 1_000_000.0d,
                    Math.round(max * 1_000_000.0d) / 1_000_000.0d
            );
        }
    }

    public static final class BenchmarkRun {
        public final int run;
        public final int httpCode;
        public final double timeTotalSeconds;
        public final long responseBytesOnDisk;
        public final Integer reportedTotal;
        public final Integer reportedPageSize;
        public final Long firstTimestamp;
        public final Long lastTimestamp;
        public final String errorBody;
        public final String responseFile;

        private BenchmarkRun(int run, int httpCode, double timeTotalSeconds, long responseBytesOnDisk,
                             Integer reportedTotal, Integer reportedPageSize, Long firstTimestamp,
                             Long lastTimestamp, String errorBody, String responseFile) {
            this.run = run;
            this.httpCode = httpCode;
            this.timeTotalSeconds = timeTotalSeconds;
            this.responseBytesOnDisk = responseBytesOnDisk;
            this.reportedTotal = reportedTotal;
            this.reportedPageSize = reportedPageSize;
            this.firstTimestamp = firstTimestamp;
            this.lastTimestamp = lastTimestamp;
            this.errorBody = errorBody;
            this.responseFile = responseFile;
        }
    }

    public static final class BenchmarkReport {
        public final String benchmark;
        public final String generatedAt;
        public final String gitBranch;
        public final String gitCommit;
        public final String office;
        public final String locationId;
        public final String seriesId;
        public final String units;
        public final String startTimeUtc;
        public final String endTimeUtc;
        public final int pointCount;
        public final int pageSize;
        public final String requestUrl;
        public final SeedInfo seed;
        public final BenchmarkSummary summary;
        public final List<BenchmarkRun> runs;

        private BenchmarkReport(String benchmark, String generatedAt, String gitBranch, String gitCommit,
                                String office, String locationId, String seriesId, String units,
                                String startTimeUtc, String endTimeUtc, int pointCount, int pageSize,
                                String requestUrl, SeedInfo seed, BenchmarkSummary summary,
                                List<BenchmarkRun> runs) {
            this.benchmark = benchmark;
            this.generatedAt = generatedAt;
            this.gitBranch = gitBranch;
            this.gitCommit = gitCommit;
            this.office = office;
            this.locationId = locationId;
            this.seriesId = seriesId;
            this.units = units;
            this.startTimeUtc = startTimeUtc;
            this.endTimeUtc = endTimeUtc;
            this.pointCount = pointCount;
            this.pageSize = pageSize;
            this.requestUrl = requestUrl;
            this.seed = seed;
            this.summary = summary;
            this.runs = runs;
        }
    }
}
