package cwms.cda.data.dto.csv;

import cwms.cda.formatters.DateFormatResolver;
import cwms.cda.formatters.DateFormat;
import cwms.cda.formatters.csv.CsvConfiguration;
import cwms.cda.formatters.csv.CsvV1;
import org.apache.commons.io.IOUtils;
import org.junit.jupiter.api.Test;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Collections;
import java.util.List;

import static cwms.cda.helpers.DTOMatch.assertMatch;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestTimeSeriesCsvRow {

    @Test
    void testSingleRow_Default_Columns_NoMetadata() throws Exception {
        TimeSeriesCsvRow row = buildRow(Instant.parse("2021-06-21T21:06:00Z"), 1.0, "ft");
        TimeSeriesCsv container = new TimeSeriesCsv.Builder()
                .withRows(Collections.singletonList(row))
                .build();

        CsvV1 csv = new CsvV1();
        String actual = csv.format(container);
        assertNotNull(actual);
        String normalized = normalize(actual);
        assertTrue(normalized.contains("date-time,value (ft)"), "Header mismatch");
        assertTrue(normalized.contains("2021-06-21T21:06:00Z,1.0"), "Row mismatch");
    }

    @Test
    void testSingleRow_Default_Columns_WithMetadataComments() throws Exception {
        TimeSeriesCsvRow row = buildRow(Instant.parse("2021-06-21T21:06:00Z"), 1.0, "ft");
        TimeSeriesCsv container = new TimeSeriesCsv.Builder()
                .withTimeSeriesId("RYAN3.Stage.Inst.5Minutes.0.ZSTORE_TS_TEST")
                .withOfficeId("SPK")
                .withVersionDate("2025-07-22T14:00:00Z")
                .withRows(Collections.singletonList(row))
                .build();

        CsvV1 csv = new CsvV1();
        CsvConfiguration config = new CsvConfiguration.Builder()
                .withMetadataIncluded(true)
                .withOptionalColumnsIncluded(false)
                .withDateFormat(DateFormat.pattern(DateFormatResolver.ISO_INSTANT_PATTERN))
                .build();
        String actual = csv.format(container, config);
        assertNotNull(actual);
        String normalized = normalize(actual);
        assertTrue(normalized.contains("# time-series-id: RYAN3.Stage.Inst.5Minutes.0.ZSTORE_TS_TEST"), "Metadata mismatch");
        assertTrue(normalized.contains("# office-id: SPK"), "Metadata mismatch");
        assertTrue(normalized.contains("date-time,value (ft)"), "Header mismatch");
        assertTrue(normalized.contains("2021-06-21T21:06:00Z,1.0"), "Row mismatch");
    }

    @Test
    void testSingleRow_WithOptionalColumns() {
        TimeSeriesCsvRow row = new TimeSeriesCsvRow.Builder()
                .withDateTime(Instant.parse("2021-06-21T21:06:00Z"))
                .withValue(1.0)
                .withQualityCode(0)
                .withDataEntryDate(Instant.parse("2021-06-21T21:00:00Z"))
                .withUnits("ft")
                .build();
        TimeSeriesCsv container = new TimeSeriesCsv.Builder()
                .withRows(Collections.singletonList(row))
                .build();

        CsvV1 csv = new CsvV1();
        CsvConfiguration config = new CsvConfiguration.Builder()
                .withOptionalColumnsIncluded(true)
                .build();
        String actual = csv.format(container, config);
        assertNotNull(actual);
        String normalized = normalize(actual);
        assertTrue(normalized.contains("date-time,value (ft),data-entry-date,quality-code"), "Header mismatch: " + normalized);
        assertTrue(normalized.contains("2021-06-21T21:06:00Z,1.0,2021-06-21T21:00:00Z,0"), "Row mismatch: " + normalized);
    }

    @Test
    void testDefaultSerialization() throws Exception {
        String csv = readResource("cwms/cda/data/dto/time-series-default.csv");
        CsvV1 formatter = new CsvV1();
        TimeSeriesCsv container = formatter.parseContent(csv, TimeSeriesCsv.class);
        assertNotNull(container);
        List<TimeSeriesCsvRow> rows = container.getRows();
        assertEquals(4, rows.size());
        assertEquals("cfs", rows.get(0).getUnits());
        assertEquals(0.0, rows.get(0).getValue());
        assertEquals(Instant.parse("2021-06-21T00:00:00Z"), rows.get(0).getDateTime());

        //serialize back
        String serialized = formatter.format(container);
        assertNotNull(serialized);

        //parse back
        TimeSeriesCsv parsedContainer = formatter.parseContent(serialized, TimeSeriesCsv.class);
        assertNotNull(parsedContainer);
        assertEquals(rows.get(0).getUnits(), parsedContainer.getRows().get(0).getUnits());
        assertMatch(rows, parsedContainer.getRows());
    }

    @Test
    void testMetadataAsCommentsSerialization() throws Exception {
        String csv = readResource("cwms/cda/data/dto/time-series-metadata-comments.csv");
        CsvV1 formatter = new CsvV1();
        TimeSeriesCsv container = formatter.parseContent(csv, TimeSeriesCsv.class);
        assertNotNull(container);
        List<TimeSeriesCsvRow> rows = container.getRows();
        assertEquals(4, rows.size());
        assertEquals("cfs", rows.get(0).getUnits());
        assertEquals(0.0, rows.get(0).getValue());
        assertEquals("ALAT2.Flow-Out.Inst.1Hour.0.Rev-SWF-REGI", container.getTimeSeriesId());
        assertEquals("SWT", container.getOfficeId());
        assertEquals("aggregate", container.getVersionDate());

        //serialize back
        CsvConfiguration config = new CsvConfiguration.Builder()
                .withMetadataIncluded(true)
                .withOptionalColumnsIncluded(false)
                .withDateFormat(DateFormat.pattern(DateFormatResolver.ISO_INSTANT_PATTERN))
                .build();
        String serialized = formatter.format(container, config);
        assertNotNull(serialized);

        //parse back
        TimeSeriesCsv parsedContainer = formatter.parseContent(serialized, TimeSeriesCsv.class);
        assertNotNull(parsedContainer);
        assertEquals(container.getTimeSeriesId(), parsedContainer.getTimeSeriesId());
        assertEquals(container.getOfficeId(), parsedContainer.getOfficeId());
        assertEquals(container.getVersionDate(), parsedContainer.getVersionDate());
        assertMatch(rows, parsedContainer.getRows());
    }

    @Test
    void testOptionalsNoMetadataSerialization() throws Exception {
        String csv = readResource("cwms/cda/data/dto/time-series-optionals-no-metadata-comments.csv");
        CsvV1 formatter = new CsvV1();
        TimeSeriesCsv container = formatter.parseContent(csv, TimeSeriesCsv.class);
        assertNotNull(container);
        List<TimeSeriesCsvRow> rows = container.getRows();
        assertEquals(4, rows.size());
        assertEquals("cfs", rows.get(0).getUnits());
        assertEquals(0.0, rows.get(0).getValue());
        assertNotNull(rows.get(0).getDataEntryDate());
        assertEquals(5, rows.get(0).getQualityCode());

        //serialize back
        CsvConfiguration config = new CsvConfiguration.Builder()
                .withOptionalColumnsIncluded(true)
                .build();
        String serialized = formatter.format(container, config);
        assertNotNull(serialized);

        //parse back
        TimeSeriesCsv parsedContainer = formatter.parseContent(serialized, TimeSeriesCsv.class);
        assertNotNull(parsedContainer);
        assertMatch(rows, parsedContainer.getRows());
    }

    @Test
    void testOptionalsWithMetadataCommentsSerialization() throws Exception {
        String csv = readResource("cwms/cda/data/dto/time-series-optionals-with-metadata-comments.csv");
        CsvV1 formatter = new CsvV1();
        TimeSeriesCsv container = formatter.parseContent(csv, TimeSeriesCsv.class);
        assertNotNull(container);
        List<TimeSeriesCsvRow> rows = container.getRows();
        assertEquals(4, rows.size());
        assertEquals("cfs", rows.get(0).getUnits());
        assertEquals(0.0, rows.get(0).getValue());
        assertEquals("ALAT2.Flow-Out.Inst.1Hour.0.Rev-SWF-REGI", container.getTimeSeriesId());
        assertEquals("SWT", container.getOfficeId());
        assertEquals("aggregate", container.getVersionDate());
        assertNotNull(rows.get(0).getDataEntryDate());
        assertEquals(5, rows.get(0).getQualityCode());

        //serialize back
        CsvConfiguration config = new CsvConfiguration.Builder()
                .withMetadataIncluded(true)
                .withOptionalColumnsIncluded(true)
                .withDateFormat(DateFormat.pattern(DateFormatResolver.ISO_INSTANT_PATTERN))
                .build();
        String serialized = formatter.format(container, config);
        assertNotNull(serialized);

        //parse back
        TimeSeriesCsv parsedContainer = formatter.parseContent(serialized, TimeSeriesCsv.class);
        assertNotNull(parsedContainer);
        assertEquals(container.getTimeSeriesId(), parsedContainer.getTimeSeriesId());
        assertEquals(container.getOfficeId(), parsedContainer.getOfficeId());
        assertEquals(container.getVersionDate(), parsedContainer.getVersionDate());
        assertMatch(rows, parsedContainer.getRows());
    }

    @Test
    void testSingleRow_EpochMillis() throws Exception {
        TimeSeriesCsvRow row = buildRow(Instant.parse("2021-06-21T21:06:00Z"), 1.0, "ft");
        TimeSeriesCsv container = new TimeSeriesCsv.Builder()
                .withRows(Collections.singletonList(row))
                .build();

        CsvV1 csv = new CsvV1();
        cwms.cda.formatters.csv.CsvConfiguration config = new cwms.cda.formatters.csv.CsvConfiguration.Builder()
                .withDateFormat(DateFormat.epochMillis())
                .build();
        String actual = csv.format(container, config);
        assertNotNull(actual);
        String normalized = normalize(actual);
        assertTrue(normalized.contains("date-time,value (ft)"), "Header mismatch");
        // 2021-06-21T21:06:00Z is 1624309560000
        assertTrue(normalized.contains("1624309560000,1.0"), "Row mismatch: " + normalized);
    }

    @Test
    void testSingleRow_CustomPattern() throws Exception {
        TimeSeriesCsvRow row = buildRow(Instant.parse("2021-06-21T21:06:00Z"), 1.0, "ft");
        TimeSeriesCsv container = new TimeSeriesCsv.Builder()
                .withRows(Collections.singletonList(row))
                .build();

        CsvV1 csv = new CsvV1();
        CsvConfiguration config = new CsvConfiguration.Builder()
                .withDateFormat(DateFormat.pattern("yyyyMMddHHmm"))
                .build();
        String actual = csv.format(container, config);
        assertNotNull(actual);
        String normalized = normalize(actual);
        assertTrue(normalized.contains("date-time,value (ft)"), "Header mismatch");
        // 2021-06-21T21:06:00Z should be 202106212106
        assertTrue(normalized.contains("202106212106,1.0"), "Row mismatch: " + normalized);
    }

    private static String readResource(String path) throws Exception {
        InputStream stream = TestTimeSeriesCsvRow.class.getClassLoader().getResourceAsStream(path);
        assertNotNull(stream, "Missing test resource: " + path);
        String expected = IOUtils.toString(stream, StandardCharsets.UTF_8);
        return normalize(expected);
    }

    private static String normalize(String s) {
        return s.replaceAll("\r", "");
    }

    private static TimeSeriesCsvRow buildRow(Instant dateTime, Double value, String units) {
        return new TimeSeriesCsvRow.Builder()
                .withDateTime(dateTime)
                .withValue(value)
                .withQualityCode(0)
                .withUnits(units)
                .build();
    }
}
