package cwms.cda.data.dto.csv;

import cwms.cda.formatters.csv.CsvV1;
import org.apache.commons.io.IOUtils;
import org.junit.jupiter.api.Test;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.ZonedDateTime;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

public class TestTimeSeriesCsvRow {

    @Test
    void testSingleRowDefault_NoMetadata() throws Exception {
        TimeSeriesCsvRow row = buildRow(Instant.parse("2021-06-21T21:06:00Z"), 1.0);

        String expected = readResource("cwms/cda/data/dto/csv/time-series-row-default.csv");

        CsvV1 csv = new CsvV1();
        String actual = csv.format(row);
        assertNotNull(actual);
        assertEquals(expected, normalize(actual));
    }

    @Test
    void testListRowDefault_NoMetadata() throws Exception {
        TimeSeriesCsvRow row = buildRow(Instant.parse("2021-06-21T21:06:00Z"), 1.0);

        String expected = readResource("cwms/cda/data/dto/csv/time-series-row-default.csv");
        List<TimeSeriesCsvRow> rows = List.of(row);
        CsvV1 csv = new CsvV1();
        String actual = csv.format(rows);
        assertNotNull(actual);
        assertEquals(expected, normalize(actual));
    }

    @Test
    void testSingleRow_WithMetadataComments() throws Exception {
        TimeSeriesCsvRow row = buildRow(Instant.parse("2021-06-21T21:06:00Z"), 1.0);

        String expected = readResource("cwms/cda/data/dto/csv/time-series-row-with-metadata-comment.csv");

        CsvV1 csv = new CsvV1();
        String actual = csv.formatWithMetaDataIncludedAsComments(row);
        assertNotNull(actual);
        assertEquals(expected, normalize(actual));
    }

    @Test
    void testMultipleRows_WithMetadataColumns() throws Exception {
        TimeSeriesCsvRow row1 = buildRow(Instant.parse("2021-06-21T21:06:00Z"), 1.0);
        TimeSeriesCsvRow row2 = buildRow(Instant.parse("2021-06-22T21:06:00Z"), 2.0);
        List<TimeSeriesCsvRow> rows = Arrays.asList(row1, row2);

        String expected = readResource("cwms/cda/data/dto/csv/time-series-rows-with-metadata-columns.csv");

        CsvV1 csv = new CsvV1();
        String actual = csv.formatWithMetaDataIncludedAsColumns(rows);
        assertNotNull(actual);
        assertEquals(expected, normalize(actual));
    }

    @Test
    void testMultipleRows_WithMetadataComments() throws Exception {
        TimeSeriesCsvRow row1 = buildRow(Instant.parse("2021-06-21T21:06:00Z"), 1.0);
        TimeSeriesCsvRow row2 = buildRow(Instant.parse("2021-06-22T21:06:00Z"), 2.0);
        List<TimeSeriesCsvRow> rows = Arrays.asList(row1, row2);

        String expected = readResource("cwms/cda/data/dto/csv/time-series-rows-with-metadata-comment.csv");

        CsvV1 csv = new CsvV1();
        String actual = csv.formatWithMetaDataIncludedAsComments(rows);
        assertNotNull(actual);
        assertEquals(expected, normalize(actual));
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

    private static TimeSeriesCsvRow buildRow(Instant dateTime, Double value) {
        String tsId = "RYAN3.Stage.Inst.5Minutes.0.ZSTORE_TS_TEST";
        String office = "SPK";
        String units = "ft";
        int qualityCode = 0;
        ZonedDateTime versionDate = ZonedDateTime.parse("2025-07-22T14:00:00-00:00[UTC]");
        return new TimeSeriesCsvRow(tsId, office, dateTime, value, units, versionDate.toInstant(), qualityCode);
    }
}
