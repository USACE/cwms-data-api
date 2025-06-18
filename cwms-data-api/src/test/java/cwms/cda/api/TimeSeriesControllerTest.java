package cwms.cda.api;

import static org.junit.jupiter.api.Assertions.*;

import cwms.cda.data.dto.TimeSeries;
import cwms.cda.formatters.ContentType;
import cwms.cda.formatters.Formats;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.time.ZonedDateTime;
import java.util.List;
import java.util.TimeZone;

import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class TimeSeriesControllerTest extends ControllerTest {


    private void assertSimilar(TimeSeries expected, TimeSeries actual) {
        // Make sure ts we got back resembles the fakeTS our mock dao was supposed to return.
        assertEquals(expected.getOfficeId(), actual.getOfficeId(), "offices did not match");
        assertEquals(expected.getName(), actual.getName(), "names did not match");
        assertRecordsMatch(expected.getValues(), actual.getValues());
        assertTrue(expected.getBegin().isEqual(actual.getBegin()), "begin dates not equal");
        assertTrue(expected.getEnd().isEqual(actual.getEnd()), "end dates not equal");
    }

    private void assertRecordsMatch(List<TimeSeries.Record> expected, List<TimeSeries.Record> actual) {
        for (int i = 0; i < expected.size(); i++) {
            assertEquals(expected.get(i).getDateTime(), actual.get(i).getDateTime(), "Timestamps did not match");
            assertEquals(expected.get(i).getValue(), actual.get(i).getValue(), "Values did not match");
            assertEquals(expected.get(i).getQualityCode(), actual.get(i).getQualityCode(), "Quality codes did not match");
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {Formats.XMLV2, Formats.JSONV2})
    void testSerializeTimeSeries(String format) {
        String officeId = "LRL";
        String tsId = "RYAN3.Stage.Inst.5Minutes.0.ZSTORE_TS_TEST";
        TimeSeries fakeTs = buildTimeSeries(officeId, tsId);
        ContentType contentType = Formats.parseHeader(format, TimeSeries.class);
        String formatted = Formats.format(contentType, fakeTs);
        assertNotNull(formatted);
        assertFalse(formatted.contains("null"));
        TimeSeries ts2 = Formats.parseContent(contentType, formatted, TimeSeries.class);
        assertNotNull(ts2);
        assertSimilar(fakeTs, ts2);
    }

    @ParameterizedTest
    @ValueSource(strings = {Formats.XMLV2, Formats.JSONV2})
    void testSerializeTimeSeriesWithDataEntryDate(String format) {
        String officeId = "LRL";
        String tsId = "RYAN3.Stage.Inst.5Minutes.0.ZSTORE_TS_TEST";
        TimeSeries fakeTs = buildTimeSeriesWithEntryDate(officeId, tsId);
        assertEquals(4, fakeTs.getValueColumnsJSON().size());
        assertInstanceOf(TimeSeries.Record.class,
                fakeTs.getValues().get(0));
        ContentType contentType = Formats.parseHeader(format, TimeSeries.class);
        String formatted = Formats.format(contentType, fakeTs);
        assertNotNull(formatted);
        assertTrue(formatted.contains("data-entry-date"));
        TimeSeries ts2 = Formats.parseContent(contentType, formatted, TimeSeries.class);
        assertNotNull(ts2);
        assertSimilar(fakeTs, ts2);
    }


    @ParameterizedTest
    @ValueSource(strings = {Formats.XMLV2, Formats.JSONV2})
    void testDeserializeTimeSeries(String format) {
        String officeId = "LRL";
        String tsId = "RYAN3.Stage.Inst.5Minutes.0.ZSTORE_TS_TEST";
        TimeSeries fakeTs = buildTimeSeries(officeId, tsId);
        ContentType contentType = Formats.parseHeader(format, TimeSeries.class);
        String formatted = Formats.format(contentType, fakeTs);
        assertNotNull(formatted);
        TimeSeries ts2 = Formats.parseContent(contentType, formatted, TimeSeries.class);
        assertNotNull(ts2);
        assertSimilar(fakeTs, ts2);
    }

    @ParameterizedTest
    @ValueSource(strings = {Formats.XMLV2, Formats.JSONV2})
    void testDeserializeTimeSeriesWithEntryDate(String format) {
        String officeId = "LRL";
        String tsId = "RYAN3.Stage.Inst.5Minutes.0.ZSTORE_TS_TEST";
        TimeSeries fakeTs = buildTimeSeriesWithEntryDate(officeId, tsId);
        ContentType contentType = Formats.parseHeader(format, TimeSeries.class);
        String formatted = Formats.format(contentType, fakeTs);
        assertNotNull(formatted);
        TimeSeries ts2 = Formats.parseContent(contentType, formatted, TimeSeries.class);
        assertNotNull(ts2);
        assertSimilar(fakeTs, ts2);
    }

    @Test
    void testDeserializeTimeSeriesWithEntryDateFromFile() {
        InputStream inputStream = this.getClass()
                .getResourceAsStream("/cwms/cda/api/lrl/timeseries_with_data_entry_dates.json");
        ContentType contentType = Formats.parseHeader(Formats.JSONV2, TimeSeries.class);
        TimeSeries fakeTs = Formats.parseContent(contentType, inputStream, TimeSeries.class);
        String formatted = Formats.format(contentType, fakeTs);
        TimeSeries ts2 = Formats.parseContent(contentType, formatted, TimeSeries.class);
        assertNotNull(ts2);
        assertSimilar(fakeTs, ts2);
    }

    @Test
    void testXMLSerializeDeserializeTimeSeries()
    {
        String format = Formats.XMLV2;
        String officeId = "LRL";
        String tsId = "RYAN3.Stage.Inst.5Minutes.0.ZSTORE_TS_TEST";
        TimeSeries fakeTs = buildTimeSeriesWithEntryDate(officeId, tsId);
        ContentType contentType = Formats.parseHeader(format, TimeSeries.class);
        String formatted = Formats.format(contentType, fakeTs);
        assertNotNull(formatted);
        assertTrue(formatted.contains("quality-code"));
        assertTrue(formatted.contains("data-entry-date"));
        TimeSeries ts2 = Formats.parseContent(contentType, formatted, TimeSeries.class);
        assertNotNull(ts2);
        assertSimilar(fakeTs, ts2);
    }

    @Test
    void testDeserializeTimeSeriesXmlUTC() {
        TimeZone aDefault = TimeZone.getDefault();
        try {
            TimeZone.setDefault(TimeZone.getTimeZone("UTC"));

            String xml = loadResourceAsString("cwms/cda/api/timeseries_create.xml");
            assertNotNull(xml);
            InputStream inputStream = new ByteArrayInputStream(xml.getBytes(StandardCharsets.UTF_8));
            ContentType contentType = Formats.parseHeader(Formats.XMLV2, TimeSeries.class);
            TimeSeries ts = Formats.parseContent(contentType, inputStream, TimeSeries.class);

            assertNotNull(ts);

            TimeSeries fakeTs = buildTimeSeries("LRL", "RYAN3.Stage.Inst.5Minutes.0.ZSTORE_TS_TEST");
            assertSimilar(fakeTs, ts);
        } finally {
            TimeZone.setDefault(aDefault);
        }
    }

    @Test
    void testDeserializeTimeSeriesXml() {
            String xml = loadResourceAsString("cwms/cda/api/timeseries_create.xml");
            assertNotNull(xml);
			 // Should this be XMLv2?
        InputStream inputStream = new ByteArrayInputStream(xml.getBytes(StandardCharsets.UTF_8));
        ContentType contentType = Formats.parseHeader(Formats.XMLV2, TimeSeries.class);
        TimeSeries ts = Formats.parseContent(contentType, inputStream, TimeSeries.class);
        assertNotNull(ts);

        TimeSeries fakeTs = buildTimeSeries("LRL", "RYAN3.Stage.Inst.5Minutes.0.ZSTORE_TS_TEST");
        assertSimilar(fakeTs, ts);
    }

    @Test
    void testDeserializeTimeSeriesJSON() {
        String jsonV2 = loadResourceAsString("cwms/cda/api/timeseries_create.json");
        assertNotNull(jsonV2);
        InputStream inputStream = new ByteArrayInputStream(jsonV2.getBytes(StandardCharsets.UTF_8));
        ContentType contentType = Formats.parseHeader(Formats.JSONV2, TimeSeries.class);
        TimeSeries ts = Formats.parseContent(contentType, inputStream, TimeSeries.class);

        assertNotNull(ts);

        TimeSeries fakeTs = buildTimeSeries("LRL", "RYAN3.Stage.Inst.5Minutes.0.ZSTORE_TS_TEST");
        assertSimilar(fakeTs, ts);
    }

    @NotNull
    private TimeSeries buildTimeSeries(String officeId, String tsId) {
        ZonedDateTime start = ZonedDateTime.parse("2021-06-21T08:00:00-07:00[PST8PDT]");
        ZonedDateTime end = ZonedDateTime.parse("2021-06-21T09:00:00-07:00[PST8PDT]");

        long diff = end.toEpochSecond() - start.toEpochSecond();
        assertEquals(3600, diff); // just to make sure I've got the date parsing thing right.

        int minutes = 15;
        int count = 60/15 ; // do I need a +1?  ie should this be 12 or 13?
        // Also, should end be the last point or the next interval?

        TimeSeries ts = new TimeSeries(null,
                                      -1,
                                       0,
                                       tsId,
                                       officeId,
                                       start,
                                       end,
                                       "m",
                                       Duration.ofMinutes(minutes));

        ZonedDateTime next = start;
        for(int i = 0; i < count; i++) {
            Timestamp dateTime = Timestamp.from(next.toInstant());
            ts.addValue(dateTime, (double) i, 0);
            next = next.plusMinutes(minutes);
        }
        return ts;
    }

    @NotNull
    private TimeSeries buildTimeSeriesWithEntryDate(String officeId, String tsId) {
        ZonedDateTime start = ZonedDateTime.parse("2021-06-21T08:00:00-07:00[PST8PDT]");
        ZonedDateTime end = ZonedDateTime.parse("2021-06-21T09:00:00-07:00[PST8PDT]");

        long diff = end.toEpochSecond() - start.toEpochSecond();
        assertEquals(3600, diff); // just to make sure I've got the date parsing thing right.

        int minutes = 15;
        int count = 60/15 ; // do I need a +1?  ie should this be 12 or 13?
        // Also, should end be the last point or the next interval?

        TimeSeries ts = new TimeSeries(null,
                -1,
                0,
                tsId,
                officeId,
                start,
                end,
                "m",
                Duration.ofMinutes(minutes),
                null,
                null,
                null,
                null,
                null);

        ZonedDateTime next = start;
        for(int i = 0; i < count; i++) {
            Timestamp dateTime = Timestamp.from(next.toInstant());
            ts.addValue(dateTime, (double) i, 0, Timestamp.from(Instant.now()));
            next = next.plusMinutes(minutes);
        }
        return ts;
    }

    @Test
    void testGetIds(){
        String input = "a.b.c.e.f,2a.2b.2c.2d,3a.3b.3c";
        List<String> tsIds = TimeSeriesRecentController.getTsIds(input);
        assertNotNull(tsIds);
        assertEquals(3, tsIds.size());

        assertEquals("a.b.c.e.f", tsIds.get(0));
        assertEquals("2a.2b.2c.2d", tsIds.get(1));
        assertEquals("3a.3b.3c", tsIds.get(2));

        // input can have double quotes too
        input = "\"a.b.c.e.f\",2a.2b.2c.2d,\"3a.3b.3c\"";
        tsIds = TimeSeriesRecentController.getTsIds(input);
        assertNotNull(tsIds);
        assertEquals(3, tsIds.size());
        // but you will get them back
        assertEquals("\"a.b.c.e.f\"", tsIds.get(0));
        assertEquals("2a.2b.2c.2d", tsIds.get(1));
        assertEquals("\"3a.3b.3c\"", tsIds.get(2));

        // input can have brackets too
        input = "[a.b.c.e.f,2a.2b.2c.2d,3a.3b.3c]";
        tsIds = TimeSeriesRecentController.getTsIds(input);
        assertNotNull(tsIds);
        assertEquals(3, tsIds.size());

        assertEquals("a.b.c.e.f", tsIds.get(0));
        assertEquals("2a.2b.2c.2d", tsIds.get(1));
        assertEquals("3a.3b.3c", tsIds.get(2));

    }


}