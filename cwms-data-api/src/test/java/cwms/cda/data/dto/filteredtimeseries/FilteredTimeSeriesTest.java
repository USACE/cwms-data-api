package cwms.cda.data.dto.filteredtimeseries;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import cwms.cda.data.dao.FilteredTimeSeriesParameters;
import cwms.cda.data.dto.TimeSeries;
import cwms.cda.formatters.ContentType;
import cwms.cda.formatters.Formats;
import cwms.cda.formatters.json.JsonV2;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import org.apache.commons.io.IOUtils;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.Test;

class FilteredTimeSeriesTest {

    @Test
    void testConstructor() {
        TimeSeries ts = buildTimeSeries();
        FilteredTimeSeriesParameters params = buildFilterParams();

        FilteredTimeSeries filteredTs = new FilteredTimeSeries(ts, params);

        assertEquals(ts, filteredTs.getTimeSeries());
        assertEquals(params, filteredTs.getFilterParams());
    }

    @Test
    void testJsonSerialization() throws JsonProcessingException {
        TimeSeries ts = buildTimeSeries();
        FilteredTimeSeriesParameters params = buildFilterParams();
        FilteredTimeSeries filteredTs = new FilteredTimeSeries(ts, params);

        ObjectMapper om = buildObjectMapper();

        String tsBody = om.writeValueAsString(filteredTs);
        assertNotNull(tsBody);

        assertTrue(tsBody.contains("\"name\":\"Calhoun.Flow.Inst.~1Hour.0.cda-test\""));
        assertTrue(tsBody.contains("\"office-id\":\"SPK\""));
        assertTrue(tsBody.contains("\"units\":\"cfs\""));


        assertTrue(tsBody.contains("\"filter-parameters\""));
        assertTrue(tsBody.contains("\"ascending\":true"));
        assertTrue(tsBody.contains("\"min-value\":450.0"));
        assertTrue(tsBody.contains("\"max-value\":550.0"));
        assertTrue(tsBody.contains("\"filter-nulls\":true"));
    }

    @Test
    void testSerializerWithNulls() {
        TimeSeries ts = buildTimeSeriesWithNulls();
        FilteredTimeSeriesParameters params = buildFilterParams();
        FilteredTimeSeries filteredTs = new FilteredTimeSeries(ts, params);

        String tsBody = Formats.format(new ContentType(Formats.JSONV2), filteredTs);
        assertNotNull(tsBody);
    }

    @Test
    void testResourceFileExists() throws IOException {
        InputStream resource = this.getClass().getResourceAsStream(
                "/cwms/cda/data/dto/filteredtimeseries/filtered_timeseries_test.json");
        assertNotNull(resource);
        String tsData = IOUtils.toString(resource, StandardCharsets.UTF_8);
        assertNotNull(tsData);

        assertTrue(tsData.contains("name"));
        assertTrue(tsData.contains("Calhoun.Flow.Inst.~1Hour.0.cda-test"));
        assertTrue(tsData.contains("office-id"));
        assertTrue(tsData.contains("SPK"));
        assertTrue(tsData.contains("units"));
        assertTrue(tsData.contains("cfs"));

        assertTrue(tsData.contains("filter-parameters"));
        assertTrue(tsData.contains("ascending"));
        assertTrue(tsData.contains("min-value"));
        assertTrue(tsData.contains("max-value"));
        assertTrue(tsData.contains("filter-nulls"));
    }

    @NotNull
    private TimeSeries buildTimeSeries() {
        String tsId = "Calhoun.Flow.Inst.~1Hour.0.cda-test";

        ZonedDateTime start = ZonedDateTime.parse("2023-01-11T12:00:00Z");
        ZonedDateTime end = ZonedDateTime.parse("2023-01-11T13:00:00Z");
        ZonedDateTime versionDate = Instant.now().atZone(ZoneId.of("UTC"));
        TimeSeries ts = new TimeSeries(null, -1, 0, tsId, "SPK", start, end, "cfs", Duration.ZERO, null, versionDate, null);


        ts.addValue(Timestamp.from(Instant.ofEpochMilli(1673438400000L)), 500.0, 0);
        ts.addValue(Timestamp.from(Instant.ofEpochMilli(1673442000000L)), 600.0, 0);

        return ts;
    }

    private TimeSeries buildTimeSeriesWithNulls() {
        String tsId = "Calhoun.Flow.Inst.~1Hour.0.cda-test";

        ZonedDateTime start = ZonedDateTime.parse("2023-01-11T12:00:00Z");
        ZonedDateTime end = ZonedDateTime.parse("2023-01-11T13:00:00Z");
        ZonedDateTime versionDate = Instant.now().atZone(ZoneId.of("UTC"));
        TimeSeries ts = new TimeSeries(null, -1, 0, tsId, "SPK", start, end, "cfs", Duration.ZERO, null, versionDate, null);

        ts.addValue(Timestamp.from(Instant.ofEpochMilli(1673438400000L)), 500.0, 0);
        ts.addValue(Timestamp.from(Instant.ofEpochMilli(1673439400000L)), null, 5);
        ts.addValue(Timestamp.from(Instant.ofEpochMilli(1673442000000L)), 600.0, 0);

        return ts;
    }

    private FilteredTimeSeriesParameters buildFilterParams() {
        return new FilteredTimeSeriesParameters.Builder()
                .withAscending(true)
                .withMinValue(450.0)
                .withMaxValue(550.0)
                .withFilterNulls(true)
                .build();
    }

    @NotNull
    private ObjectMapper buildObjectMapper() {
        return JsonV2.buildObjectMapper();
    }
}
