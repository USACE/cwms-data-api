package cwms.cda.data.dto;

import cwms.cda.helpers.DTOMatch;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import cwms.cda.formatters.json.JsonV2;

import java.time.ZonedDateTime;
import org.junit.jupiter.api.Test;

class TimeSeriesExtentsTest {
    @Test
    void test_serialize_json() throws JsonProcessingException {
        TimeSeriesExtents extents = buildTimeSeriesExtents();

        ObjectMapper om = JsonV2.buildObjectMapper();

        String result = om.writeValueAsString(extents);
        assertNotNull(result);

        assertTrue(result.contains("last-update"), "should contain kebab-case last-update");
    }

    @Test
    void test_roundtrip_serialization() throws JsonProcessingException {
        TimeSeriesExtents extents = buildTimeSeriesExtents();

        ObjectMapper om = JsonV2.buildObjectMapper();

        String result = om.writeValueAsString(extents);
        assertNotNull(result);

        TimeSeriesExtents deserialized = om.readValue(result, TimeSeriesExtents.class);
        DTOMatch.assertMatch(extents, deserialized);
    }


    private TimeSeriesExtents buildTimeSeriesExtents() {
        ZonedDateTime version = ZonedDateTime.parse("2019-01-01T00:00:00Z");
        ZonedDateTime earliest = ZonedDateTime.parse("2020-01-01T00:00:00Z");
        ZonedDateTime latest = ZonedDateTime.parse("2021-01-01T00:00:00Z");
        ZonedDateTime updated = ZonedDateTime.parse("2022-01-01T00:00:00Z");

        return new TimeSeriesExtents.Builder()
                .withEarliestTime(earliest)
                .withEarliestTime(earliest)
                .withLatestTime(latest)
                .withVersionTime(version)
                .withLastUpdate(updated)
                .build();
    }
}
