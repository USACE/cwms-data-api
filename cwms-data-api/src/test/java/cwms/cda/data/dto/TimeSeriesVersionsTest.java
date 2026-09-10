package cwms.cda.data.dto;

import static org.junit.jupiter.api.Assertions.assertNotNull;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import cwms.cda.formatters.json.JsonV2;
import cwms.cda.helpers.DTOMatch;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.time.ZonedDateTime;
import org.junit.jupiter.api.Test;

class TimeSeriesVersionsTest {

    @Test
    void test_roundtrip_serialization() throws JsonProcessingException {
        TimeSeriesVersions versions = buildTimeSeriesVersions();

        ObjectMapper om = JsonV2.buildObjectMapper();

        String result = om.writeValueAsString(versions);
        assertNotNull(result);

        TimeSeriesVersions deserialized = om.readValue(result, TimeSeriesVersions.class);
        DTOMatch.assertMatch(versions, deserialized);
    }

    @Test
    void test_roundtrip_from_example_json_fixture() throws IOException {
        ObjectMapper om = JsonV2.buildObjectMapper();

        String inputJson;
        try (InputStream is = getClass().getResourceAsStream("/cwms/cda/data/dto/time_series_versions.json")) {
            assertNotNull(is);
            inputJson = new String(is.readAllBytes(), StandardCharsets.UTF_8);
        }

        TimeSeriesVersions fromFixture = om.readValue(inputJson, TimeSeriesVersions.class);
        assertNotNull(fromFixture);

        String serialized = om.writeValueAsString(fromFixture);
        assertNotNull(serialized);

        TimeSeriesVersions roundTripped = om.readValue(serialized, TimeSeriesVersions.class);
        DTOMatch.assertMatch(fromFixture, roundTripped);
    }

    private TimeSeriesVersions buildTimeSeriesVersions() {
        ZonedDateTime version1 = ZonedDateTime.parse("2019-01-01T00:00:00Z");
        ZonedDateTime earliest1 = ZonedDateTime.parse("2020-01-01T00:00:00Z");
        ZonedDateTime latest1 = ZonedDateTime.parse("2021-01-01T00:00:00Z");
        ZonedDateTime updated1 = ZonedDateTime.parse("2022-01-01T00:00:00Z");

        TimeSeriesExtents extents1 = new TimeSeriesExtents.Builder()
                .withEarliestTime(earliest1)
                .withLatestTime(latest1)
                .withVersionTime(version1)
                .withLastUpdate(updated1)
                .build();

        ZonedDateTime version2 = ZonedDateTime.parse("2019-02-01T00:00:00Z");
        ZonedDateTime earliest2 = ZonedDateTime.parse("2020-02-01T00:00:00Z");
        ZonedDateTime latest2 = ZonedDateTime.parse("2021-02-01T00:00:00Z");
        ZonedDateTime updated2 = ZonedDateTime.parse("2022-02-01T00:00:00Z");

        TimeSeriesExtents extents2 = new TimeSeriesExtents.Builder()
                .withEarliestTime(earliest2)
                .withLatestTime(latest2)
                .withVersionTime(version2)
                .withLastUpdate(updated2)
                .build();

        return new TimeSeriesVersions.Builder()
                .withTsId(new CwmsId.Builder()
                        .withName("TestTS")
                        .withOfficeId("SWT")
                        .build())
                .addVersion(extents1)
                .addVersion(extents2)
                .withPage("page")
                .withPageSize(10)
                .withTotal(2)
                .build();
    }
}
