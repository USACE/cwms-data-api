package cwms.cda.data.dto.forecast;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import cwms.cda.data.dto.TimeSeriesIdentifierDescriptor;
import cwms.cda.formatters.ContentType;
import cwms.cda.formatters.Formats;
import cwms.cda.formatters.json.JsonV2;
import java.util.ArrayList;
import java.util.Collections;
import org.apache.commons.io.IOUtils;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.HashMap;

import static org.junit.jupiter.api.Assertions.*;

public class ForecastInstanceTest {

    public static final String OFFICE = "SPK";
    public static final String LOCATION = "BIG_BEND";

    @Test
    void testRoundTripJson() throws JsonProcessingException {
        ForecastInstance i1 = buildForecastInstance();

        ObjectMapper om = buildObjectMapper();

        String jsonString = om.writeValueAsString(i1);
        assertNotNull(jsonString);

        ForecastInstance i2 = om.readValue(jsonString, ForecastInstance.class);
        assertNotNull(i2);

        assertForecastInstanceEquals(i1, i2);
    }

    @Test
    void testFormatsSerialization() {
        ForecastInstance i1 = buildForecastInstance();
        ContentType contentType = Formats.parseHeader(Formats.JSONV2);
        String jsonStr = Formats.format(contentType, i1);
        assertNotNull(jsonStr);
    }

    @Test
    void testRoundTripEmptyJson() throws JsonProcessingException {
        ForecastInstance i1 = buildEmptyForecastInstance();

        ObjectMapper om = buildObjectMapper();

        String jsonString = om.writeValueAsString(i1);
        assertNotNull(jsonString);

        ForecastInstance i2 = om.readValue(jsonString, ForecastInstance.class);
        assertNotNull(i2);

        assertForecastInstanceEquals(i1, i2);
    }

    @Test
    void testJsonFile() throws IOException {
        String json;
        try (InputStream stream = getClass().getResourceAsStream("forecast_instance_test.json")) {
            assertNotNull(stream);
            json = IOUtils.toString(stream, StandardCharsets.UTF_8);
        }

        ObjectMapper om = buildObjectMapper();
        ForecastInstance fi = om.readValue(json, ForecastInstance.class);

        assertNotNull(fi);
        assertForecastInstanceEquals(fi, buildForecastInstance());
    }

    @NotNull
    private ForecastInstance buildForecastInstance() {
        Instant dateTime = Instant.parse("2021-06-21T14:00:10Z");
        Instant issueDateTime = Instant.parse("2022-05-22T12:03:40Z");
        Instant firstDateTime = Instant.parse("2023-08-22T11:02:30Z");
        Instant lastDateTime = Instant.parse("2024-09-22T15:01:00Z");

        HashMap<String, String> metadata = new HashMap<>();
        metadata.put("key1", "value1");
        metadata.put("key2", "value2");
        metadata.put("key3", "value3");

        ForecastSpec spec = buildForecastSpec();

        return new ForecastInstance.Builder()
                .withSpec(spec)
                .withDateTime(dateTime)
                .withIssueDateTime(issueDateTime)
                .withFirstDateTime(firstDateTime)
                .withLastDateTime(lastDateTime)
                .withMaxAge(5)
                .withNotes("test notes")
                .withMetadata(metadata)
                .withFilename("testFilename.txt")
                .withFileDescription("test file description")
                .withFileData("test file content".getBytes(StandardCharsets.UTF_8))
                .withFileDataUrl(null)
                .build()
                ;
    }

    @NotNull
    private ForecastSpec buildForecastSpec() {
        return new ForecastSpec.Builder()
                .withSpecId("spec")
                .withOfficeId("office")
                .withLocationIds(Collections.singleton("location"))
                .withDesignator("designator")
                .build()
                ;
    }

    @NotNull
    private ForecastInstance buildEmptyForecastInstance() {
        return new ForecastInstance.Builder().build();
    }

    @NotNull
    public static ObjectMapper buildObjectMapper() {
        return JsonV2.buildObjectMapper();
    }

    void assertForecastInstanceEquals(ForecastInstance i1, ForecastInstance i2) throws JsonProcessingException {
        ObjectMapper om = buildObjectMapper();
        assertEquals(om.writeValueAsString(i1), om.writeValueAsString(i2));
    }

}
