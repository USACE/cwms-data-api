package cwms.cda.data.dto.forecast;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import cwms.cda.formatters.ContentType;
import cwms.cda.formatters.Formats;
import cwms.cda.formatters.json.JsonV2;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.commons.io.IOUtils;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.Test;

public class ForecastSpecTest {

    @Test
    void testRoundTripJson() throws JsonProcessingException {
        ForecastSpec s1 = buildForecastSpec();

        ObjectMapper om = buildObjectMapper();

        String jsonString = om.writeValueAsString(s1);
        assertNotNull(jsonString);

        ForecastSpec s2 = om.readValue(jsonString, ForecastSpec.class);
        assertNotNull(s2);

        assertForecastSpecEquals(s1, s2);
    }

    @Test
    void testFormatsSerialization() {
        ForecastSpec s1 = buildForecastSpec();
        ContentType contentType = Formats.parseHeader(Formats.JSONV2);
        String jsonStr = Formats.format(contentType, s1);
        assertNotNull(jsonStr);
    }

    @Test
    void testRoundTripEmptyJson() throws JsonProcessingException {
        ForecastSpec s1 = buildEmptyForecastSpec();

        ObjectMapper om = buildObjectMapper();

        String jsonString = om.writeValueAsString(s1);
        assertNotNull(jsonString);

        ForecastSpec s2 = om.readValue(jsonString, ForecastSpec.class);
        assertNotNull(s2);

        assertForecastSpecEquals(s1, s2);
    }

    @Test
    void testJsonFile() throws IOException {
        String json;
        try (InputStream stream = getClass().getResourceAsStream("forecast_spec_test.json")) {
            assertNotNull(stream);
            json = IOUtils.toString(stream, StandardCharsets.UTF_8);
        }

        ObjectMapper om = buildObjectMapper();
        ForecastSpec fi = om.readValue(json, ForecastSpec.class);

        assertNotNull(fi);
        assertForecastSpecEquals(fi, buildForecastSpec());
    }

    @NotNull
    private ForecastSpec buildForecastSpec() {
        List<String> tsids = new ArrayList<>();
        tsids.add("tsid1");
        tsids.add("tsid2");
        tsids.add("tsid3");

        return new ForecastSpec.Builder()
                .withSpecId("spec")
                .withOfficeId("office")
                .withLocationId("location")
                .withSourceEntityId("sourceEntity").withDesignator("designator")
                .withDescription("description")
                .withTimeSeriesIds(tsids)
                .build();
    }

    @NotNull
    private ForecastSpec buildEmptyForecastSpec() {
        return new ForecastSpec.Builder().build();
    }

    @NotNull
    public static ObjectMapper buildObjectMapper() {
        return JsonV2.buildObjectMapper();
    }

    void assertForecastSpecEquals(ForecastSpec s1, ForecastSpec s2) throws JsonProcessingException {
        ObjectMapper om = buildObjectMapper();
        assertEquals(om.writeValueAsString(s1), om.writeValueAsString(s2));
    }

}
