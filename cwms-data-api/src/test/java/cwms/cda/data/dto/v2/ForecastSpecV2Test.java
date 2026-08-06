package cwms.cda.data.dto.v2;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import cwms.cda.api.errors.FieldException;
import cwms.cda.formatters.json.JsonV1;
import cwms.cda.helpers.DTOMatch;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import cwms.cda.data.dto.CwmsId;
import cwms.cda.formatters.ContentType;
import cwms.cda.formatters.Formats;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.io.IOUtils;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.Test;

public class ForecastSpecV2Test {

    @Test
    void testRoundTripJson() throws JsonProcessingException {
        ForecastSpecV2 s1 = buildForecastSpecV2();

        ObjectMapper om = buildObjectMapper();

        String jsonString = om.writeValueAsString(s1);
        assertNotNull(jsonString);

        ForecastSpecV2 s2 = om.readValue(jsonString, ForecastSpecV2.class);
        assertNotNull(s2);

        DTOMatch.assertMatch(s1, s2);
    }

    @Test
    void testFormatsSerialization() {
        ForecastSpecV2 s1 = buildForecastSpecV2();
        ContentType contentType = Formats.parseHeader(Formats.JSONV1, ForecastSpecV2.class);
        String jsonStr = Formats.format(contentType, s1);
        assertNotNull(jsonStr);
    }

    @Test
    void testMissingRequired()  {
        assertThrows(FieldException.class, () -> new ForecastSpecV2.Builder().build().validate());
    }

    @Test
    void testJsonFile() throws IOException {
        String json;
        try (InputStream stream = getClass().getResourceAsStream("forecast_spec_v2_test.json")) {
            assertNotNull(stream);
            json = IOUtils.toString(stream, StandardCharsets.UTF_8);
        }

        ObjectMapper om = buildObjectMapper();
        ForecastSpecV2 fi = om.readValue(json, ForecastSpecV2.class);

        assertNotNull(fi);
        DTOMatch.assertMatch(fi, buildForecastSpecV2());
    }

    @Test
    void testLocationsPreserved() {
        ForecastSpecV2 s1 = buildForecastSpecV2();

        List<ForecastLocation> locations = s1.getLocationIds();
        assertNotNull(locations);
        DTOMatch.assertMatch(new ForecastLocation.Builder()
                .withLocationId("location1")
                .withIsPrimary(true)
                .build(), locations.get(0));
        DTOMatch.assertMatch(new ForecastLocation.Builder()
                .withLocationId("location2")
                .withSortOrder(2)
                .withIsPrimary(false)
                .build(), locations.get(1));
    }

    @NotNull
    private ForecastSpecV2 buildForecastSpecV2() {
        List<String> tsids = new ArrayList<>();
        tsids.add("tsid1");
        tsids.add("tsid2");
        tsids.add("tsid3");

        List<ForecastLocation> locations = new ArrayList<>();
        locations.add(new ForecastLocation.Builder()
                .withLocationId("location1")
                .withIsPrimary(true)
                .build());
        locations.add(new ForecastLocation.Builder()
                .withLocationId("location2")
                .withSortOrder(2)
                .withIsPrimary(false)
                .build());

        return new ForecastSpecV2.Builder()
                .withSpecId(new CwmsId.Builder()
                        .withName("spec")
                        .withOfficeId("office")
                        .build())
                .withLocationIds(locations)
                .withSourceEntityId("sourceEntity").withDesignator("designator")
                .withDescription("description")
                .withTimeSeriesIds(tsids)
                .build();
    }

    @NotNull
    public static ObjectMapper buildObjectMapper() {
        return JsonV1.buildObjectMapper();
    }
}
