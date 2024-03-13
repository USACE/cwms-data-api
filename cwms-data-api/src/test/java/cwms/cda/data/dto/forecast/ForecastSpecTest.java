package cwms.cda.data.dto.forecast;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import cwms.cda.data.dto.TimeSeriesIdentifierDescriptor;
import cwms.cda.data.dto.TimeSeriesIdentifierDescriptors;
import cwms.cda.formatters.json.JsonV2;
import org.apache.commons.io.IOUtils;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;

import static org.junit.jupiter.api.Assertions.*;

public class ForecastSpecTest {

  @Test
  void testRoundTripJson() throws JsonProcessingException
  {
    ForecastSpec s1 = buildForecastSpec();

    ObjectMapper om = buildObjectMapper();

    String jsonString = om.writeValueAsString(s1);
    assertNotNull(jsonString);

    ForecastSpec s2 = om.readValue(jsonString, ForecastSpec.class);
    assertNotNull(s2);

    assertForecastSpecEquals(s1, s2);
  }

  @Test
  void testRoundTripEmptyJson() throws JsonProcessingException
  {
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
    ArrayList<TimeSeriesIdentifierDescriptor> tsids = new ArrayList<>();
    tsids.add(new TimeSeriesIdentifierDescriptor.Builder().withTimeSeriesId("tsid1").build());
    tsids.add(new TimeSeriesIdentifierDescriptor.Builder().withTimeSeriesId("tsid2").build());
    tsids.add(new TimeSeriesIdentifierDescriptor.Builder().withTimeSeriesId("tsid3").build());

    return new ForecastSpec("spec", "office", "location", "sourceEntity",
            "designator", "description", tsids);
  }

  @NotNull
  private ForecastSpec buildEmptyForecastSpec() {
    return new ForecastSpec(null, null, null, null, null, null, null);
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
