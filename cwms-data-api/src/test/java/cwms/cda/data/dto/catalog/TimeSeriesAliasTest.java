package cwms.cda.data.dto.catalog;

import com.fasterxml.jackson.core.JsonProcessingException;
import cwms.cda.formatters.json.JsonV2;
import cwms.cda.formatters.xml.XMLv1;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import org.apache.commons.io.IOUtils;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import org.junit.jupiter.api.Test;

final class TimeSeriesAliasTest {

    @Test
    void testJsonDeserialization() throws IOException {
        InputStream resource = this.getClass().getResourceAsStream("/cwms/cda/data/dto/time-series-alias.json");
        assertNotNull(resource);
        String json = IOUtils.toString(resource, StandardCharsets.UTF_8);
        TimeSeriesAlias deserialized = JsonV2.buildObjectMapper().readValue(json, TimeSeriesAlias.class);
        assertEquals(deserialized.getName(), "Test Category-LessThan3");
        assertEquals(deserialized.getValue(), "test alias 1");
    }

    @Test
    void testJsonSerializationRoundTrip() throws JsonProcessingException {
        TimeSeriesAlias alias = new TimeSeriesAlias.Builder()
                .withName("Test Category-LessThan3")
                .withValue("test alias 1")
                .build();

        String json = JsonV2.buildObjectMapper().writeValueAsString(alias);

        assertNotNull(json);

        TimeSeriesAlias returned = JsonV2.buildObjectMapper().readValue(json, TimeSeriesAlias.class);

        assertEquals(alias, returned);
    }

    @Test
    void testXmlSerializationRoundTip() throws JsonProcessingException {
        TimeSeriesAlias alias = new TimeSeriesAlias.Builder()
                .withName("Test Category-LessThan3")
                .withValue("test alias 1")
                .build();

        String xml = XMLv1.buildObjectMapper().writeValueAsString(alias);

        assertNotNull(xml);

        TimeSeriesAlias returned = XMLv1.buildObjectMapper().readValue(xml, TimeSeriesAlias.class);

        assertEquals(alias, returned);
    }
}
