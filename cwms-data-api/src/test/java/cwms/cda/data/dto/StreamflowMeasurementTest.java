package cwms.cda.data.dto;

import cwms.cda.data.dto.measurement.StreamflowMeasurement;
import cwms.cda.formatters.ContentType;
import cwms.cda.formatters.Formats;
import cwms.cda.helpers.DTOMatch;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import org.apache.commons.io.IOUtils;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import org.junit.jupiter.api.Test;

final class StreamflowMeasurementTest {

    @Test
    void createStreamflowMeasurement_allFieldsProvided_success() {
        StreamflowMeasurement item = new StreamflowMeasurement.Builder()
                .withGageHeight(5.5)
                .withFlow(250.0)
                .withQuality("Good")
                .build();

        assertAll(
                () -> assertEquals(5.5, item.getGageHeight(), "Gage Height"),
                () -> assertEquals(250.0, item.getFlow(), "Flow"),
                () -> assertEquals("Good", item.getQuality(), "Quality")
        );
    }

    @Test
    void createStreamflowMeasurement_serialize_roundtrip() {
        StreamflowMeasurement measurement = new StreamflowMeasurement.Builder()
                .withGageHeight(5.5)
                .withFlow(250.0)
                .withQuality("Good")
                .build();

        ContentType contentType = new ContentType(Formats.JSON);
        String json = Formats.format(contentType, measurement);
        StreamflowMeasurement deserialized = Formats.parseContent(contentType, json, StreamflowMeasurement.class);

        DTOMatch.assertMatch(measurement, deserialized);
    }

    @Test
    void createStreamflowMeasurement_deserialize() throws Exception {
        InputStream resource = this.getClass().getResourceAsStream("/cwms/cda/data/dto/streamflow_measurement.json");
        assertNotNull(resource);
        String json = IOUtils.toString(resource, StandardCharsets.UTF_8);
        ContentType contentType = new ContentType(Formats.JSON);
        StreamflowMeasurement deserialized = Formats.parseContent(contentType, json, StreamflowMeasurement.class);

        StreamflowMeasurement expectedMeasurement = new StreamflowMeasurement.Builder()
                .withGageHeight(5.5)
                .withFlow(250.0)
                .withQuality("Good")
                .build();

        DTOMatch.assertMatch(expectedMeasurement, deserialized);
    }
}
