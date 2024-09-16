package cwms.cda.data.dto;

import cwms.cda.data.dto.measurement.UsgsMeasurement;
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

final class UsgsMeasurementTest {

    @Test
    void createUsgsMeasurement_allFieldsProvided_success() {
        UsgsMeasurement item = new UsgsMeasurement.Builder()
                .withRemarks("Remarks")
                .withCurrentRating("Rating")
                .withControlCondition("Condition")
                .withFlowAdjustment("Adjustment")
                .withShiftUsed(0.1)
                .withPercentDifference(5.0)
                .withDeltaHeight(0.05)
                .withDeltaTime(10.0)
                .withAirTemp(20.0)
                .withWaterTemp(15.0)
                .build();

        assertAll(
                () -> assertEquals("Remarks", item.getRemarks(), "Remarks"),
                () -> assertEquals("Rating", item.getCurrentRating(), "Current Rating"),
                () -> assertEquals("Condition", item.getControlCondition(), "Control Condition"),
                () -> assertEquals("Adjustment", item.getFlowAdjustment(), "Flow Adjustment"),
                () -> assertEquals(0.1, item.getShiftUsed(), "Shift Used"),
                () -> assertEquals(5.0, item.getPercentDifference(), "Percent Difference"),
                () -> assertEquals(0.05, item.getDeltaHeight(), "Delta Height"),
                () -> assertEquals(10.0, item.getDeltaTime(), "Delta Time"),
                () -> assertEquals(20.0, item.getAirTemp(), "Air Temperature"),
                () -> assertEquals(15.0, item.getWaterTemp(), "Water Temperature")
        );
    }

    @Test
    void createUsgsMeasurement_serialize_roundtrip() {
        UsgsMeasurement measurement = new UsgsMeasurement.Builder()
                .withRemarks("Remarks")
                .withCurrentRating("Rating")
                .withControlCondition("Condition")
                .withFlowAdjustment("Adjustment")
                .withShiftUsed(0.1)
                .withPercentDifference(5.0)
                .withDeltaHeight(0.05)
                .withDeltaTime(10.0)
                .withAirTemp(20.0)
                .withWaterTemp(15.0)
                .build();

        ContentType contentType = new ContentType(Formats.JSON);
        String json = Formats.format(contentType, measurement);
        UsgsMeasurement deserialized = Formats.parseContent(contentType, json, UsgsMeasurement.class);

        DTOMatch.assertMatch(measurement, deserialized);
    }

    @Test
    void createUsgsMeasurement_deserialize() throws Exception {
        InputStream resource = this.getClass().getResourceAsStream("/cwms/cda/data/dto/usgs_measurement.json");
        assertNotNull(resource);
        String json = IOUtils.toString(resource, StandardCharsets.UTF_8);
        ContentType contentType = new ContentType(Formats.JSON);
        UsgsMeasurement deserialized = Formats.parseContent(contentType, json, UsgsMeasurement.class);

        UsgsMeasurement expectedMeasurement = new UsgsMeasurement.Builder()
                .withRemarks("Remarks")
                .withCurrentRating("Rating")
                .withControlCondition("Condition")
                .withFlowAdjustment("Adjustment")
                .withShiftUsed(0.1)
                .withPercentDifference(5.0)
                .withDeltaHeight(0.05)
                .withDeltaTime(10.0)
                .withAirTemp(20.0)
                .withWaterTemp(15.0)
                .build();

        DTOMatch.assertMatch(expectedMeasurement, deserialized);
    }
}
