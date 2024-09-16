package cwms.cda.data.dto;

import cwms.cda.api.errors.RequiredFieldException;
import cwms.cda.data.dto.measurement.Measurement;
import cwms.cda.data.dto.measurement.StreamflowMeasurement;
import cwms.cda.data.dto.measurement.SupplementalStreamflowMeasurement;
import cwms.cda.data.dto.measurement.UsgsMeasurement;
import cwms.cda.formatters.ContentType;
import cwms.cda.formatters.Formats;
import cwms.cda.helpers.DTOMatch;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import org.apache.commons.io.IOUtils;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import org.junit.jupiter.api.Test;

final class MeasurementTest {
    @Test
    void createMeasurement_allFieldsProvided_success() {
        CwmsId cwmsId = new CwmsId.Builder()
                .withName("Location123")
                .withOfficeId("SPK")
                .build();

        StreamflowMeasurement sfm = new StreamflowMeasurement.Builder()
                .withGageHeight(5.5)
                .withFlow(250.0)
                .withQuality("Good")
                .build();

        SupplementalStreamflowMeasurement supplementalStreamflowMeas = new SupplementalStreamflowMeasurement.Builder()
                .withChannelFlow(300.0)
                .withOverbankFlow(50.0)
                .withOverbankMaxDepth(5.0)
                .withChannelMaxDepth(10.0)
                .withAvgVelocity(1.5)
                .withSurfaceVelocity(2.0)
                .withMaxVelocity(3.0)
                .withEffectiveFlowArea(200.0)
                .withCrossSectionalArea(250.0)
                .withMeanGage(20.0)
                .withTopWidth(30.0)
                .withMainChannelArea(150.0)
                .withOverbankArea(80.0)
                .build();

        UsgsMeasurement usgsMeas = new UsgsMeasurement.Builder()
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

        Measurement item = new Measurement.Builder()
                .withId(cwmsId)
                .withHeightUnit("ft")
                .withFlowUnit("cfs")
                .withTempUnit("F")
                .withVelocityUnit("ft/s")
                .withAreaUnit("sq ft")
                .withUsed(true)
                .withAgency("USGS")
                .withParty("Survey Party")
                .withWmComments("Measurement made during normal flow conditions.")
                .withInstant(Instant.parse("2024-09-16T00:00:00Z"))
                .withNumber("123456")
                .withStreamflowMeasurement(sfm)
                .withSupplementalStreamflowMeasurement(supplementalStreamflowMeas)
                .withUsgsMeasurement(usgsMeas)
                .build();

        assertAll(() -> assertEquals("ft", item.getHeightUnit(), "Height unit"),
                () -> assertEquals("cfs", item.getFlowUnit(), "Flow unit"),
                () -> assertEquals("F", item.getTempUnit(), "Temperature unit"),
                () -> assertEquals("ft/s", item.getVelocityUnit(), "Velocity unit"),
                () -> assertEquals("sq ft", item.getAreaUnit(), "Area unit"),
                () -> assertTrue(item.isUsed(), "Used flag"),
                () -> assertEquals("USGS", item.getAgency(), "Agency"),
                () -> assertEquals("Survey Party", item.getParty(), "Party"),
                () -> assertEquals("Measurement made during normal flow conditions.", item.getWmComments(), "Comments"),
                () -> assertNotNull(item.getInstant(), "Instant"),
                () -> assertEquals("123456", item.getNumber(), "Measurement number"),
                () -> DTOMatch.assertMatch(cwmsId, item.getId()),
                () -> DTOMatch.assertMatch(sfm, item.getStreamflowMeasurement()),
                () -> DTOMatch.assertMatch(supplementalStreamflowMeas, item.getSupplementalStreamflowMeasurement()),
                () -> DTOMatch.assertMatch(usgsMeas, item.getUsgsMeasurement())
        );
    }

    @Test
    void createMeasurement_missingField_throwsFieldException() {
        assertThrows(RequiredFieldException.class, () -> {
            Measurement item = new Measurement.Builder()
                    .withHeightUnit("ft")
                    .withFlowUnit("cfs")
                    .withNumber("123456")
                    .withInstant(Instant.parse("2024-09-16T00:00:00Z"))
                    .build();

            item.validate();
        }, "The validate method should have thrown a RequiredFieldException due to missing fields.");

        assertThrows(RequiredFieldException.class, () -> {
            Measurement item = new Measurement.Builder()
                    .withHeightUnit("ft")
                    .withFlowUnit("cfs")
                    .withId(new CwmsId.Builder().withName("Location123").withOfficeId("SPK").build())
                    .withInstant(Instant.parse("2024-09-16T00:00:00Z"))
                    .build();

            item.validate();
        }, "The validate method should have thrown a RequiredFieldException due to missing fields.");
    }

    @Test
    void createMeasurement_serialize_roundtrip() {
        CwmsId cwmsId = new CwmsId.Builder()
                .withName("Location123")
                .withOfficeId("SPK")
                .build();

        StreamflowMeasurement sfm = new StreamflowMeasurement.Builder()
                .withGageHeight(5.5)
                .withFlow(250.0)
                .withQuality("Good")
                .build();

        SupplementalStreamflowMeasurement supplementalStreamflowMeas = new SupplementalStreamflowMeasurement.Builder()
                .withChannelFlow(300.0)
                .withOverbankFlow(50.0)
                .withOverbankMaxDepth(5.0)
                .withChannelMaxDepth(10.0)
                .withAvgVelocity(1.5)
                .withSurfaceVelocity(2.0)
                .withMaxVelocity(3.0)
                .withEffectiveFlowArea(200.0)
                .withCrossSectionalArea(250.0)
                .withMeanGage(20.0)
                .withTopWidth(30.0)
                .withMainChannelArea(150.0)
                .withOverbankArea(80.0)
                .build();

        UsgsMeasurement usgsMeas = new UsgsMeasurement.Builder()
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

        Measurement measurement = new Measurement.Builder()
                .withId(cwmsId)
                .withHeightUnit("ft")
                .withFlowUnit("cfs")
                .withTempUnit("F")
                .withVelocityUnit("ft/s")
                .withAreaUnit("sq ft")
                .withUsed(true)
                .withAgency("USGS")
                .withParty("Survey Party")
                .withWmComments("Measurement made during normal flow conditions.")
                .withInstant(Instant.parse("2024-09-16T00:00:00Z"))
                .withNumber("123456")
                .withStreamflowMeasurement(sfm)
                .withSupplementalStreamflowMeasurement(supplementalStreamflowMeas)
                .withUsgsMeasurement(usgsMeas)
                .build();

        ContentType contentType = new ContentType(Formats.JSON);
        String json = Formats.format(contentType, measurement);
        Measurement deserialized = Formats.parseContent(contentType, json, Measurement.class);

        DTOMatch.assertMatch(measurement, deserialized);
    }

    @Test
    void createMeasurement_deserialize() throws Exception {
        CwmsId cwmsId = new CwmsId.Builder()
                .withName("Location123")
                .withOfficeId("SPK")
                .build();

        StreamflowMeasurement sfm = new StreamflowMeasurement.Builder()
                .withGageHeight(5.5)
                .withFlow(250.0)
                .withQuality("Good")
                .build();

        SupplementalStreamflowMeasurement supplementalStreamflowMeas = new SupplementalStreamflowMeasurement.Builder()
                .withChannelFlow(300.0)
                .withOverbankFlow(50.0)
                .withOverbankMaxDepth(5.0)
                .withChannelMaxDepth(10.0)
                .withAvgVelocity(1.5)
                .withSurfaceVelocity(2.0)
                .withMaxVelocity(3.0)
                .withEffectiveFlowArea(200.0)
                .withCrossSectionalArea(250.0)
                .withMeanGage(20.0)
                .withTopWidth(30.0)
                .withMainChannelArea(150.0)
                .withOverbankArea(80.0)
                .build();

        UsgsMeasurement usgsMeas = new UsgsMeasurement.Builder()
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

        Measurement expectedMeasurement = new Measurement.Builder()
                .withId(cwmsId)
                .withHeightUnit("ft")
                .withFlowUnit("cfs")
                .withTempUnit("F")
                .withVelocityUnit("ft/s")
                .withAreaUnit("sq ft")
                .withUsed(true)
                .withAgency("USGS")
                .withParty("Survey Party")
                .withWmComments("Measurement made during normal flow conditions.")
                .withInstant(Instant.parse("2024-09-16T00:00:00Z"))
                .withNumber("123456")
                .withStreamflowMeasurement(sfm)
                .withSupplementalStreamflowMeasurement(supplementalStreamflowMeas)
                .withUsgsMeasurement(usgsMeas)
                .build();

        InputStream resource = this.getClass().getResourceAsStream("/cwms/cda/data/dto/measurement.json");
        assertNotNull(resource);
        String json = IOUtils.toString(resource, StandardCharsets.UTF_8);
        ContentType contentType = new ContentType(Formats.JSON);
        Measurement deserialized = Formats.parseContent(contentType, json, Measurement.class);

        DTOMatch.assertMatch(expectedMeasurement, deserialized);
    }
}
