/*
 * MIT License
 *
 * Copyright (c) 2024 Hydrologic Engineering Center
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */
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
import java.util.ArrayList;
import java.util.List;
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

    @Test
    void createMeasurement_deserialize_multiple() throws Exception {
        CwmsId cwmsId1 = new CwmsId.Builder()
                .withName("Location123")
                .withOfficeId("SPK")
                .build();

        StreamflowMeasurement sfm1 = new StreamflowMeasurement.Builder()
                .withGageHeight(5.5)
                .withFlow(250.0)
                .withQuality("Good")
                .build();

        SupplementalStreamflowMeasurement supplementalStreamflowMeas1 = new SupplementalStreamflowMeasurement.Builder()
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

        UsgsMeasurement usgsMeas1 = new UsgsMeasurement.Builder()
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

        Measurement expectedMeasurement1 = new Measurement.Builder()
                .withId(cwmsId1)
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
                .withStreamflowMeasurement(sfm1)
                .withSupplementalStreamflowMeasurement(supplementalStreamflowMeas1)
                .withUsgsMeasurement(usgsMeas1)
                .build();

        // Second Measurement (updated)
        CwmsId cwmsId2 = new CwmsId.Builder()
                .withName("Location456")
                .withOfficeId("SPK")
                .build();

        StreamflowMeasurement sfm2 = new StreamflowMeasurement.Builder()
                .withGageHeight(6.0)
                .withFlow(275.0)
                .withQuality("Fair")
                .build();

        SupplementalStreamflowMeasurement supplementalStreamflowMeas2 = new SupplementalStreamflowMeasurement.Builder()
                .withChannelFlow(320.0)
                .withOverbankFlow(45.0)
                .withOverbankMaxDepth(4.5)
                .withChannelMaxDepth(9.5)
                .withAvgVelocity(1.8)
                .withSurfaceVelocity(2.5)
                .withMaxVelocity(3.5)
                .withEffectiveFlowArea(220.0)
                .withCrossSectionalArea(260.0)
                .withMeanGage(21.0)
                .withTopWidth(32.0)
                .withMainChannelArea(160.0)
                .withOverbankArea(85.0)
                .build();

        UsgsMeasurement usgsMeas2 = new UsgsMeasurement.Builder()
                .withRemarks("Post-rain conditions.")
                .withCurrentRating("Updated Rating")
                .withControlCondition("Adjusted Condition")
                .withFlowAdjustment("Flow Adjustment Applied")
                .withShiftUsed(0.15)
                .withPercentDifference(4.5)
                .withDeltaHeight(0.1)
                .withDeltaTime(15.0)
                .withAirTemp(18.0)
                .withWaterTemp(16.0)
                .build();

        Measurement expectedMeasurement2 = new Measurement.Builder()
                .withId(cwmsId2)
                .withHeightUnit("ft")
                .withFlowUnit("cfs")
                .withTempUnit("F")
                .withVelocityUnit("ft/s")
                .withAreaUnit("sq ft")
                .withUsed(false)
                .withAgency("USGS")
                .withParty("Second Survey Party")
                .withWmComments("Measurement made after recent rainfall.")
                .withInstant(Instant.parse("2024-09-17T12:00:00Z"))
                .withNumber("654321")
                .withStreamflowMeasurement(sfm2)
                .withSupplementalStreamflowMeasurement(supplementalStreamflowMeas2)
                .withUsgsMeasurement(usgsMeas2)
                .build();

        // Reading and Deserializing the JSON
        InputStream resource = this.getClass().getResourceAsStream("/cwms/cda/data/dto/measurements.json");
        assertNotNull(resource);
        String json = IOUtils.toString(resource, StandardCharsets.UTF_8);
        ContentType contentType = new ContentType(Formats.JSON);
        List<Measurement> deserialized = Formats.parseContentList(contentType, json, Measurement.class);

        // Assert both measurements were deserialized correctly
        assertEquals(2, deserialized.size(), "Expected 2 measurements");
        DTOMatch.assertMatch(expectedMeasurement1, deserialized.get(0));
        DTOMatch.assertMatch(expectedMeasurement2, deserialized.get(1));
    }

    @Test
    void testRoundTripMultiple()
    {
        CwmsId cwmsId1 = new CwmsId.Builder()
                .withName("Location123")
                .withOfficeId("SPK")
                .build();

        StreamflowMeasurement sfm1 = new StreamflowMeasurement.Builder()
                .withGageHeight(5.5)
                .withFlow(250.0)
                .withQuality("Good")
                .build();

        SupplementalStreamflowMeasurement supplementalStreamflowMeas1 = new SupplementalStreamflowMeasurement.Builder()
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

        UsgsMeasurement usgsMeas1 = new UsgsMeasurement.Builder()
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

        Measurement expectedMeasurement1 = new Measurement.Builder()
                .withId(cwmsId1)
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
                .withStreamflowMeasurement(sfm1)
                .withSupplementalStreamflowMeasurement(supplementalStreamflowMeas1)
                .withUsgsMeasurement(usgsMeas1)
                .build();

        // Second Measurement (updated)
        CwmsId cwmsId2 = new CwmsId.Builder()
                .withName("Location456")
                .withOfficeId("SPK")
                .build();

        StreamflowMeasurement sfm2 = new StreamflowMeasurement.Builder()
                .withGageHeight(6.0)
                .withFlow(275.0)
                .withQuality("Fair")
                .build();

        SupplementalStreamflowMeasurement supplementalStreamflowMeas2 = new SupplementalStreamflowMeasurement.Builder()
                .withChannelFlow(320.0)
                .withOverbankFlow(45.0)
                .withOverbankMaxDepth(4.5)
                .withChannelMaxDepth(9.5)
                .withAvgVelocity(1.8)
                .withSurfaceVelocity(2.5)
                .withMaxVelocity(3.5)
                .withEffectiveFlowArea(220.0)
                .withCrossSectionalArea(260.0)
                .withMeanGage(21.0)
                .withTopWidth(32.0)
                .withMainChannelArea(160.0)
                .withOverbankArea(85.0)
                .build();

        UsgsMeasurement usgsMeas2 = new UsgsMeasurement.Builder()
                .withRemarks("Post-rain conditions.")
                .withCurrentRating("Updated Rating")
                .withControlCondition("Adjusted Condition")
                .withFlowAdjustment("Flow Adjustment Applied")
                .withShiftUsed(0.15)
                .withPercentDifference(4.5)
                .withDeltaHeight(0.1)
                .withDeltaTime(15.0)
                .withAirTemp(18.0)
                .withWaterTemp(16.0)
                .build();

        Measurement expectedMeasurement2 = new Measurement.Builder()
                .withId(cwmsId2)
                .withHeightUnit("ft")
                .withFlowUnit("cfs")
                .withTempUnit("F")
                .withVelocityUnit("ft/s")
                .withAreaUnit("sq ft")
                .withUsed(false)
                .withAgency("USGS")
                .withParty("Second Survey Party")
                .withWmComments("Measurement made after recent rainfall.")
                .withInstant(Instant.parse("2024-09-17T12:00:00Z"))
                .withNumber("654321")
                .withStreamflowMeasurement(sfm2)
                .withSupplementalStreamflowMeasurement(supplementalStreamflowMeas2)
                .withUsgsMeasurement(usgsMeas2)
                .build();

        List<Measurement> measurements = new ArrayList<>();
        measurements.add(expectedMeasurement1);
        measurements.add(expectedMeasurement2);

        ContentType contentType = new ContentType(Formats.JSON);
        String serialized = Formats.format(contentType, measurements, Measurement.class);
        List<Measurement> deserialized = Formats.parseContentList(contentType, serialized, Measurement.class);

        assertEquals(2, deserialized.size(), "Expected 2 measurements");
        DTOMatch.assertMatch(expectedMeasurement1, deserialized.get(0));
        DTOMatch.assertMatch(expectedMeasurement2, deserialized.get(1));
    }
}
