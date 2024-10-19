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
package cwms.cda.api;

import com.google.common.flogger.FluentLogger;
import cwms.cda.api.enums.UnitSystem;
import static cwms.cda.data.dao.DaoTest.getDslContext;
import cwms.cda.data.dao.DeleteRule;
import cwms.cda.data.dao.MeasurementDao;
import static cwms.cda.data.dao.MeasurementDaoTestIT.MINIMUM_SCHEMA;
import cwms.cda.data.dao.StreamDao;
import cwms.cda.data.dto.CwmsId;
import cwms.cda.data.dto.measurement.Measurement;
import cwms.cda.data.dto.stream.Stream;
import cwms.cda.formatters.ContentType;
import cwms.cda.formatters.Formats;
import static cwms.cda.security.KeyAccessManager.AUTH_HEADER;
import fixtures.CwmsDataApiSetupCallback;
import fixtures.MinimumSchema;
import fixtures.TestAccounts;
import static io.restassured.RestAssured.given;
import io.restassured.filter.log.LogDetail;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import javax.servlet.http.HttpServletResponse;
import mil.army.usace.hec.test.database.CwmsDatabaseContainer;
import org.apache.commons.io.IOUtils;
import org.junit.jupiter.api.AfterAll;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import static org.hamcrest.Matchers.*;

@Tag("integration")
final class MeasurementControllerTestIT extends DataApiTestIT {


    private static final FluentLogger LOGGER = FluentLogger.forEnclosingClass();
    private static final String OFFICE_ID = TestAccounts.KeyUser.SPK_NORMAL.getOperatingOffice();
    private static final List<Stream> TEST_STREAMS = new ArrayList<>();
    private static final List<String> TEST_STREAM_LOC_IDS = new ArrayList<>();

    @BeforeAll
    public static void setup() throws SQLException {
        String testLoc = "StreamLoc321"; // match the stream location name in the json file
        createLocation(testLoc, true, OFFICE_ID, "STREAM_LOCATION");
        TEST_STREAM_LOC_IDS.add(testLoc);
        createAndStoreTestStream("ImOnThisStream2");
    }

    static void createAndStoreTestStream(String testLoc) throws SQLException {
        createLocation(testLoc, true, OFFICE_ID, "STREAM");
        CwmsDatabaseContainer<?> db = CwmsDataApiSetupCallback.getDatabaseLink();
        db.connection(c -> {
            StreamDao streamDao = new StreamDao(getDslContext(c, OFFICE_ID));
            Stream streamToStore = new Stream.Builder()
                    .withId(new CwmsId.Builder()
                            .withOfficeId(OFFICE_ID)
                            .withName(testLoc)
                            .build())
                    .withLength(100.0)
                    .withLengthUnits("km")
                    .build();
            TEST_STREAMS.add(streamToStore);
            streamDao.storeStream(streamToStore, false);
        }, CwmsDataApiSetupCallback.getWebUser());
    }

    @AfterAll
    public static void tearDown() {
        for (Stream stream : TEST_STREAMS) {
            try {
                CwmsDatabaseContainer<?> db = CwmsDataApiSetupCallback.getDatabaseLink();
                db.connection(c -> {
                    StreamDao streamDao = new StreamDao(getDslContext(c, OFFICE_ID));
                    try {
                        streamDao.deleteStream(stream.getId().getOfficeId(), stream.getId().getName(), DeleteRule.DELETE_ALL);
                    } catch (Exception e) {
                        LOGGER.atInfo().log("Failed to delete stream: " + stream.getId().getName() + ". Stream likely already deleted");
                    }
                }, CwmsDataApiSetupCallback.getWebUser());
            } catch (SQLException ex) {
                throw new RuntimeException(ex);
            }
        }
        TEST_STREAMS.clear();
        for(String measLoc: TEST_STREAM_LOC_IDS)
        {
            try {
                CwmsDatabaseContainer<?> db = CwmsDataApiSetupCallback.getDatabaseLink();
                db.connection(c -> {
                    MeasurementDao measDao = new MeasurementDao(getDslContext(c, OFFICE_ID));
                    try {
                        measDao.deleteMeasurements(OFFICE_ID, measLoc, null, null, null, null);
                    } catch (Exception e) {
                        LOGGER.atInfo().log("Failed to delete measurements for: " + measLoc + ". Measurement(s) likely already deleted");
                    }
                }, CwmsDataApiSetupCallback.getWebUser());
            } catch (SQLException ex) {
                throw new RuntimeException(ex);
            }
        }
    }

    @Test
    @MinimumSchema(MINIMUM_SCHEMA)
    void test_create_retrieve_delete_measurement() throws IOException {
        InputStream resource = this.getClass().getResourceAsStream("/cwms/cda/api/measurement.json");
        assertNotNull(resource);
        String json = IOUtils.toString(resource, StandardCharsets.UTF_8);
        assertNotNull(json);
        List<Measurement> measurements = Formats.parseContentList(new ContentType(Formats.JSON), json, Measurement.class);
        Measurement measurement = measurements.get(0);
        TestAccounts.KeyUser user = TestAccounts.KeyUser.SPK_NORMAL;

        // Create the Measurement
        given()
                .log().ifValidationFails(LogDetail.ALL, true)
                .accept(Formats.JSON)
                .contentType(Formats.JSON)
                .body(json)
                .header(AUTH_HEADER, user.toHeaderValue())
        .when()
                .redirects().follow(true)
                .redirects().max(3)
                .post("/measurements/")
        .then()
                .log().ifValidationFails(LogDetail.ALL, true)
        .assertThat()
                .statusCode(is(HttpServletResponse.SC_CREATED));

        String locationId = measurement.getLocationId();
        String number = measurement.getNumber();

        // Retrieve the Measurement and assert that it exists
        given()
                .log().ifValidationFails(LogDetail.ALL, true)
                .accept(Formats.JSON)
                .queryParam(Controllers.OFFICE_MASK, measurement.getId().getOfficeId())
                .queryParam(Controllers.ID_MASK, measurement.getLocationId())
                .queryParam(Controllers.MIN_NUMBER, number)
                .queryParam(Controllers.MAX_NUMBER, number)
                .queryParam(Controllers.MIN_HEIGHT, 0.0)
                .queryParam(Controllers.MAX_FLOW, 1000)
                .queryParam(Controllers.UNIT_SYSTEM, UnitSystem.EN.getValue())
        .when()
                .redirects().follow(true)
                .redirects().max(3)
                .get("/measurements/")
        .then()
                .log().ifValidationFails(LogDetail.ALL, true)
        .assertThat()
                .statusCode(is(HttpServletResponse.SC_OK))
                .body("[0].height-unit", equalTo(measurement.getHeightUnit()))
                .body("[0].flow-unit", equalTo(measurement.getFlowUnit()))
                .body("[0].temp-unit", equalTo(measurement.getTempUnit()))
                .body("[0].velocity-unit", equalTo(measurement.getVelocityUnit()))
                .body("[0].area-unit", equalTo(measurement.getAreaUnit()))
                .body("[0].used", equalTo(measurement.isUsed()))
                .body("[0].agency", equalTo(measurement.getAgency()))
                .body("[0].party", equalTo(measurement.getParty()))
                .body("[0].wm-comments", equalTo(measurement.getWmComments()))
                .body("[0].instant", equalTo(measurement.getInstant().toString()))
                .body("[0].number", equalTo(measurement.getNumber()))
                .body("[0].id.name", equalTo(measurement.getLocationId()))
                .body("[0].id.office-id", equalTo(measurement.getOfficeId()))
                .body("[0].streamflow-measurement.gage-height", equalTo(measurement.getStreamflowMeasurement().getGageHeight().floatValue()))
                .body("[0].streamflow-measurement.flow", equalTo(measurement.getStreamflowMeasurement().getFlow().floatValue()))
                .body("[0].streamflow-measurement.quality", equalTo(measurement.getStreamflowMeasurement().getQuality()))
                .body("[0].supplemental-streamflow-measurement.channel-flow", equalTo(measurement.getSupplementalStreamflowMeasurement().getChannelFlow().floatValue()))
                .body("[0].supplemental-streamflow-measurement.overbank-flow", equalTo(measurement.getSupplementalStreamflowMeasurement().getOverbankFlow().floatValue()))
                .body("[0].supplemental-streamflow-measurement.overbank-max-depth", equalTo(measurement.getSupplementalStreamflowMeasurement().getOverbankMaxDepth().floatValue()))
                .body("[0].supplemental-streamflow-measurement.channel-max-depth", equalTo(measurement.getSupplementalStreamflowMeasurement().getChannelMaxDepth().floatValue()))
                .body("[0].supplemental-streamflow-measurement.avg-velocity", equalTo(measurement.getSupplementalStreamflowMeasurement().getAvgVelocity().floatValue()))
                .body("[0].supplemental-streamflow-measurement.surface-velocity", equalTo(measurement.getSupplementalStreamflowMeasurement().getSurfaceVelocity().floatValue()))
                .body("[0].supplemental-streamflow-measurement.max-velocity", equalTo(measurement.getSupplementalStreamflowMeasurement().getMaxVelocity().floatValue()))
                .body("[0].supplemental-streamflow-measurement.effective-flow-area", equalTo(measurement.getSupplementalStreamflowMeasurement().getEffectiveFlowArea().floatValue()))
                .body("[0].supplemental-streamflow-measurement.cross-sectional-area", equalTo(measurement.getSupplementalStreamflowMeasurement().getCrossSectionalArea().floatValue()))
                .body("[0].supplemental-streamflow-measurement.mean-gage", equalTo(measurement.getSupplementalStreamflowMeasurement().getMeanGage().floatValue()))
                .body("[0].supplemental-streamflow-measurement.top-width", equalTo(measurement.getSupplementalStreamflowMeasurement().getTopWidth().floatValue()))
                .body("[0].supplemental-streamflow-measurement.main-channel-area", equalTo(measurement.getSupplementalStreamflowMeasurement().getMainChannelArea().floatValue()))
                .body("[0].supplemental-streamflow-measurement.overbank-area", equalTo(measurement.getSupplementalStreamflowMeasurement().getOverbankArea().floatValue()))
                .body("[0].usgs-measurement.remarks", equalTo(measurement.getUsgsMeasurement().getRemarks()))
                .body("[0].usgs-measurement.current-rating", equalTo(measurement.getUsgsMeasurement().getCurrentRating()))
                .body("[0].usgs-measurement.control-condition", equalTo(measurement.getUsgsMeasurement().getControlCondition()))
                .body("[0].usgs-measurement.flow-adjustment", equalTo(measurement.getUsgsMeasurement().getFlowAdjustment()))
                .body("[0].usgs-measurement.shift-used", equalTo(measurement.getUsgsMeasurement().getShiftUsed().floatValue()))
                .body("[0].usgs-measurement.percent-difference", equalTo(measurement.getUsgsMeasurement().getPercentDifference().floatValue()))
                .body("[0].usgs-measurement.delta-height", equalTo(measurement.getUsgsMeasurement().getDeltaHeight().floatValue()))
                .body("[0].usgs-measurement.delta-time", equalTo(measurement.getUsgsMeasurement().getDeltaTime().floatValue()))
                .body("[0].usgs-measurement.air-temp", equalTo(measurement.getUsgsMeasurement().getAirTemp().floatValue()))
                .body("[0].usgs-measurement.water-temp", equalTo(measurement.getUsgsMeasurement().getWaterTemp().floatValue()));

        // Delete the Measurement
        given()
                .log().ifValidationFails(LogDetail.ALL, true)
                .accept(Formats.JSON)
                .header(AUTH_HEADER, user.toHeaderValue())
                .queryParam(Controllers.OFFICE, measurement.getId().getOfficeId())
                .queryParam(Controllers.MIN_NUMBER, number)
                .queryParam(Controllers.MAX_NUMBER, number)
        .when()
                .redirects().follow(true)
                .redirects().max(3)
                .delete("/measurements/" + locationId)
        .then()
                .log().ifValidationFails(LogDetail.ALL, true)
        .assertThat()
                .statusCode(is(HttpServletResponse.SC_NO_CONTENT));

        // Retrieve the Measurement and assert that it does not exist
        given()
                .log().ifValidationFails(LogDetail.ALL, true)
                .accept(Formats.JSON)
                .queryParam(Controllers.OFFICE, measurement.getId().getOfficeId())
                .queryParam(Controllers.ID_MASK, measurement.getLocationId())
                .queryParam(Controllers.MIN_NUMBER, number)
                .queryParam(Controllers.MAX_NUMBER, number)
                .queryParam(Controllers.UNIT_SYSTEM, UnitSystem.EN.getValue())
        .when()
                .redirects().follow(true)
                .redirects().max(3)
                .get("/measurements/")
        .then()
                .log().ifValidationFails(LogDetail.ALL, true)
        .assertThat()
                .statusCode(is(HttpServletResponse.SC_NOT_FOUND));
    }

    @Test
    @MinimumSchema(MINIMUM_SCHEMA)
    void test_create_retrieve_delete_measurement_multiple() throws IOException {
        InputStream resource = this.getClass().getResourceAsStream("/cwms/cda/api/measurements.json");
        assertNotNull(resource);
        String json = IOUtils.toString(resource, StandardCharsets.UTF_8);
        assertNotNull(json);
        List<Measurement> measurements = Formats.parseContentList(new ContentType(Formats.JSON), json, Measurement.class);

        Measurement measurement1 = measurements.get(0);
        Measurement measurement2 = measurements.get(1);
        TestAccounts.KeyUser user = TestAccounts.KeyUser.SPK_NORMAL;

        // Create the Measurements
        given()
                .log().ifValidationFails(LogDetail.ALL, true)
                .accept(Formats.JSON)
                .contentType(Formats.JSON)
                .body(json)
                .header(AUTH_HEADER, user.toHeaderValue())
        .when()
                .redirects().follow(true)
                .redirects().max(3)
                .post("/measurements/")
        .then()
                .log().ifValidationFails(LogDetail.ALL, true)
        .assertThat()
                .statusCode(is(HttpServletResponse.SC_CREATED));

        // Retrieve the Measurements and assert that they exists
        given()
                .log().ifValidationFails(LogDetail.ALL, true)
                .accept(Formats.JSON)
                .queryParam(Controllers.OFFICE_MASK, measurement1.getId().getOfficeId())
                .queryParam(Controllers.ID_MASK, measurement1.getLocationId())
                .queryParam(Controllers.UNIT_SYSTEM, UnitSystem.EN.getValue())
        .when()
                .redirects().follow(true)
                .redirects().max(3)
                .get("/measurements/")
        .then()
                .log().ifValidationFails(LogDetail.ALL, true)
        .assertThat()
                .statusCode(is(HttpServletResponse.SC_OK))
                .body("[0].height-unit", equalTo(measurement1.getHeightUnit()))
                .body("[0].flow-unit", equalTo(measurement1.getFlowUnit()))
                .body("[0].temp-unit", equalTo(measurement1.getTempUnit()))
                .body("[0].velocity-unit", equalTo(measurement1.getVelocityUnit()))
                .body("[0].area-unit", equalTo(measurement1.getAreaUnit()))
                .body("[0].used", equalTo(measurement1.isUsed()))
                .body("[0].agency", equalTo(measurement1.getAgency()))
                .body("[0].party", equalTo(measurement1.getParty()))
                .body("[0].wm-comments", equalTo(measurement1.getWmComments()))
                .body("[0].instant", equalTo(measurement1.getInstant().toString()))
                .body("[0].number", equalTo(measurement1.getNumber()))
                .body("[0].id.name", equalTo(measurement1.getLocationId()))
                .body("[0].id.office-id", equalTo(measurement1.getOfficeId()))
                .body("[0].streamflow-measurement.gage-height", equalTo(measurement1.getStreamflowMeasurement().getGageHeight().floatValue()))
                .body("[0].streamflow-measurement.flow", equalTo(measurement1.getStreamflowMeasurement().getFlow().floatValue()))
                .body("[0].streamflow-measurement.quality", equalTo(measurement1.getStreamflowMeasurement().getQuality()))
                .body("[0].supplemental-streamflow-measurement.channel-flow", equalTo(measurement1.getSupplementalStreamflowMeasurement().getChannelFlow().floatValue()))
                .body("[0].supplemental-streamflow-measurement.overbank-flow", equalTo(measurement1.getSupplementalStreamflowMeasurement().getOverbankFlow().floatValue()))
                .body("[0].supplemental-streamflow-measurement.overbank-max-depth", equalTo(measurement1.getSupplementalStreamflowMeasurement().getOverbankMaxDepth().floatValue()))
                .body("[0].supplemental-streamflow-measurement.channel-max-depth", equalTo(measurement1.getSupplementalStreamflowMeasurement().getChannelMaxDepth().floatValue()))
                .body("[0].supplemental-streamflow-measurement.avg-velocity", equalTo(measurement1.getSupplementalStreamflowMeasurement().getAvgVelocity().floatValue()))
                .body("[0].supplemental-streamflow-measurement.surface-velocity", equalTo(measurement1.getSupplementalStreamflowMeasurement().getSurfaceVelocity().floatValue()))
                .body("[0].supplemental-streamflow-measurement.max-velocity", equalTo(measurement1.getSupplementalStreamflowMeasurement().getMaxVelocity().floatValue()))
                .body("[0].supplemental-streamflow-measurement.effective-flow-area", equalTo(measurement1.getSupplementalStreamflowMeasurement().getEffectiveFlowArea().floatValue()))
                .body("[0].supplemental-streamflow-measurement.cross-sectional-area", equalTo(measurement1.getSupplementalStreamflowMeasurement().getCrossSectionalArea().floatValue()))
                .body("[0].supplemental-streamflow-measurement.mean-gage", equalTo(measurement1.getSupplementalStreamflowMeasurement().getMeanGage().floatValue()))
                .body("[0].supplemental-streamflow-measurement.top-width", equalTo(measurement1.getSupplementalStreamflowMeasurement().getTopWidth().floatValue()))
                .body("[0].supplemental-streamflow-measurement.main-channel-area", equalTo(measurement1.getSupplementalStreamflowMeasurement().getMainChannelArea().floatValue()))
                .body("[0].supplemental-streamflow-measurement.overbank-area", equalTo(measurement1.getSupplementalStreamflowMeasurement().getOverbankArea().floatValue()))
                .body("[0].usgs-measurement.remarks", equalTo(measurement1.getUsgsMeasurement().getRemarks()))
                .body("[0].usgs-measurement.current-rating", equalTo(measurement1.getUsgsMeasurement().getCurrentRating()))
                .body("[0].usgs-measurement.control-condition", equalTo(measurement1.getUsgsMeasurement().getControlCondition()))
                .body("[0].usgs-measurement.flow-adjustment", equalTo(measurement1.getUsgsMeasurement().getFlowAdjustment()))
                .body("[0].usgs-measurement.shift-used", equalTo(measurement1.getUsgsMeasurement().getShiftUsed().floatValue()))
                .body("[0].usgs-measurement.percent-difference", equalTo(measurement1.getUsgsMeasurement().getPercentDifference().floatValue()))
                .body("[0].usgs-measurement.delta-height", equalTo(measurement1.getUsgsMeasurement().getDeltaHeight().floatValue()))
                .body("[0].usgs-measurement.delta-time", equalTo(measurement1.getUsgsMeasurement().getDeltaTime().floatValue()))
                .body("[0].usgs-measurement.air-temp", equalTo(measurement1.getUsgsMeasurement().getAirTemp().floatValue()))
                .body("[0].usgs-measurement.water-temp", equalTo(measurement1.getUsgsMeasurement().getWaterTemp().floatValue()))
                .body("[1].height-unit", equalTo(measurement2.getHeightUnit()))
                .body("[1].flow-unit", equalTo(measurement2.getFlowUnit()))
                .body("[1].temp-unit", equalTo(measurement2.getTempUnit()))
                .body("[1].velocity-unit", equalTo(measurement2.getVelocityUnit()))
                .body("[1].area-unit", equalTo(measurement2.getAreaUnit()))
                .body("[1].used", equalTo(measurement2.isUsed()))
                .body("[1].agency", equalTo(measurement2.getAgency()))
                .body("[1].party", equalTo(measurement2.getParty()))
                .body("[1].wm-comments", equalTo(measurement2.getWmComments()))
                .body("[1].instant", equalTo(measurement2.getInstant().toString()))
                .body("[1].number", equalTo(measurement2.getNumber()))
                .body("[1].id.name", equalTo(measurement2.getLocationId()))
                .body("[1].id.office-id", equalTo(measurement2.getOfficeId()))
                .body("[1].streamflow-measurement.gage-height", equalTo(measurement2.getStreamflowMeasurement().getGageHeight().floatValue()))
                .body("[1].streamflow-measurement.flow", equalTo(measurement2.getStreamflowMeasurement().getFlow().floatValue()))
                .body("[1].streamflow-measurement.quality", equalTo(measurement2.getStreamflowMeasurement().getQuality()))
                .body("[1].supplemental-streamflow-measurement.channel-flow", equalTo(measurement2.getSupplementalStreamflowMeasurement().getChannelFlow().floatValue()))
                .body("[1].supplemental-streamflow-measurement.overbank-flow", equalTo(measurement2.getSupplementalStreamflowMeasurement().getOverbankFlow().floatValue()))
                .body("[1].supplemental-streamflow-measurement.overbank-max-depth", equalTo(measurement2.getSupplementalStreamflowMeasurement().getOverbankMaxDepth().floatValue()))
                .body("[1].supplemental-streamflow-measurement.channel-max-depth", equalTo(measurement2.getSupplementalStreamflowMeasurement().getChannelMaxDepth().floatValue()))
                .body("[1].supplemental-streamflow-measurement.avg-velocity", equalTo(measurement2.getSupplementalStreamflowMeasurement().getAvgVelocity().floatValue()))
                .body("[1].supplemental-streamflow-measurement.surface-velocity", equalTo(measurement2.getSupplementalStreamflowMeasurement().getSurfaceVelocity().floatValue()))
                .body("[1].supplemental-streamflow-measurement.max-velocity", equalTo(measurement2.getSupplementalStreamflowMeasurement().getMaxVelocity().floatValue()))
                .body("[1].supplemental-streamflow-measurement.effective-flow-area", equalTo(measurement2.getSupplementalStreamflowMeasurement().getEffectiveFlowArea().floatValue()))
                .body("[1].supplemental-streamflow-measurement.cross-sectional-area", equalTo(measurement2.getSupplementalStreamflowMeasurement().getCrossSectionalArea().floatValue()))
                .body("[1].supplemental-streamflow-measurement.mean-gage", equalTo(measurement2.getSupplementalStreamflowMeasurement().getMeanGage().floatValue()))
                .body("[1].supplemental-streamflow-measurement.top-width", equalTo(measurement2.getSupplementalStreamflowMeasurement().getTopWidth().floatValue()))
                .body("[1].supplemental-streamflow-measurement.main-channel-area", equalTo(measurement2.getSupplementalStreamflowMeasurement().getMainChannelArea().floatValue()))
                .body("[1].supplemental-streamflow-measurement.overbank-area", equalTo(measurement2.getSupplementalStreamflowMeasurement().getOverbankArea().floatValue()))
                .body("[1].usgs-measurement.remarks", equalTo(measurement2.getUsgsMeasurement().getRemarks()))
                .body("[1].usgs-measurement.current-rating", equalTo(measurement2.getUsgsMeasurement().getCurrentRating()))
                .body("[1].usgs-measurement.control-condition", equalTo(measurement2.getUsgsMeasurement().getControlCondition()))
                .body("[1].usgs-measurement.flow-adjustment", equalTo(measurement2.getUsgsMeasurement().getFlowAdjustment()))
                .body("[1].usgs-measurement.shift-used", equalTo(measurement2.getUsgsMeasurement().getShiftUsed().floatValue()))
                .body("[1].usgs-measurement.percent-difference", equalTo(measurement2.getUsgsMeasurement().getPercentDifference().floatValue()))
                .body("[1].usgs-measurement.delta-height", equalTo(measurement2.getUsgsMeasurement().getDeltaHeight().floatValue()))
                .body("[1].usgs-measurement.delta-time", equalTo(measurement2.getUsgsMeasurement().getDeltaTime().floatValue()))
                .body("[1].usgs-measurement.air-temp", equalTo(measurement2.getUsgsMeasurement().getAirTemp().floatValue()))
                .body("[1].usgs-measurement.water-temp", equalTo(measurement2.getUsgsMeasurement().getWaterTemp().floatValue()));

        // Delete the Measurements
        given()
                .log().ifValidationFails(LogDetail.ALL, true)
                .accept(Formats.JSON)
                .header(AUTH_HEADER, user.toHeaderValue())
                .queryParam(Controllers.OFFICE, OFFICE_ID)
        .when()
                .redirects().follow(true)
                .redirects().max(3)
                .delete("/measurements/" +  measurement1.getLocationId())
        .then()
                .log().ifValidationFails(LogDetail.ALL, true)
        .assertThat()
                .statusCode(is(HttpServletResponse.SC_NO_CONTENT));

        // Retrieve the Measurements and assert that they do not exist
        given()
                .log().ifValidationFails(LogDetail.ALL, true)
                .accept(Formats.JSON)
                .queryParam(Controllers.OFFICE, OFFICE_ID)
                .queryParam(Controllers.ID_MASK, measurement1.getLocationId())
                .queryParam(Controllers.UNIT_SYSTEM, UnitSystem.EN.getValue())
        .when()
                .redirects().follow(true)
                .redirects().max(3)
                .get("/measurements/")
        .then()
                .log().ifValidationFails(LogDetail.ALL, true)
        .assertThat()
                .statusCode(is(HttpServletResponse.SC_NOT_FOUND));
    }

    @Test
    @MinimumSchema(MINIMUM_SCHEMA)
    void test_delete_does_not_exist() {
        TestAccounts.KeyUser user = TestAccounts.KeyUser.SPK_NORMAL;
        // Delete a Measurement
        given()
                .log().ifValidationFails(LogDetail.ALL,true)
                .queryParam(Controllers.OFFICE, user.getOperatingOffice())
                .header(AUTH_HEADER, user.toHeaderValue())
        .when()
                .redirects().follow(true)
                .redirects().max(3)
        .delete("measurements/" + Instant.now().toEpochMilli())
                .then()
                .log().ifValidationFails(LogDetail.ALL,true)
        .assertThat()
                .statusCode(is(HttpServletResponse.SC_NOT_FOUND));
    }

}
