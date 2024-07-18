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

import static cwms.cda.api.Controllers.AREA_UNIT;
import static cwms.cda.api.Controllers.NAME;
import static cwms.cda.api.Controllers.NAME_MASK;
import static cwms.cda.api.Controllers.OFFICE;
import static cwms.cda.api.Controllers.OFFICE_MASK;
import static cwms.cda.api.Controllers.STAGE_UNIT;
import static cwms.cda.api.Controllers.STATION_UNIT;
import static cwms.cda.api.Controllers.STREAM_ID;
import static cwms.cda.api.Controllers.STREAM_ID_MASK;
import cwms.cda.api.errors.NotFoundException;
import static cwms.cda.data.dao.DaoTest.getDslContext;
import cwms.cda.data.dao.DeleteRule;
import cwms.cda.data.dao.StreamDao;
import cwms.cda.data.dto.CwmsId;
import cwms.cda.data.dto.stream.Stream;
import cwms.cda.data.dto.stream.StreamLocation;
import cwms.cda.formatters.ContentType;
import cwms.cda.formatters.Formats;
import fixtures.CwmsDataApiSetupCallback;
import fixtures.TestAccounts;
import io.restassured.filter.log.LogDetail;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import mil.army.usace.hec.test.database.CwmsDatabaseContainer;
import org.apache.commons.io.IOUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;

import static cwms.cda.security.KeyAccessManager.AUTH_HEADER;
import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.assertNotNull;

@Tag("integration")
final class StreamLocationControllerTestIT extends DataApiTestIT {

    private static final String OFFICE_ID = TestAccounts.KeyUser.SPK_NORMAL.getOperatingOffice();
    private static final List<Stream> STREAMS_CREATED = new ArrayList<>();

    @BeforeAll
    public static void setup() throws SQLException {
        String testLoc = "StreamLoc123"; // match the stream location name in the json file
        createLocation(testLoc, true, OFFICE_ID, "STREAM_LOCATION");
        createAndStoreTestStream("ImOnThisStream");
    }

    private static void createAndStoreTestStream(String testLoc) throws SQLException {
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
            STREAMS_CREATED.add(streamToStore);
            streamDao.storeStream(streamToStore, true);
        });
    }

    @AfterAll
    public static void tearDown() {
        for (Stream stream : STREAMS_CREATED) {
            try {
                CwmsDatabaseContainer<?> db = CwmsDataApiSetupCallback.getDatabaseLink();
                db.connection(c -> {
                    StreamDao streamDao = new StreamDao(getDslContext(c, OFFICE_ID));
                    try {
                        streamDao.deleteStream(stream.getId().getOfficeId(), stream.getId().getName(), DeleteRule.DELETE_ALL);
                    } catch (NotFoundException e) {
                        // ignore
                    }
                });
            } catch (SQLException ex) {
                throw new RuntimeException(ex);
            }
        }
        STREAMS_CREATED.clear();
    }

    @Test
    void test_get_create_delete() throws IOException {
        InputStream resource = this.getClass().getResourceAsStream("/cwms/cda/api/stream_location.json");
        assertNotNull(resource);
        String json = IOUtils.toString(resource, StandardCharsets.UTF_8);
        assertNotNull(json);
        StreamLocation streamLocation = Formats.parseContent(new ContentType(Formats.JSON), json, StreamLocation.class);

        // Structure of test:
        // 1) Create the StreamLocation
        // 2) Retrieve the StreamLocation and assert that it exists
        // 3) Delete the StreamLocation
        // 4) Retrieve the StreamLocation and assert that it does not exist
        TestAccounts.KeyUser user = TestAccounts.KeyUser.SPK_NORMAL;

        // Create the StreamLocation
        given()
                .log().ifValidationFails(LogDetail.ALL, true)
                .accept(Formats.JSON)
                .contentType(Formats.JSON)
                .body(json)
                .header(AUTH_HEADER, user.toHeaderValue())
        .when()
                .redirects().follow(true)
                .redirects().max(3)
                .post("/stream-locations/")
        .then()
                .log().ifValidationFails(LogDetail.ALL, true)
        .assertThat()
                .statusCode(is(HttpServletResponse.SC_CREATED));

        String streamLocationId = streamLocation.getId().getName();

        // Retrieve the StreamLocation and assert that it exists
        given()
                .log().ifValidationFails(LogDetail.ALL, true)
                .accept(Formats.JSON)
                .queryParam(NAME, streamLocationId)
                .queryParam(OFFICE, OFFICE_ID)
                .queryParam(STREAM_ID, streamLocation.getStreamId().getName())
                .queryParam(STATION_UNIT, "km")
                .queryParam(AREA_UNIT, "km2")
                .queryParam(STAGE_UNIT, "m")
        .when()
                .redirects().follow(true)
                .redirects().max(3)
                .get("/stream-locations/" + streamLocationId)
        .then()
                .log().ifValidationFails(LogDetail.ALL, true)
         .assertThat()
                .statusCode(is(HttpServletResponse.SC_OK))
                .body("stream-location-node.id.office-id", equalTo(streamLocation.getId().getOfficeId()))
                .body("stream-location-node.id.name", equalTo(streamLocation.getId().getName()))
                .body("stream-location-node.stream-node.stream-id.office-id", equalTo(streamLocation.getStreamId().getOfficeId()))
                .body("stream-location-node.stream-node.stream-id.name", equalTo(streamLocation.getStreamId().getName()))
                .body("stream-location-node.stream-node.bank", equalTo(streamLocation.getStreamLocationNode().getStreamNode().getBank().getCode()))
                .body("stream-location-node.stream-node.station", equalTo(streamLocation.getStreamLocationNode().getStreamNode().getStation().floatValue()))
                .body("stream-location-node.stream-node.station-units", equalTo(streamLocation.getStreamLocationNode().getStreamNode().getStationUnits()))
                .body("published-station", equalTo(streamLocation.getPublishedStation().floatValue()))
                .body("navigation-station", equalTo(streamLocation.getNavigationStation().floatValue()))
                .body("lowest-measurable-stage", equalTo(streamLocation.getLowestMeasurableStage().floatValue()))
                .body("total-drainage-area", equalTo(streamLocation.getTotalDrainageArea().floatValue()))
                .body("ungaged-drainage-area", equalTo(streamLocation.getUngagedDrainageArea().floatValue()))
                .body("area-units", equalTo(streamLocation.getAreaUnits()))
                .body("stage-units", equalTo(streamLocation.getStageUnits()));

        // Delete the StreamLocation
        given()
                .log().ifValidationFails(LogDetail.ALL, true)
                .accept(Formats.JSON)
                .header(AUTH_HEADER, user.toHeaderValue())
                .queryParam(OFFICE, OFFICE_ID)
                .queryParam(STREAM_ID, streamLocation.getStreamId().getName())
        .when()
                .redirects().follow(true)
                .redirects().max(3)
                .delete("/stream-locations/" + streamLocationId)
        .then()
                .log().ifValidationFails(LogDetail.ALL, true)
        .assertThat()
                .statusCode(is(HttpServletResponse.SC_NO_CONTENT));

        // Retrieve the StreamLocation and assert that it does not exist
        given()
                .log().ifValidationFails(LogDetail.ALL, true)
                .accept(Formats.JSON)
                .queryParam(NAME, streamLocationId)
                .queryParam(OFFICE, OFFICE_ID)
                .queryParam(STREAM_ID, streamLocation.getStreamId().getName())
                .queryParam(STATION_UNIT, "km")
                .queryParam(AREA_UNIT, "km2")
                .queryParam(STAGE_UNIT, "m")
        .when()
                .redirects().follow(true)
                .redirects().max(3)
                .get("/stream-locations/" + streamLocationId)
        .then()
                .log().ifValidationFails(LogDetail.ALL, true)
        .assertThat()
                .statusCode(is(HttpServletResponse.SC_NOT_FOUND));//not sure better way to capture this as a not found
    }

    @Test
    void test_update_does_not_exist() {
        TestAccounts.KeyUser user = TestAccounts.KeyUser.SPK_NORMAL;
        given()
                .log().ifValidationFails(LogDetail.ALL, true)
                .queryParam(Controllers.OFFICE, user.getOperatingOffice())
                .queryParam(Controllers.NAME, "NewBogus")
                .header(AUTH_HEADER, user.toHeaderValue())
        .when()
                .redirects().follow(true)
                .redirects().max(3)
                .put("/stream-locations/")
        .then()
                .log().ifValidationFails(LogDetail.ALL, true)
        .assertThat()
                .statusCode(is(HttpServletResponse.SC_NOT_FOUND));
    }

    @Test
    void test_delete_does_not_exist() {
        TestAccounts.KeyUser user = TestAccounts.KeyUser.SPK_NORMAL;
        given()
                .log().ifValidationFails(LogDetail.ALL, true)
                .queryParam(Controllers.OFFICE, user.getOperatingOffice())
                .queryParam(Controllers.NAME, "NewBogus")
                .queryParam(STREAM_ID, STREAMS_CREATED.get(0).getId().getName())
                .header(AUTH_HEADER, user.toHeaderValue())
        .when()
                .redirects().follow(true)
                .redirects().max(3)
                .delete("/stream-locations/")
        .then()
                .log().ifValidationFails(LogDetail.ALL, true)
        .assertThat()
                .statusCode(is(HttpServletResponse.SC_NOT_FOUND));
    }

    @Test
    void test_get_all() throws IOException {
        InputStream resource = this.getClass().getResourceAsStream("/cwms/cda/api/stream_location.json");
        assertNotNull(resource);
        String json = IOUtils.toString(resource, StandardCharsets.UTF_8);
        assertNotNull(json);
        StreamLocation streamLocation = Formats.parseContent(new ContentType(Formats.JSON), json, StreamLocation.class);

        // Structure of test:
        // 1) Create the StreamLocation
        // 2) Retrieve the StreamLocation with getAll and assert that it exists
        // 3) Delete the StreamLocation
        TestAccounts.KeyUser user = TestAccounts.KeyUser.SPK_NORMAL;

        // Create the StreamLocation
        given()
                .log().ifValidationFails(LogDetail.ALL, true)
                .contentType(Formats.JSON)
                .body(json)
                .header(AUTH_HEADER, user.toHeaderValue())
        .when()
                .redirects().follow(true)
                .redirects().max(3)
                .post("/stream-locations/")
        .then()
                .log().ifValidationFails(LogDetail.ALL, true)
        .assertThat()
                .statusCode(is(HttpServletResponse.SC_CREATED));

        String office = streamLocation.getId().getOfficeId();
        String streamLocationId = streamLocation.getId().getName();

        // Retrieve the StreamLocation and assert that it exists
        given()
                .log().ifValidationFails(LogDetail.ALL, true)
                .accept(Formats.JSON)
                .queryParam(OFFICE_MASK, office)
                .queryParam(NAME_MASK, streamLocationId)
                .queryParam(STREAM_ID_MASK, streamLocation.getStreamId().getName())
                .queryParam(STATION_UNIT, "km")
                .queryParam(AREA_UNIT, "km2")
                .queryParam(STAGE_UNIT, "m")
        .when()
                .redirects().follow(true)
                .redirects().max(3)
                .get("stream-locations/")
        .then()
                .log().ifValidationFails(LogDetail.ALL, true)
        .assertThat()
                .statusCode(is(HttpServletResponse.SC_OK))
                .body("[0].stream-location-node.id.office-id", equalTo(streamLocation.getId().getOfficeId()))
                .body("[0].stream-location-node.id.name", equalTo(streamLocation.getId().getName()))
                .body("[0].stream-location-node.stream-node.stream-id.office-id", equalTo(streamLocation.getStreamId().getOfficeId()))
                .body("[0].stream-location-node.stream-node.stream-id.name", equalTo(streamLocation.getStreamId().getName()))
                .body("[0].stream-location-node.stream-node.bank", equalTo(streamLocation.getStreamLocationNode().getStreamNode().getBank().getCode()))
                .body("[0].stream-location-node.stream-node.station", equalTo(streamLocation.getStreamLocationNode().getStreamNode().getStation().floatValue()))
                .body("[0].stream-location-node.stream-node.station-units", equalTo(streamLocation.getStreamLocationNode().getStreamNode().getStationUnits()))
                .body("[0].published-station", equalTo(streamLocation.getPublishedStation().floatValue()))
                .body("[0].navigation-station", equalTo(streamLocation.getNavigationStation().floatValue()))
                .body("[0].lowest-measurable-stage", equalTo(streamLocation.getLowestMeasurableStage().floatValue()))
                .body("[0].total-drainage-area", equalTo(streamLocation.getTotalDrainageArea().floatValue()))
                .body("[0].ungaged-drainage-area", equalTo(streamLocation.getUngagedDrainageArea().floatValue()))
                .body("[0].area-units", equalTo(streamLocation.getAreaUnits()))
                .body("[0].stage-units", equalTo(streamLocation.getStageUnits()));

        // Delete the StreamLocation
        given()
                .log().ifValidationFails(LogDetail.ALL, true)
                .queryParam(Controllers.OFFICE, office)
                .queryParam(STREAM_ID, streamLocation.getStreamId().getName())
                .header(AUTH_HEADER, user.toHeaderValue())
        .when()
                .redirects().follow(true)
                .redirects().max(3)
                .delete("stream-locations/" + streamLocationId)
        .then()
                .log().ifValidationFails(LogDetail.ALL, true)
        .assertThat()
                .statusCode(is(HttpServletResponse.SC_NO_CONTENT));
    }
}
