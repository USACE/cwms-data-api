package cwms.cda.api;

import static cwms.cda.api.Controllers.ALL_UPSTREAM;
import static cwms.cda.api.Controllers.AREA_UNIT;
import static cwms.cda.api.Controllers.FAIL_IF_EXISTS;
import static cwms.cda.api.Controllers.NAME;
import static cwms.cda.api.Controllers.OFFICE;
import static cwms.cda.api.Controllers.SAME_STREAM_ONLY;
import static cwms.cda.api.Controllers.STAGE_UNIT;
import static cwms.cda.api.Controllers.STATION_UNIT;
import static cwms.cda.api.Controllers.STREAM_ID;
import static cwms.cda.data.dao.DaoTest.getDslContext;
import cwms.cda.data.dao.DeleteRule;
import cwms.cda.data.dao.StreamDao;
import cwms.cda.data.dto.CwmsId;
import cwms.cda.data.dto.stream.Stream;
import cwms.cda.data.dto.stream.StreamLocation;
import cwms.cda.formatters.ContentType;
import cwms.cda.formatters.Formats;
import static cwms.cda.security.KeyAccessManager.AUTH_HEADER;
import fixtures.CwmsDataApiSetupCallback;
import fixtures.TestAccounts;
import static io.restassured.RestAssured.given;
import io.restassured.filter.log.LogDetail;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import javax.servlet.http.HttpServletResponse;
import mil.army.usace.hec.test.database.CwmsDatabaseContainer;
import org.apache.commons.io.IOUtils;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import org.junit.jupiter.api.AfterAll;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag("integration")
final class UpstreamLocationsGetControllerIT extends DataApiTestIT {

    private static final String OFFICE_ID = TestAccounts.KeyUser.SWT_NORMAL.getOperatingOffice();
    private static final List<Stream> STREAMS_CREATED = new ArrayList<>();

    @BeforeAll
    public static void setup() throws SQLException {
        createLocation("StreamLoc1234", true, OFFICE_ID, "STREAM_LOCATION");
        createLocation("StreamLoc23", true, OFFICE_ID, "STREAM_LOCATION");
        createAndStoreTestStream("ImOnThisStream3");
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
                    .withStartsDownstream(true)
                    .build();
            STREAMS_CREATED.add(streamToStore);
            streamDao.storeStream(streamToStore, false);
        }, CwmsDataApiSetupCallback.getWebUser());
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
                    } catch (Exception e) {
                        // ignore
                    }
                }, CwmsDataApiSetupCallback.getWebUser());
            } catch (SQLException ex) {
                throw new RuntimeException(ex);
            }
        }
        STREAMS_CREATED.clear();
    }

    @Test
    void test_getUpstreamLocations() throws IOException {
        InputStream resource = this.getClass().getResourceAsStream("/cwms/cda/api/stream_location4.json");
        assertNotNull(resource);
        String json = IOUtils.toString(resource, StandardCharsets.UTF_8);
        assertNotNull(json);
        StreamLocation streamLocation = Formats.parseContent(new ContentType(Formats.JSON), json, StreamLocation.class);

        // Structure of test:
        // 1) Create the StreamLocation
        // 2) Retrieve the StreamLocation and assert that it exists
        // 3) Retrieve upstream locations and assert they exist
        // 4) Delete the StreamLocation

        TestAccounts.KeyUser user = TestAccounts.KeyUser.SWT_NORMAL;

        // Create the StreamLocation
        given()
                .log().ifValidationFails(LogDetail.ALL, true)
                .accept(Formats.JSON)
                .queryParam(FAIL_IF_EXISTS, false)
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

        resource = this.getClass().getResourceAsStream("/cwms/cda/api/stream_location5.json");
        assertNotNull(resource);
        json = IOUtils.toString(resource, StandardCharsets.UTF_8);
        assertNotNull(json);
        StreamLocation streamLocation2 = Formats.parseContent(new ContentType(Formats.JSON), json, StreamLocation.class);

        String streamLocationId2 = streamLocation2.getId().getName();

        // Create the StreamLocation2
        given()
                .log().ifValidationFails(LogDetail.ALL, true)
                .accept(Formats.JSON)
                .queryParam(FAIL_IF_EXISTS, false)
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

        // Retrieve the StreamLocation2 and assert that it exists
        given()
                .log().ifValidationFails(LogDetail.ALL, true)
                .accept(Formats.JSON)
                .queryParam(NAME, streamLocationId2)
                .queryParam(OFFICE, OFFICE_ID)
                .queryParam(STREAM_ID, streamLocation2.getStreamId().getName())
                .queryParam(STATION_UNIT, "km")
                .queryParam(AREA_UNIT, "km2")
                .queryParam(STAGE_UNIT, "m")
        .when()
                .redirects().follow(true)
                .redirects().max(3)
                .get("/stream-locations/" + streamLocationId2)
        .then()
                .log().ifValidationFails(LogDetail.ALL, true)
        .assertThat()
                .statusCode(is(HttpServletResponse.SC_OK))
                .body("stream-location-node.id.office-id", equalTo(streamLocation2.getId().getOfficeId()))
                .body("stream-location-node.id.name", equalTo(streamLocation2.getId().getName()))
                .body("stream-location-node.stream-node.stream-id.office-id", equalTo(streamLocation2.getStreamId().getOfficeId()))
                .body("stream-location-node.stream-node.stream-id.name", equalTo(streamLocation2.getStreamId().getName()))
                .body("stream-location-node.stream-node.bank", equalTo(streamLocation2.getStreamLocationNode().getStreamNode().getBank().getCode()))
                .body("stream-location-node.stream-node.station", equalTo(streamLocation2.getStreamLocationNode().getStreamNode().getStation().floatValue()))
                .body("stream-location-node.stream-node.station-units", equalTo(streamLocation2.getStreamLocationNode().getStreamNode().getStationUnits()))
                .body("published-station", equalTo(streamLocation2.getPublishedStation().floatValue()))
                .body("navigation-station", equalTo(streamLocation2.getNavigationStation().floatValue()))
                .body("lowest-measurable-stage", equalTo(streamLocation2.getLowestMeasurableStage().floatValue()))
                .body("total-drainage-area", equalTo(streamLocation2.getTotalDrainageArea().floatValue()))
                .body("ungaged-drainage-area", equalTo(streamLocation2.getUngagedDrainageArea().floatValue()))
                .body("area-units", equalTo(streamLocation2.getAreaUnits()))
                .body("stage-units", equalTo(streamLocation2.getStageUnits()));

        // Retrieve upstream locations and assert they exist
        given()
                .log().ifValidationFails(LogDetail.ALL, true)
                .accept(Formats.JSON)
                .queryParam(ALL_UPSTREAM, true)
                .queryParam(SAME_STREAM_ONLY, true)
                .queryParam(STATION_UNIT, "km")
                .queryParam(AREA_UNIT, "km2")
                .queryParam(STAGE_UNIT, "m")
        .when()
                .redirects().follow(true)
                .redirects().max(3)
                .get("/stream-locations/" + streamLocation2.getId().getOfficeId() + "/" + streamLocationId2 + "/upstream-locations/")
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
                .accept(Formats.JSON)
                .queryParam(OFFICE, OFFICE_ID)
                .queryParam(STREAM_ID, streamLocationId)
                .contentType(Formats.JSON)
                .header(AUTH_HEADER, user.toHeaderValue())
        .when()
                .redirects().follow(true)
                .redirects().max(3)
                .delete("/stream-locations/" + streamLocationId)
        .then()
                .log().ifValidationFails(LogDetail.ALL, true)
        .assertThat()
                .statusCode(is(HttpServletResponse.SC_NO_CONTENT));

        // Delete the StreamLocation2
        given()
                .log().ifValidationFails(LogDetail.ALL, true)
                .accept(Formats.JSON)
                .queryParam(OFFICE, OFFICE_ID)
                .queryParam(STREAM_ID, streamLocationId2)
                .contentType(Formats.JSON)
                .header(AUTH_HEADER, user.toHeaderValue())
        .when()
                .redirects().follow(true)
                .redirects().max(3)
                .delete("/stream-locations/" + streamLocationId2)
        .then()
                .log().ifValidationFails(LogDetail.ALL, true)
        .assertThat()
                .statusCode(is(HttpServletResponse.SC_NO_CONTENT));
    }
}
