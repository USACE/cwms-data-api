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

import static cwms.cda.api.Controllers.*;
import cwms.cda.api.errors.NotFoundException;
import static cwms.cda.data.dao.DaoTest.getDslContext;
import cwms.cda.data.dao.DeleteRule;
import cwms.cda.data.dao.StreamDao;
import cwms.cda.data.dao.StreamLocationDao;
import cwms.cda.data.dto.CwmsId;
import cwms.cda.data.dto.stream.Bank;
import cwms.cda.data.dto.stream.Stream;
import cwms.cda.data.dto.stream.StreamLocation;
import cwms.cda.data.dto.stream.StreamLocationNode;
import cwms.cda.data.dto.stream.StreamNode;
import cwms.cda.data.dto.stream.StreamReach;
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
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
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
import static org.junit.jupiter.api.Assertions.assertNotNull;

@Tag("integration")
final class StreamReachControllerTestIT extends DataApiTestIT {

    private static final String OFFICE_ID = TestAccounts.KeyUser.SPK_NORMAL.getOperatingOffice();
    private static final List<Stream> STREAMS_CREATED = new ArrayList<>();
    private static final List<StreamLocation> STREAM_LOCATIONS_CREATED = new ArrayList<>();

    @BeforeAll
    public static void setup() throws SQLException {
        //Create setup that matches stream_reach.json
        String testLoc = "Reach123";
        createLocation(testLoc, true, OFFICE_ID, "STREAM_REACH");
        createLocation("DownstreamLoc123", true, OFFICE_ID, "STREAM_LOCATION");
        createLocation("UpstreamLoc123", true, OFFICE_ID, "STREAM_LOCATION");
        String streamId = "Stream123";
        createAndStoreTestStream(streamId);
        createAndStoreTestStreamLocation("DownstreamLoc123", streamId, 25.0, Bank.RIGHT);
        createAndStoreTestStreamLocation("UpstreamLoc123", streamId, 20.0, Bank.LEFT);
    }

    private static void createAndStoreTestStreamLocation(String testLoc, String streamId, Double station, Bank bank) throws SQLException {
        CwmsDatabaseContainer<?> db = CwmsDataApiSetupCallback.getDatabaseLink();
        db.connection(c-> {
            StreamLocationDao streamLocationDao = new StreamLocationDao(getDslContext(c, OFFICE_ID));
            StreamLocation streamLoc = new StreamLocation.Builder()
                    .withStreamLocationNode(new StreamLocationNode.Builder()
                            .withId(new CwmsId.Builder()
                                    .withOfficeId(OFFICE_ID)
                                    .withName(testLoc)
                                    .build())
                            .withStreamNode(new StreamNode.Builder()
                                    .withStreamId(new CwmsId.Builder()
                                            .withOfficeId(OFFICE_ID)
                                            .withName(streamId)
                                            .build())
                                    .withBank(bank)
                                    .withStation(station)
                                    .withStationUnits("km")
                                    .build())
                            .build())
                    .build();
            STREAM_LOCATIONS_CREATED.add(streamLoc);
            streamLocationDao.storeStreamLocation(streamLoc, false);
        }, CwmsDataApiSetupCallback.getWebUser());
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
                    } catch (NotFoundException e) {
                        // ignore
                    }
                }, CwmsDataApiSetupCallback.getWebUser());
            } catch (SQLException ex) {
                throw new RuntimeException(ex);
            }
        }
        STREAMS_CREATED.clear();

        for (StreamLocation streamLocation : STREAM_LOCATIONS_CREATED) {
            try {
                CwmsDatabaseContainer<?> db = CwmsDataApiSetupCallback.getDatabaseLink();
                db.connection(c -> {
                    StreamLocationDao streamLocationDao = new StreamLocationDao(getDslContext(c, OFFICE_ID));
                    try {
                        streamLocationDao.deleteStreamLocation(streamLocation.getStreamLocationNode().getId().getOfficeId(),
                                streamLocation.getStreamId().getName(),
                                streamLocation.getStreamLocationNode().getId().getName());
                    } catch (NotFoundException e) {
                        //ignore
                    }
                }, CwmsDataApiSetupCallback.getWebUser());
            } catch (SQLException ex) {
                throw new RuntimeException(ex);
            }
        }

        STREAM_LOCATIONS_CREATED.clear();
    }

    @Test
    void test_get_create_delete() throws IOException {
        InputStream resource = this.getClass().getResourceAsStream("/cwms/cda/api/stream_reach.json");
        assertNotNull(resource);
        String json = IOUtils.toString(resource, StandardCharsets.UTF_8);
        assertNotNull(json);
        StreamReach streamReach = Formats.parseContent(new ContentType(Formats.JSON), json, StreamReach.class);

        // Structure of test:
        // 1) Create the StreamReach
        // 2) Retrieve the StreamReach and assert that it exists
        // 3) Delete the StreamReach
        // 4) Retrieve the StreamReach and assert that it does not exist
        TestAccounts.KeyUser user = TestAccounts.KeyUser.SPK_NORMAL;

        // Create the StreamReach
        given()
                .log().ifValidationFails(LogDetail.ALL, true)
                .accept(Formats.JSON)
                .contentType(Formats.JSON)
                .body(json)
                .header(AUTH_HEADER, user.toHeaderValue())
        .when()
                .redirects().follow(true)
                .redirects().max(3)
                .post("/stream-reaches/")
        .then()
                .log().ifValidationFails(LogDetail.ALL, true)
        .assertThat()
                .statusCode(is(HttpServletResponse.SC_CREATED));

        String streamReachId = streamReach.getId().getName();

        // Retrieve the StreamReach and assert that it exists
        given()
                .log().ifValidationFails(LogDetail.ALL, true)
                .accept(Formats.JSON)
                .queryParam(NAME, streamReachId)
                .queryParam(OFFICE, OFFICE_ID)
                .queryParam(STREAM_ID, streamReach.getStreamId().getName())
                .queryParam(STATION_UNIT, "km")
        .when()
                .redirects().follow(true)
                .redirects().max(3)
                .get("/stream-reaches/" + streamReachId)
        .then()
                .log().ifValidationFails(LogDetail.ALL, true)
        .assertThat()
                .statusCode(is(HttpServletResponse.SC_OK))
                .body("id.office-id", equalTo(streamReach.getId().getOfficeId()))
                .body("id.name", equalTo(streamReach.getId().getName()))
                .body("stream-id.office-id", equalTo(streamReach.getStreamId().getOfficeId()))
                .body("stream-id.name", equalTo(streamReach.getStreamId().getName()))
                .body("comment", equalTo(streamReach.getComment()))
                .body("downstream-node.id.office-id", equalTo(streamReach.getDownstreamNode().getId().getOfficeId()))
                .body("downstream-node.id.name", equalTo(streamReach.getDownstreamNode().getId().getName()))
                .body("downstream-node.stream-node.stream-id.office-id", equalTo(streamReach.getDownstreamNode().getStreamNode().getStreamId().getOfficeId()))
                .body("downstream-node.stream-node.stream-id.name", equalTo(streamReach.getDownstreamNode().getStreamNode().getStreamId().getName()))
                .body("downstream-node.stream-node.station", equalTo(streamReach.getDownstreamNode().getStreamNode().getStation().floatValue()))
                .body("downstream-node.stream-node.bank", equalTo(streamReach.getDownstreamNode().getStreamNode().getBank().getCode()))
                .body("downstream-node.stream-node.station-units", equalTo(streamReach.getDownstreamNode().getStreamNode().getStationUnits()))
                .body("upstream-node.id.office-id", equalTo(streamReach.getUpstreamNode().getId().getOfficeId()))
                .body("upstream-node.id.name", equalTo(streamReach.getUpstreamNode().getId().getName()))
                .body("upstream-node.stream-node.stream-id.office-id", equalTo(streamReach.getUpstreamNode().getStreamNode().getStreamId().getOfficeId()))
                .body("upstream-node.stream-node.stream-id.name", equalTo(streamReach.getUpstreamNode().getStreamNode().getStreamId().getName()))
                .body("upstream-node.stream-node.station", equalTo(streamReach.getUpstreamNode().getStreamNode().getStation().floatValue()))
                .body("upstream-node.stream-node.bank", equalTo(streamReach.getUpstreamNode().getStreamNode().getBank().getCode()))
                .body("upstream-node.stream-node.station-units", equalTo(streamReach.getUpstreamNode().getStreamNode().getStationUnits()))
                .body("configuration-id.office-id", equalTo(streamReach.getConfigurationId().getOfficeId()))
                .body("configuration-id.name", equalTo(streamReach.getConfigurationId().getName()));

        // Delete the StreamReach
        given()
                .log().ifValidationFails(LogDetail.ALL, true)
                .accept(Formats.JSON)
                .header(AUTH_HEADER, user.toHeaderValue())
                .queryParam(OFFICE, OFFICE_ID)
        .when()
                .redirects().follow(true)
                .redirects().max(3)
                .delete("/stream-reaches/" + streamReachId)
        .then()
                .log().ifValidationFails(LogDetail.ALL, true)
        .assertThat()
                .statusCode(is(HttpServletResponse.SC_OK));

        // Retrieve the StreamReach and assert that it does not exist
        given()
                .log().ifValidationFails(LogDetail.ALL, true)
                .accept(Formats.JSON)
                .queryParam(NAME, streamReachId)
                .queryParam(OFFICE, OFFICE_ID)
                .queryParam(STREAM_ID, streamReach.getStreamId().getName())
                .queryParam(STATION_UNIT, "km")
        .when()
                .redirects().follow(true)
                .redirects().max(3)
                .get("/stream-reaches/" + streamReachId)
        .then()
                .log().ifValidationFails(LogDetail.ALL, true)
        .assertThat()
                .statusCode(is(HttpServletResponse.SC_NOT_FOUND));
    }

    @Test
    void test_update_does_not_exist() {
        TestAccounts.KeyUser user = TestAccounts.KeyUser.SPK_NORMAL;
        given()
                .log().ifValidationFails(LogDetail.ALL, true)
                .queryParam(OFFICE, user.getOperatingOffice())
                .queryParam(NAME, "NewBogus")
                .header(AUTH_HEADER, user.toHeaderValue())
        .when()
                .redirects().follow(true)
                .redirects().max(3)
                .put("/stream-reaches/")
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
                .queryParam(OFFICE, user.getOperatingOffice())
                .queryParam(NAME, "NewBogus")
                .queryParam(STREAM_ID_MASK, STREAMS_CREATED.get(0).getId().getName())
                .header(AUTH_HEADER, user.toHeaderValue())
        .when()
                .redirects().follow(true)
                .redirects().max(3)
                .delete("/stream-reaches/")
        .then()
                .log().ifValidationFails(LogDetail.ALL, true)
        .assertThat()
                .statusCode(is(HttpServletResponse.SC_NOT_FOUND));
    }

    @Test
    void test_get_all() throws IOException {
        InputStream resource = this.getClass().getResourceAsStream("/cwms/cda/api/stream_reach.json");
        assertNotNull(resource);
        String json = IOUtils.toString(resource, StandardCharsets.UTF_8);
        assertNotNull(json);
        StreamReach streamReach = Formats.parseContent(new ContentType(Formats.JSON), json, StreamReach.class);

        // Structure of test:
        // 1) Create the StreamReach
        // 2) Retrieve the StreamReach with getAll and assert that it exists
        // 3) Delete the StreamReach
        TestAccounts.KeyUser user = TestAccounts.KeyUser.SPK_NORMAL;

        // Create the StreamReach
        given()
                .log().ifValidationFails(LogDetail.ALL, true)
                .contentType(Formats.JSON)
                .body(json)
                .header(AUTH_HEADER, user.toHeaderValue())
        .when()
                .redirects().follow(true)
                .redirects().max(3)
                .post("/stream-reaches/")
        .then()
                .log().ifValidationFails(LogDetail.ALL, true)
        .assertThat()
                .statusCode(is(HttpServletResponse.SC_CREATED));

        String office = streamReach.getId().getOfficeId();
        String streamReachId = streamReach.getId().getName();

        // Retrieve the StreamReach and assert that it exists
        given()
                .log().ifValidationFails(LogDetail.ALL, true)
                .accept(Formats.JSON)
                .queryParam(OFFICE_MASK, office)
                .queryParam(NAME_MASK, streamReachId)
                .queryParam(STREAM_ID_MASK, streamReach.getStreamId().getName())
                .queryParam(STATION_UNIT, "km")
        .when()
                .redirects().follow(true)
                .redirects().max(3)
                .get("stream-reaches/")
        .then()
                .log().ifValidationFails(LogDetail.ALL, true)
        .assertThat()
                .statusCode(is(HttpServletResponse.SC_OK))
                .body("[0].id.office-id", equalTo(streamReach.getId().getOfficeId()))
                .body("[0].id.name", equalTo(streamReach.getId().getName()))
                .body("[0].stream-id.office-id", equalTo(streamReach.getStreamId().getOfficeId()))
                .body("[0].stream-id.name", equalTo(streamReach.getStreamId().getName()))
                .body("[0].comment", equalTo(streamReach.getComment()))
                .body("[0].downstream-node.id.office-id", equalTo(streamReach.getDownstreamNode().getId().getOfficeId()))
                .body("[0].downstream-node.id.name", equalTo(streamReach.getDownstreamNode().getId().getName()))
                .body("[0].downstream-node.stream-node.stream-id.office-id", equalTo(streamReach.getDownstreamNode().getStreamNode().getStreamId().getOfficeId()))
                .body("[0].downstream-node.stream-node.stream-id.name", equalTo(streamReach.getDownstreamNode().getStreamNode().getStreamId().getName()))
                .body("[0].downstream-node.stream-node.station", equalTo(streamReach.getDownstreamNode().getStreamNode().getStation().floatValue()))
                .body("[0].downstream-node.stream-node.bank", equalTo(streamReach.getDownstreamNode().getStreamNode().getBank().getCode()))
                .body("[0].downstream-node.stream-node.station-units", equalTo(streamReach.getDownstreamNode().getStreamNode().getStationUnits()))
                .body("[0].upstream-node.id.office-id", equalTo(streamReach.getUpstreamNode().getId().getOfficeId()))
                .body("[0].upstream-node.id.name", equalTo(streamReach.getUpstreamNode().getId().getName()))
                .body("[0].upstream-node.stream-node.stream-id.office-id", equalTo(streamReach.getUpstreamNode().getStreamNode().getStreamId().getOfficeId()))
                .body("[0].upstream-node.stream-node.stream-id.name", equalTo(streamReach.getUpstreamNode().getStreamNode().getStreamId().getName()))
                .body("[0].upstream-node.stream-node.station", equalTo(streamReach.getUpstreamNode().getStreamNode().getStation().floatValue()))
                .body("[0].upstream-node.stream-node.bank", equalTo(streamReach.getUpstreamNode().getStreamNode().getBank().getCode()))
                .body("[0].upstream-node.stream-node.station-units", equalTo(streamReach.getUpstreamNode().getStreamNode().getStationUnits()))
                .body("[0].configuration-id.office-id", equalTo(streamReach.getConfigurationId().getOfficeId()))
                .body("[0].configuration-id.name", equalTo(streamReach.getConfigurationId().getName()));

        // Delete the StreamReach
        given()
                .log().ifValidationFails(LogDetail.ALL, true)
                .queryParam(Controllers.OFFICE, office)
                .queryParam(STREAM_ID_MASK, streamReach.getStreamId().getName())
                .header(AUTH_HEADER, user.toHeaderValue())
        .when()
                .redirects().follow(true)
                .redirects().max(3)
                .delete("/stream-reaches/" + streamReachId)
        .then()
                .log().ifValidationFails(LogDetail.ALL, true)
        .assertThat()
                .statusCode(is(HttpServletResponse.SC_OK));

        //verify deletion
        given()
                .log().ifValidationFails(LogDetail.ALL, true)
                .accept(Formats.JSON)
                .queryParam(NAME, streamReachId)
                .queryParam(OFFICE, OFFICE_ID)
                .queryParam(STREAM_ID, streamReach.getStreamId().getName())
                .queryParam(STATION_UNIT, "km")
        .when()
                .redirects().follow(true)
                .redirects().max(3)
                .get("/stream-reaches/" + streamReachId)
        .then()
                .log().ifValidationFails(LogDetail.ALL, true)
        .assertThat()
                .statusCode(is(HttpServletResponse.SC_NOT_FOUND));
    }
}
