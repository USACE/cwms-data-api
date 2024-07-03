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

import static cwms.cda.api.Controllers.NAME;
import static cwms.cda.api.Controllers.OFFICE;
import cwms.cda.api.errors.NotFoundException;
import static cwms.cda.data.dao.DaoTest.getDslContext;
import cwms.cda.data.dao.DeleteRule;
import cwms.cda.data.dao.StreamDao;
import cwms.cda.data.dto.CwmsId;
import cwms.cda.data.dto.stream.Stream;
import cwms.cda.formatters.ContentType;
import cwms.cda.formatters.Formats;
import fixtures.CwmsDataApiSetupCallback;
import fixtures.TestAccounts;
import io.restassured.filter.log.LogDetail;
import java.sql.SQLException;
import java.time.Instant;
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
final class StreamControllerTestIT extends DataApiTestIT {

    private static final String OFFICE_ID = TestAccounts.KeyUser.SPK_NORMAL.getOperatingOffice();
    private static final List<Stream> STREAMS_CREATED = new ArrayList<>();

    @BeforeAll
    public static void setup() throws SQLException {
        String testLoc = "Stream123Test"; //match the stream name in the json file
        createLocation(testLoc, true, OFFICE_ID, "STREAM");
        createAndStoreTestStream("DownstreamStream123");
        createAndStoreTestStream("UpstreamStream123");
    }

    private static void createAndStoreTestStream(String testLoc) throws SQLException {
        createLocation(testLoc, true, OFFICE_ID, "STREAM");
        CwmsDatabaseContainer<?> db = CwmsDataApiSetupCallback.getDatabaseLink();
        db.connection(c-> {
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
        for(Stream stream : STREAMS_CREATED){
            try {
                CwmsDatabaseContainer<?> db = CwmsDataApiSetupCallback.getDatabaseLink();
                db.connection(c-> {
                    StreamDao streamDao = new StreamDao(getDslContext(c, OFFICE_ID));
                    try {
                        streamDao.deleteStream(stream.getId().getOfficeId(), stream.getId().getName(), DeleteRule.DELETE_ALL);
                    } catch (NotFoundException e) {
                        //ignore
                    }
                });
            } catch(SQLException ex) {
                throw new RuntimeException(ex);
            }
        }
        STREAMS_CREATED.clear();
    }

    @Test
    void test_get_create_delete() throws IOException {
        InputStream resource = this.getClass().getResourceAsStream("/cwms/cda/api/stream.json");
        assertNotNull(resource);
        String json = IOUtils.toString(resource, StandardCharsets.UTF_8);
        assertNotNull(json);
        Stream stream = Formats.parseContent(new ContentType(Formats.JSON), json, Stream.class);

        // Structure of test:
        // 1) Create the Stream
        // 2) Retrieve the Stream and assert that it exists
        // 3) Delete the Stream
        // 4) Retrieve the Stream and assert that it does not exist
        TestAccounts.KeyUser user = TestAccounts.KeyUser.SPK_NORMAL;

        // Create the Stream
        given()
                .log().ifValidationFails(LogDetail.ALL, true)
                .accept(Formats.JSON)
                .contentType(Formats.JSON)
                .body(json)
                .header(AUTH_HEADER, user.toHeaderValue())
        .when()
                .redirects().follow(true)
                .redirects().max(3)
                .post("/streams/")
        .then()
                .log().ifValidationFails(LogDetail.ALL, true)
        .assertThat()
                .statusCode(is(HttpServletResponse.SC_CREATED));

        String streamId = stream.getId().getName();

        // Retrieve the Stream and assert that it exists
        given()
                .log().ifValidationFails(LogDetail.ALL, true)
                .accept(Formats.JSON)
                .queryParam(NAME, streamId)
                .queryParam(OFFICE, OFFICE_ID)
        .when()
                .redirects().follow(true)
                .redirects().max(3)
                .get("/streams/" + streamId)
        .then()
                .log().ifValidationFails(LogDetail.ALL, true)
        .assertThat()
                .statusCode(is(HttpServletResponse.SC_OK))
                .body("starts-downstream", equalTo(true))
                .body("flows-into-stream-node.stream-id.office-id", equalTo(stream.getFlowsIntoStreamNode().getStreamId().getOfficeId()))
                .body("flows-into-stream-node.stream-id.name", equalTo(stream.getFlowsIntoStreamNode().getStreamId().getName()))
                .body("flows-into-stream-node.station", equalTo(stream.getFlowsIntoStreamNode().getStation().floatValue()))
                .body("flows-into-stream-node.bank", equalTo(stream.getFlowsIntoStreamNode().getBank().getCode()))
                .body("flows-into-stream-node.station-units", equalTo(stream.getFlowsIntoStreamNode().getStationUnits()))
                .body("diverts-from-stream-node.stream-id.office-id", equalTo(stream.getDivertsFromStreamNode().getStreamId().getOfficeId()))
                .body("diverts-from-stream-node.stream-id.name", equalTo(stream.getDivertsFromStreamNode().getStreamId().getName()))
                .body("diverts-from-stream-node.station", equalTo(stream.getDivertsFromStreamNode().getStation().floatValue()))
                .body("diverts-from-stream-node.bank", equalTo(stream.getDivertsFromStreamNode().getBank().getCode()))
                .body("diverts-from-stream-node.station-units", equalTo(stream.getDivertsFromStreamNode().getStationUnits()))
                .body("length", equalTo(stream.getLength().floatValue()))
                .body("average-slope", equalTo(stream.getAverageSlope().floatValue()))
                .body("length-units", equalTo(stream.getLengthUnits()))
                .body("slope-units", equalTo(stream.getSlopeUnits()))
                .body("comment", equalTo(stream.getComment()))
                .body("id.office-id", equalTo(stream.getOfficeId()))
                .body("id.name", equalTo(stream.getId().getName()));

        // Delete the Stream
        given()
                .log().ifValidationFails(LogDetail.ALL, true)
                .accept(Formats.JSON)
                .header(AUTH_HEADER, user.toHeaderValue())
                .queryParam(OFFICE, OFFICE_ID)
        .when()
                .redirects().follow(true)
                .redirects().max(3)
                .delete("/streams/" + streamId)
        .then()
                .log().ifValidationFails(LogDetail.ALL, true)
        .assertThat()
                .statusCode(is(HttpServletResponse.SC_NO_CONTENT));

        // Retrieve the Stream and assert that it does not exist
        given()
                .log().ifValidationFails(LogDetail.ALL, true)
                .accept(Formats.JSON)
                .queryParam(NAME, streamId)
                .queryParam(OFFICE, OFFICE_ID)
        .when()
                .redirects().follow(true)
                .redirects().max(3)
                .get("/streams/" + streamId)
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
                .queryParam(Controllers.OFFICE, user.getOperatingOffice())
                .queryParam(Controllers.NAME, "NewBogus")
                .header(AUTH_HEADER, user.toHeaderValue())
        .when()
                .redirects().follow(true)
                .redirects().max(3)
                .patch("/streams/bogus")
        .then()
                .log().ifValidationFails(LogDetail.ALL, true)
        .assertThat()
                .statusCode(is(HttpServletResponse.SC_NOT_FOUND));
    }

    @Test
    void test_delete_does_not_exist() {
        TestAccounts.KeyUser user = TestAccounts.KeyUser.SPK_NORMAL;
        // Delete a Embankment
        given()
                .log().ifValidationFails(LogDetail.ALL,true)
                .queryParam(Controllers.OFFICE, user.getOperatingOffice())
                .header(AUTH_HEADER, user.toHeaderValue())
        .when()
                .redirects().follow(true)
                .redirects().max(3)
                .delete("streams/" + Instant.now().toEpochMilli())
        .then()
                .log().ifValidationFails(LogDetail.ALL,true)
        .assertThat()
                .statusCode(is(HttpServletResponse.SC_NOT_FOUND));
    }

    @Test
    void test_get_all() throws IOException {
        InputStream resource = this.getClass().getResourceAsStream("/cwms/cda/api/stream.json");
        assertNotNull(resource);
        String json = IOUtils.toString(resource, StandardCharsets.UTF_8);
        assertNotNull(json);
        Stream stream = Formats.parseContent(new ContentType(Formats.JSON), json, Stream.class);

        // Structure of test:
        // 1) Create the Stream
        // 2) Retrieve the Stream with getAll and assert that it exists
        // 3) Delete the Stream
        TestAccounts.KeyUser user = TestAccounts.KeyUser.SPK_NORMAL;

        // Create the Stream
        given()
                .log().ifValidationFails(LogDetail.ALL, true)
                .contentType(Formats.JSON)
                .body(json)
                .header(AUTH_HEADER, user.toHeaderValue())
        .when()
                .redirects().follow(true)
                .redirects().max(3)
                .post("/streams/")
        .then()
                .log().ifValidationFails(LogDetail.ALL, true)
        .assertThat()
                .statusCode(is(HttpServletResponse.SC_CREATED));

        String office = stream.getId().getOfficeId();
        String streamId = stream.getId().getName();

        // Retrieve the Stream and assert that it exists
        given()
                .log().ifValidationFails(LogDetail.ALL, true)
                .accept(Formats.JSON)
                .queryParam(Controllers.OFFICE, office)
                .queryParam(NAME, streamId)
       .when()
                .redirects().follow(true)
                .redirects().max(3)
                .get("streams/")
       .then()
                .log().ifValidationFails(LogDetail.ALL, true)
       .assertThat()
                .statusCode(is(HttpServletResponse.SC_OK))
                .body("[0].starts-downstream", equalTo(true))
                .body("[0].flows-into-stream-node.stream-id.office-id", equalTo(stream.getFlowsIntoStreamNode().getStreamId().getOfficeId()))
                .body("[0].flows-into-stream-node.stream-id.name", equalTo(stream.getFlowsIntoStreamNode().getStreamId().getName()))
                .body("[0].flows-into-stream-node.station", equalTo(stream.getFlowsIntoStreamNode().getStation().floatValue()))
                .body("[0].flows-into-stream-node.bank", equalTo(stream.getFlowsIntoStreamNode().getBank().getCode()))
                .body("[0].flows-into-stream-node.station-units", equalTo(stream.getFlowsIntoStreamNode().getStationUnits()))
                .body("[0].diverts-from-stream-node.stream-id.office-id", equalTo(stream.getDivertsFromStreamNode().getStreamId().getOfficeId()))
                .body("[0].diverts-from-stream-node.stream-id.name", equalTo(stream.getDivertsFromStreamNode().getStreamId().getName()))
                .body("[0].diverts-from-stream-node.station", equalTo(stream.getDivertsFromStreamNode().getStation().floatValue()))
                .body("[0].diverts-from-stream-node.bank", equalTo(stream.getDivertsFromStreamNode().getBank().getCode()))
                .body("[0].diverts-from-stream-node.station-units", equalTo(stream.getDivertsFromStreamNode().getStationUnits()))
                .body("[0].length", equalTo(stream.getLength().floatValue()))
                .body("[0].average-slope", equalTo(stream.getAverageSlope().floatValue()))
                .body("[0].length-units", equalTo(stream.getLengthUnits()))
                .body("[0].slope-units", equalTo(stream.getSlopeUnits()))
                .body("[0].comment", equalTo(stream.getComment()))
                .body("[0].id.office-id", equalTo(stream.getOfficeId()))
                .body("[0].id.name", equalTo(stream.getId().getName()));

        // Delete the Stream
        given()
                .log().ifValidationFails(LogDetail.ALL, true)
                .queryParam(Controllers.OFFICE, office)
                .header(AUTH_HEADER, user.toHeaderValue())
        .when()
                .redirects().follow(true)
                .redirects().max(3)
                .delete("streams/" + streamId)
        .then()
                .log().ifValidationFails(LogDetail.ALL, true)
        .assertThat()
                .statusCode(is(HttpServletResponse.SC_NO_CONTENT));
    }

}