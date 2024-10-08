/*
 *
 * MIT License
 *
 * Copyright (c) 2024 Hydrologic Engineering Center
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 *  to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
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
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER
 * DEALINGS IN THE
 * SOFTWARE.
 */

package cwms.cda.api;

import cwms.cda.api.errors.NotFoundException;
import cwms.cda.data.dao.timeseriesprofile.TimeSeriesProfileDao;
import cwms.cda.data.dao.timeseriesprofile.TimeSeriesProfileParserDao;
import cwms.cda.data.dto.CwmsId;
import cwms.cda.data.dto.timeseriesprofile.TimeSeriesProfile;
import cwms.cda.data.dto.timeseriesprofile.TimeSeriesProfileParserColumnar;
import cwms.cda.data.dto.timeseriesprofile.TimeSeriesProfileParserIndexed;
import cwms.cda.formatters.ContentType;
import cwms.cda.formatters.Formats;
import fixtures.CwmsDataApiSetupCallback;
import fixtures.TestAccounts;
import io.restassured.filter.log.LogDetail;
import mil.army.usace.hec.test.database.CwmsDatabaseContainer;
import org.apache.commons.io.IOUtils;
import org.jooq.DSLContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import javax.servlet.http.HttpServletResponse;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;

import static cwms.cda.api.Controllers.*;
import static cwms.cda.security.KeyAccessManager.AUTH_HEADER;
import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertNotNull;

@Tag("integration")
final class TimeSeriesProfileParserControllerIT extends DataApiTestIT {
    private static final String OFFICE_ID = "SPK";
    private static final TestAccounts.KeyUser user = TestAccounts.KeyUser.SPK_NORMAL;
    private final InputStream resourceIndexed = this.getClass()
            .getResourceAsStream("/cwms/cda/api/timeseriesprofile/ts_profile_parser_indexed.json");
    private final InputStream resourceColumnar = this.getClass()
            .getResourceAsStream("/cwms/cda/api/timeseriesprofile/ts_profile_parser_columnar.json");
    private final InputStream resource = this.getClass()
            .getResourceAsStream("/cwms/cda/api/timeseriesprofile/ts_profile.json");
    private TimeSeriesProfileParserIndexed tspParserIndexed;
    private TimeSeriesProfileParserColumnar tspParserColumnar;
    private TimeSeriesProfile tsProfile;
    private TimeSeriesProfile tspModified;
    private String tsDataIndexed;
    private String tsDataColumnar;


    @BeforeEach
    public void setup() throws Exception {
        assertNotNull(resourceIndexed);
        assertNotNull(resource);
        assertNotNull(resourceColumnar);
        tsDataIndexed = IOUtils.toString(resourceIndexed, StandardCharsets.UTF_8);
        tsDataColumnar = IOUtils.toString(resourceColumnar, StandardCharsets.UTF_8);
        String tsData = IOUtils.toString(resource, StandardCharsets.UTF_8);
        assertNotNull(tsData);
        tspParserIndexed = Formats.parseContent(Formats.parseHeader(Formats.JSONV1,
                TimeSeriesProfileParserIndexed.class), tsDataIndexed, TimeSeriesProfileParserIndexed.class);
        tspParserColumnar = Formats.parseContent(Formats.parseHeader(Formats.JSONV1,
                TimeSeriesProfileParserColumnar.class), tsDataColumnar, TimeSeriesProfileParserColumnar.class);
        tsProfile = Formats.parseContent(Formats.parseHeader(Formats.JSONV1,
                TimeSeriesProfile.class), tsData, TimeSeriesProfile.class);
        tspModified = new TimeSeriesProfile.Builder()
                .withDescription(tsProfile.getDescription())
                .withKeyParameter(tsProfile.getKeyParameter())
                .withLocationId(new CwmsId.Builder().withOfficeId(OFFICE_ID).withName("NEW NAME").build())
                .withParameterList(tsProfile.getParameterList()).build();
        assertNotNull(tsProfile);
        assertNotNull(tspModified);
        assertNotNull(tsDataIndexed);
        assertNotNull(tspParserIndexed);
        assertNotNull(tsDataColumnar);
        assertNotNull(tspParserColumnar);
        createLocation(tspParserIndexed.getLocationId().getName(), true, OFFICE_ID, "SITE");
        createLocation("NEW NAME", true, OFFICE_ID, "SITE");
        CwmsDatabaseContainer<?> db = CwmsDataApiSetupCallback.getDatabaseLink();
        db.connection(c -> {
            TimeSeriesProfileDao dao = new TimeSeriesProfileDao(dslContext(c, OFFICE_ID));
            dao.storeTimeSeriesProfile(tsProfile, false);
            dao.storeTimeSeriesProfile(tspModified, false);
        }, CwmsDataApiSetupCallback.getWebUser());
    }

    @AfterEach
    public void tearDown() throws Exception {
        cleanupParser(tspParserIndexed.getLocationId().getName(), tspParserIndexed.getKeyParameter());
        cleanupTS(tsProfile.getLocationId().getName(), tsProfile.getKeyParameter());
        cleanupTS(tspModified.getLocationId().getName(), tspModified.getKeyParameter());
    }

    @Test
    void test_create_retrieve_TimeSeriesProfileParser_Indexed() {
        // Create a Time Series Profile Parser
        given()
            .log().ifValidationFails(LogDetail.ALL, true)
            .accept(Formats.JSONV1)
            .contentType(Formats.JSONV1)
            .body(tsDataIndexed)
            .header(AUTH_HEADER, user.toHeaderValue())
            .queryParam(FAIL_IF_EXISTS, false)
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .post("/timeseries/profile-parser")
        .then()
            .log().ifValidationFails(LogDetail.ALL, true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_CREATED))
        ;

        // Retrieve the Time Series Profile Parser
        given()
            .log().ifValidationFails(LogDetail.ALL, true)
            .accept(Formats.JSONV1)
            .header(AUTH_HEADER, user.toHeaderValue())
            .queryParam(OFFICE, OFFICE_ID)
            .queryParam(LOCATION_ID, tspParserIndexed.getLocationId().getName())
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .get("/timeseries/profile-parser/" + tspParserIndexed.getLocationId().getName() + "/" + tspParserIndexed.getKeyParameter())
        .then()
            .log().ifValidationFails(LogDetail.ALL, true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_OK))
            .body("location-id.name", is(tspParserIndexed.getLocationId().getName()))
            .body("key-parameter", is(tspParserIndexed.getKeyParameter()))
            .body("location-id.name", equalTo(tspParserIndexed.getLocationId().getName()))
            .body("location-id.office-id", equalTo(tspParserIndexed.getLocationId().getOfficeId()))
            .body("key-parameter", equalTo(tspParserIndexed.getKeyParameter()))
            .body("time-format", equalTo(tspParserIndexed.getTimeFormat()))
            .body("time-zone", equalTo(tspParserIndexed.getTimeZone()))
            .body("parameter-info-list[0].parameter", equalTo(tspParserIndexed.getKeyParameter()))
        ;
    }

    @Test
    void test_create_retrieve_TimeSeriesProfileParser_Columnar() {
        // Create a Time Series Profile Parser
        given()
            .log().ifValidationFails(LogDetail.ALL, true)
            .accept(Formats.JSONV1)
            .contentType(Formats.JSONV1)
            .body(tsDataColumnar)
            .header(AUTH_HEADER, user.toHeaderValue())
            .queryParam(FAIL_IF_EXISTS, false)
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .post("/timeseries/profile-parser")
        .then()
            .log().ifValidationFails(LogDetail.ALL, true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_CREATED))
        ;

        // Retrieve the Time Series Profile Parser
        given()
            .log().ifValidationFails(LogDetail.ALL, true)
            .accept(Formats.JSONV1)
            .header(AUTH_HEADER, user.toHeaderValue())
            .queryParam(OFFICE, OFFICE_ID)
            .queryParam(LOCATION_ID, tspParserColumnar.getLocationId().getName())
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .get("/timeseries/profile-parser/" + tspParserIndexed.getLocationId().getName() + "/" + tspParserColumnar.getKeyParameter())
        .then()
            .log().ifValidationFails(LogDetail.ALL, true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_OK))
            .body("location-id.name", equalTo(tspParserColumnar.getLocationId().getName()))
            .body("location-id.office-id", equalTo(tspParserColumnar.getLocationId().getOfficeId()))
            .body("key-parameter", equalTo(tspParserColumnar.getKeyParameter()))
            .body("time-format", equalTo(tspParserColumnar.getTimeFormat()))
            .body("time-zone", equalTo(tspParserColumnar.getTimeZone()))
            .body("parameter-info-list[0].parameter", equalTo(tspParserColumnar.getKeyParameter()))
        ;
    }

    @Test
    void test_delete_TimeSeriesProfileParser() {
        // Create a Time Series Profile Parser
        given()
            .log().ifValidationFails(LogDetail.ALL, true)
            .accept(Formats.JSONV1)
            .contentType(Formats.JSONV1)
            .body(tsDataIndexed)
            .header(AUTH_HEADER, user.toHeaderValue())
            .queryParam(FAIL_IF_EXISTS, false)
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .post("/timeseries/profile-parser")
        .then()
            .log().ifValidationFails(LogDetail.ALL, true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_CREATED))
        ;

        // Delete the Time Series Profile Parser
        given()
            .log().ifValidationFails(LogDetail.ALL, true)
            .accept(Formats.JSONV1)
            .header(AUTH_HEADER, user.toHeaderValue())
            .queryParam(OFFICE, OFFICE_ID)
            .queryParam(LOCATION_ID, tspParserIndexed.getLocationId().getName())
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .delete("/timeseries/profile-parser/" + tspParserIndexed.getLocationId().getName() + "/" + tspParserIndexed.getKeyParameter())
        .then()
            .log().ifValidationFails(LogDetail.ALL, true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_NO_CONTENT))
        ;

        // Attempt to retrieve the Time Series Profile Parser and assert it does not exist
        given()
            .log().ifValidationFails(LogDetail.ALL, true)
            .accept(Formats.JSONV1)
            .header(AUTH_HEADER, user.toHeaderValue())
            .queryParam(OFFICE, OFFICE_ID)
            .queryParam(LOCATION_ID, tspParserIndexed.getLocationId().getName())
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .get("/timeseries/profile-parser/" + tspParserIndexed.getLocationId().getName() + "/" + tspParserIndexed.getKeyParameter())
        .then()
            .log().ifValidationFails(LogDetail.ALL, true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_NOT_FOUND))
        ;
    }

    @Test
    void test_delete_nonExistent_TimeSeriesProfileParser() {
        // Delete the Time Series Profile Parser
        given()
            .log().ifValidationFails(LogDetail.ALL, true)
            .accept(Formats.JSONV1)
            .header(AUTH_HEADER, user.toHeaderValue())
            .queryParam(OFFICE, OFFICE_ID)
            .queryParam(LOCATION_ID, "non existent location")
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .delete("/timeseries/profile-parser/" + tspParserIndexed.getLocationId().getName() + "/" + tspParserIndexed.getKeyParameter())
        .then()
            .log().ifValidationFails(LogDetail.ALL, true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_NOT_FOUND))
        ;
    }

    @Test
    void test_get_all_TimeSeriesProfileParser() throws Exception {
        TimeSeriesProfileParserIndexed tspIndex
                = (TimeSeriesProfileParserIndexed) new TimeSeriesProfileParserIndexed.Builder()
                .withTimeField(tspParserIndexed.getTimeField().intValue())
                .withFieldDelimiter(tspParserIndexed.getFieldDelimiter())
                .withTimeFormat(tspParserIndexed.getTimeFormat())
                .withRecordDelimiter(tspParserIndexed.getRecordDelimiter())
                .withTimeInTwoFields(tspParserIndexed.getTimeInTwoFields())
                .withKeyParameter(tspParserIndexed.getKeyParameter())
                .withLocationId(new CwmsId.Builder().withOfficeId(OFFICE_ID).withName("NEW NAME").build())
                .withTimeZone(tspParserIndexed.getTimeZone())
                .withParameterInfoList(tspParserIndexed.getParameterInfoList())
                .build();
        createLocation(tspIndex.getLocationId().getName(), true, OFFICE_ID, "SITE");
        ContentType contentType = Formats.parseHeader(Formats.JSONV1, TimeSeriesProfileParserIndexed.class);
        String tspDataInd = Formats.format(contentType, tspIndex);

        // Create a Time Series Profile Parser
        given()
            .log().ifValidationFails(LogDetail.ALL, true)
            .accept(Formats.JSONV1)
            .contentType(Formats.JSONV1)
            .body(tspDataInd)
            .header(AUTH_HEADER, user.toHeaderValue())
            .queryParam(FAIL_IF_EXISTS, false)
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .post("/timeseries/profile-parser")
        .then()
            .log().ifValidationFails(LogDetail.ALL, true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_CREATED))
        ;

        given()
            .log().ifValidationFails(LogDetail.ALL, true)
            .accept(Formats.JSONV1)
            .contentType(Formats.JSONV1)
            .body(tsDataIndexed)
            .header(AUTH_HEADER, user.toHeaderValue())
            .queryParam(FAIL_IF_EXISTS, false)
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .post("/timeseries/profile-parser")
        .then()
            .log().ifValidationFails(LogDetail.ALL, true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_CREATED))
        ;

        // Get all Time Series Profile Parsers
        given()
            .log().ifValidationFails(LogDetail.ALL, true)
            .accept(Formats.JSONV1)
            .header(AUTH_HEADER, user.toHeaderValue())
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .get("/timeseries/profile-parser/")
        .then()
            .log().ifValidationFails(LogDetail.ALL, true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_OK))
            .body("[0].location-id.name", equalTo(tspIndex.getLocationId().getName()))
            .body("[0].key-parameter", equalTo(tspIndex.getKeyParameter()))
            .body("[0].time-format", equalTo(tspIndex.getTimeFormat()))
            .body("[0].time-zone", equalTo(tspIndex.getTimeZone()))
            .body("[0].parameter-info-list[0].parameter", equalTo(tspIndex.getParameterInfoList().get(1).getParameter()))
            .body("[0].parameter-info-list[1].parameter", equalTo(tspIndex.getParameterInfoList().get(0).getParameter()))
            .body("[0].location-id.office-id", equalTo(tspIndex.getLocationId().getOfficeId()))
            .body("[0].type", equalTo("indexed-timeseries-profile-parser"))
        ;

        cleanupParser(tspIndex.getLocationId().getName(), tspIndex.getKeyParameter());
    }

    private void cleanupParser(String locationId, String parameterId) throws Exception {
        try {
            CwmsDatabaseContainer<?> db = CwmsDataApiSetupCallback.getDatabaseLink();
            db.connection(c -> {
                DSLContext dsl = dslContext(c, OFFICE_ID);
                TimeSeriesProfileParserDao dao = new TimeSeriesProfileParserDao(dsl);
                dao.deleteTimeSeriesProfileParser(locationId, parameterId, OFFICE_ID);
            }, CwmsDataApiSetupCallback.getWebUser());
        } catch (NotFoundException e) {
            // Ignore
        }
    }

    private void cleanupTS(String locationId, String keyParameter) throws Exception {
        try {
            CwmsDatabaseContainer<?> db = CwmsDataApiSetupCallback.getDatabaseLink();
            db.connection(c -> {
                DSLContext dsl = dslContext(c, OFFICE_ID);
                TimeSeriesProfileDao dao = new TimeSeriesProfileDao(dsl);
                dao.deleteTimeSeriesProfile(locationId, keyParameter, OFFICE_ID);
            }, CwmsDataApiSetupCallback.getWebUser());
        } catch (NotFoundException e) {
            // Ignore
        }
    }
}
