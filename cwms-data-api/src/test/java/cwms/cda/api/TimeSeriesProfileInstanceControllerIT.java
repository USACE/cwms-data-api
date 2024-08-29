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

import static cwms.cda.api.Controllers.*;
import static cwms.cda.api.timeseriesprofile.TimeSeriesProfileInstanceController.*;
import static cwms.cda.security.KeyAccessManager.AUTH_HEADER;
import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import cwms.cda.api.errors.NotFoundException;
import cwms.cda.data.dao.StoreRule;
import cwms.cda.data.dao.timeseriesprofile.TimeSeriesProfileDao;
import cwms.cda.data.dao.timeseriesprofile.TimeSeriesProfileInstanceDao;
import cwms.cda.data.dao.timeseriesprofile.TimeSeriesProfileParserDao;
import cwms.cda.data.dto.CwmsId;
import cwms.cda.data.dto.timeseriesprofile.TimeSeriesProfile;
import cwms.cda.data.dto.timeseriesprofile.TimeSeriesProfileInstance;
import cwms.cda.data.dto.timeseriesprofile.TimeSeriesProfileParserColumnar;
import cwms.cda.data.dto.timeseriesprofile.TimeSeriesProfileParserIndexed;
import cwms.cda.formatters.Formats;
import fixtures.CwmsDataApiSetupCallback;
import fixtures.TestAccounts;
import io.restassured.filter.log.LogDetail;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import javax.servlet.http.HttpServletResponse;
import mil.army.usace.hec.test.database.CwmsDatabaseContainer;
import org.apache.commons.io.IOUtils;
import org.jooq.DSLContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;


@Tag("integration")
final class TimeSeriesProfileInstanceControllerIT extends DataApiTestIT {
    private static final String OFFICE_ID = "SPK";
    private static final TestAccounts.KeyUser user = TestAccounts.KeyUser.SPK_NORMAL;
    private final InputStream resource = this.getClass()
            .getResourceAsStream("/cwms/cda/api/timeseriesprofile/ts_profile.json");
    private final InputStream resourceIndexed = this.getClass()
            .getResourceAsStream("/cwms/cda/api/timeseriesprofile/ts_profile_parser_indexed.json");
    private final InputStream resourceColumnar = this.getClass()
            .getResourceAsStream("/cwms/cda/api/timeseriesprofile/ts_profile_parser_columnar.json");
    private final InputStream resourceInstance = this.getClass()
            .getResourceAsStream("/cwms/cda/api/timeseriesprofile/ts_profile_instance.json");
    private final InputStream profileResourceCol = this.getClass()
            .getResourceAsStream("/cwms/cda/api/timeseriesprofile/ts_profile_data_columnar.txt");
    private final InputStream profileResourceInd = this.getClass()
            .getResourceAsStream("/cwms/cda/api/timeseriesprofile/ts_profile_data.txt");
    private final InputStream profileResourceCol2 = this.getClass()
            .getResourceAsStream("/cwms/cda/api/timeseriesprofile/ts_profile_data_columnar_2.txt");
    private final InputStream profileResourceInd2 = this.getClass()
            .getResourceAsStream("/cwms/cda/api/timeseriesprofile/ts_profile_data_2.txt");
    private TimeSeriesProfile tsProfile;
    private TimeSeriesProfileParserIndexed tspParserIndexed;
    private TimeSeriesProfileParserColumnar tspParserColumnar;
    private TimeSeriesProfileInstance tspInstance;
    private String tsProfileDataColumnar;
    private String tsProfileDataIndexed;
    private String tsProfileDataColumnar2;
    private String tsProfileDataIndexed2;
    private String tspData;

    @BeforeEach
    public void setup() throws Exception {
        assertNotNull(resource);
        assertNotNull(resourceIndexed);
        assertNotNull(resourceInstance);
        assertNotNull(profileResourceCol);
        assertNotNull(resourceColumnar);
        assertNotNull(profileResourceInd);
        assertNotNull(profileResourceCol2);
        assertNotNull(profileResourceInd2);
        String tsDataInstance = IOUtils.toString(resourceInstance, StandardCharsets.UTF_8);
        tsProfileDataColumnar = IOUtils.toString(profileResourceCol, StandardCharsets.UTF_8);
        tsProfileDataIndexed = IOUtils.toString(profileResourceInd, StandardCharsets.UTF_8);
        tsProfileDataColumnar2 = IOUtils.toString(profileResourceCol2, StandardCharsets.UTF_8);
        tsProfileDataIndexed2 = IOUtils.toString(profileResourceInd2, StandardCharsets.UTF_8);
        tspData = IOUtils.toString(resource, StandardCharsets.UTF_8);
        tsProfile = Formats.parseContent(Formats.parseHeader(Formats.JSONV2,
                TimeSeriesProfile.class), tspData, TimeSeriesProfile.class);
        tspParserIndexed = Formats.parseContent(Formats.parseHeader(Formats.JSONV2,
                TimeSeriesProfileParserIndexed.class), resourceIndexed, TimeSeriesProfileParserIndexed.class);
        tspParserColumnar = Formats.parseContent(Formats.parseHeader(Formats.JSONV2,
                TimeSeriesProfileParserColumnar.class), resourceColumnar, TimeSeriesProfileParserColumnar.class);
        tspInstance = Formats.parseContent(Formats.parseHeader(Formats.JSONV2,
                TimeSeriesProfileInstance.class), tsDataInstance, TimeSeriesProfileInstance.class);
        createLocation(tsProfile.getLocationId().getName(), true, OFFICE_ID, "SITE");
        CwmsDatabaseContainer<?> db = CwmsDataApiSetupCallback.getDatabaseLink();
        db.connection(c -> {
            DSLContext dsl = dslContext(c, OFFICE_ID);
            TimeSeriesProfileDao dao = new TimeSeriesProfileDao(dsl);
            dao.storeTimeSeriesProfile(tsProfile, false);
        }, CwmsDataApiSetupCallback.getWebUser());
    }

    @AfterEach
    public void tearDown() throws Exception {
        CwmsDatabaseContainer<?> db = CwmsDataApiSetupCallback.getDatabaseLink();
        db.connection(c -> {
            DSLContext dsl = dslContext(c, OFFICE_ID);
            cleanupInstance(dsl, tspInstance.getTimeSeriesProfile().getLocationId(),
                    tspInstance.getTimeSeriesProfile().getKeyParameter(), tspInstance.getVersion(),
                    tspInstance.getFirstDate(), "UTC",
                    false, tspInstance.getVersionDate());
            cleanupParser(dsl, tspParserIndexed.getLocationId().getName(),
                    tspParserIndexed.getKeyParameter());
            cleanupParser(dsl, tspParserColumnar.getLocationId().getName(),
                    tspParserColumnar.getKeyParameter());
            cleanupTS(dsl, tsProfile.getLocationId().getName(), tsProfile.getKeyParameter());
        }, CwmsDataApiSetupCallback.getWebUser());
    }

    @Test
    void test_create_retrieve_TimeSeriesProfileInstance_Columnar() throws Exception {

        storeParser(null, tspParserColumnar);

        // Create instance
        given()
            .log().ifValidationFails(LogDetail.ALL, true)
            .accept(Formats.JSONV2)
            .contentType(Formats.JSONV2)
            .body(tspData)
            .header(AUTH_HEADER, user.toHeaderValue())
            .queryParam(METHOD, StoreRule.REPLACE_ALL)
            .queryParam(OVERRIDE_PROTECTION, false)
            .queryParam(VERSION_DATE, Instant.parse("2024-07-09T12:00:00.00Z").toEpochMilli())
            .queryParam(PROFILE_DATA, tsProfileDataColumnar)
            .queryParam(VERSION, "OBS")
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .post("/timeseries/instance")
        .then()
            .log().ifValidationFails(LogDetail.ALL, true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_CREATED))
        ;

        // Retrieve instance
        given()
            .log().ifValidationFails(LogDetail.ALL, true)
            .accept(Formats.JSONV2)
            .contentType(Formats.JSONV2)
            .header(AUTH_HEADER, user.toHeaderValue())
            .queryParam(OFFICE, OFFICE_ID)
            .queryParam(PARAMETER_ID, tspInstance.getTimeSeriesProfile().getKeyParameter())
            .queryParam(VERSION, "OBS")
            .queryParam(VERSION_DATE, Instant.parse("2024-07-09T12:00:00.00Z").toEpochMilli())
            .queryParam(TIMEZONE, "UTC")
            .queryParam(START, Instant.parse("2018-07-09T19:06:20.00Z").toEpochMilli())
            .queryParam(END, Instant.parse("2025-07-09T19:06:20.00Z").toEpochMilli())
            .queryParam(START_INCLUSIVE, true)
            .queryParam(END_INCLUSIVE, true)
            .queryParam(UNIT, "m,F")
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .get("/timeseries/instance/" + tspInstance.getTimeSeriesProfile().getLocationId().getName())
        .then()
            .log().ifValidationFails(LogDetail.ALL, true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_OK))
            .body("version", equalTo("OBS"))
            .body("version-date", equalTo(Instant.parse("2024-07-09T12:00:00.00Z").toEpochMilli()))
            .body("time-series-profile.location-id.name",
                    equalTo(tspInstance.getTimeSeriesProfile().getLocationId().getName()))
            .body("time-series-profile.key-parameter",
                    equalTo(tspInstance.getTimeSeriesProfile().getKeyParameter()))
            .body("time-series-profile.parameter-list[0]", equalTo("Depth"))
            .body("time-series-list[0].values.size()", equalTo(3))
            .body("time-series-profile.parameter-list[1]", equalTo("Temp-Water"))
        ;
    }

    @Test
    void test_create_retrieve_TimeSeriesProfileInstance_Indexed() throws Exception {

        storeParser(tspParserIndexed, null);

        // Create instance
        given()
            .log().ifValidationFails(LogDetail.ALL, true)
            .accept(Formats.JSONV2)
            .contentType(Formats.JSONV2)
            .body(tspData)
            .header(AUTH_HEADER, user.toHeaderValue())
            .queryParam(METHOD, StoreRule.REPLACE_ALL)
            .queryParam(OVERRIDE_PROTECTION, true)
            .queryParam(VERSION_DATE, Instant.parse("2024-07-09T12:00:00.00Z").toEpochMilli())
            .queryParam(PROFILE_DATA, tsProfileDataIndexed)
            .queryParam(VERSION, "Raw")
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .post("/timeseries/instance")
        .then()
            .log().ifValidationFails(LogDetail.ALL, true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_CREATED))
        ;

        // Retrieve instance
        given()
            .log().ifValidationFails(LogDetail.ALL, true)
            .accept(Formats.JSONV2)
            .contentType(Formats.JSONV2)
            .header(AUTH_HEADER, user.toHeaderValue())
            .queryParam(OFFICE, OFFICE_ID)
            .queryParam(PARAMETER_ID, tspParserIndexed.getKeyParameter())
            .queryParam(VERSION, "Raw")
            .queryParam(VERSION_DATE, Instant.parse("2024-07-09T12:00:00.00Z").toEpochMilli())
            .queryParam(TIMEZONE, "UTC")
            .queryParam(START, Instant.parse("2018-07-09T19:06:20.00Z").toEpochMilli())
            .queryParam(END, Instant.parse("2025-07-09T19:06:20.00Z").toEpochMilli())
            .queryParam(START_INCLUSIVE, true)
            .queryParam(END_INCLUSIVE, true)
            .queryParam(UNIT, "m,F")
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .get("/timeseries/instance/" + tspInstance.getTimeSeriesProfile().getLocationId().getName())
        .then()
            .log().ifValidationFails(LogDetail.ALL, true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_OK))
            .body("version", equalTo("Raw"))
            .body("version-date", equalTo(Instant.parse("2024-07-09T12:00:00.00Z").toEpochMilli()))
            .body("time-series-profile.location-id.name",
                    equalTo(tspInstance.getTimeSeriesProfile().getLocationId().getName()))
            .body("time-series-profile.key-parameter",
                    equalTo(tspInstance.getTimeSeriesProfile().getKeyParameter()))
            .body("time-series-profile.parameter-list[0]", equalTo("Depth"))
            .body("time-series-list[0].values.size()", equalTo(26))
            .body("time-series-profile.parameter-list[1]", equalTo("Temp-Water"))
        ;
    }

    @Test
    void test_delete_TimeSeriesProfileInstance_Columnar() throws Exception {

        storeParser(null, tspParserColumnar);
        // Create instance
        given()
            .log().ifValidationFails(LogDetail.ALL, true)
            .accept(Formats.JSONV2)
            .contentType(Formats.JSONV2)
            .body(tspData)
            .header(AUTH_HEADER, user.toHeaderValue())
            .queryParam(METHOD, StoreRule.REPLACE_ALL)
            .queryParam(OVERRIDE_PROTECTION, false)
            .queryParam(VERSION_DATE, tspInstance.getVersionDate().toEpochMilli())
            .queryParam(PROFILE_DATA, tsProfileDataColumnar)
            .queryParam(VERSION, tspInstance.getVersion())
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .post("/timeseries/instance")
        .then()
            .log().ifValidationFails(LogDetail.ALL, true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_CREATED))
        ;

        // Delete instance
        given()
            .log().ifValidationFails(LogDetail.ALL, true)
            .accept(Formats.JSONV2)
            .contentType(Formats.JSONV2)
            .header(AUTH_HEADER, user.toHeaderValue())
            .queryParam(OFFICE, OFFICE_ID)
            .queryParam(PARAMETER_ID, tspParserColumnar.getKeyParameter())
            .queryParam(VERSION, tspInstance.getVersion())
            .queryParam(VERSION_DATE, tspInstance.getVersionDate().toEpochMilli())
            .queryParam(TIMEZONE, "UTC")
            .queryParam(OVERRIDE_PROTECTION, false)
            .queryParam(DATE, Instant.parse("2019-09-09T12:49:07Z").toEpochMilli())
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .delete("/timeseries/instance/" + tspInstance.getTimeSeriesProfile().getLocationId().getName())
        .then()
            .log().ifValidationFails(LogDetail.ALL, true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_NO_CONTENT))
        ;

        // Retrieve instance and assert it does not exist
        given()
            .log().ifValidationFails(LogDetail.ALL, true)
            .accept(Formats.JSONV2)
            .contentType(Formats.JSONV2)
            .header(AUTH_HEADER, user.toHeaderValue())
            .queryParam(OFFICE, OFFICE_ID)
            .queryParam(PARAMETER_ID, tspParserIndexed.getKeyParameter())
            .queryParam(VERSION, tspInstance.getVersion())
            .queryParam(VERSION_DATE, tspInstance.getVersionDate().toEpochMilli())
            .queryParam(TIMEZONE, "UTC")
            .queryParam(START, Instant.parse("2018-07-09T19:06:20.00Z").toEpochMilli())
            .queryParam(END, Instant.parse("2025-07-09T19:06:20.00Z").toEpochMilli())
            .queryParam(START_INCLUSIVE, true)
            .queryParam(END_INCLUSIVE, true)
            .queryParam(UNIT, "m,F")
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .get("/timeseries/instance/" + tspInstance.getTimeSeriesProfile().getLocationId().getName())
        .then()
            .log().ifValidationFails(LogDetail.ALL, true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_NOT_FOUND))
        ;
    }

    @Test
    void test_delete_TimeSeriesProfileInstance_Indexed() throws Exception {

        storeParser(tspParserIndexed, null);

        // Create instance
        given()
            .log().ifValidationFails(LogDetail.ALL, true)
            .accept(Formats.JSONV2)
            .contentType(Formats.JSONV2)
            .body(tspData)
            .header(AUTH_HEADER, user.toHeaderValue())
            .queryParam(METHOD, StoreRule.REPLACE_ALL)
            .queryParam(OVERRIDE_PROTECTION, false)
            .queryParam(VERSION_DATE, tspInstance.getVersionDate().toEpochMilli())
            .queryParam(PROFILE_DATA, tsProfileDataIndexed)
            .queryParam(VERSION, "Raw")
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .post("/timeseries/instance")
        .then()
            .log().ifValidationFails(LogDetail.ALL, true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_CREATED))
        ;

        // Delete instance
        given()
            .log().ifValidationFails(LogDetail.ALL, true)
            .accept(Formats.JSONV2)
            .contentType(Formats.JSONV2)
            .header(AUTH_HEADER, user.toHeaderValue())
            .queryParam(OFFICE, OFFICE_ID)
            .queryParam(PARAMETER_ID, tspParserColumnar.getKeyParameter())
            .queryParam(VERSION, "Raw")
            .queryParam(VERSION_DATE, tspInstance.getVersionDate().toEpochMilli())
            .queryParam(TIMEZONE, "UTC")
            .queryParam(OVERRIDE_PROTECTION, false)
            .queryParam(DATE, Instant.parse("2019-09-09T12:48:57Z").toEpochMilli())
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .delete("/timeseries/instance/" + tspInstance.getTimeSeriesProfile().getLocationId().getName())
        .then()
            .log().ifValidationFails(LogDetail.ALL, true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_NO_CONTENT))
        ;

        // Retrieve instance and assert it does not exist
        given()
            .log().ifValidationFails(LogDetail.ALL, true)
            .accept(Formats.JSONV2)
            .contentType(Formats.JSONV2)
            .header(AUTH_HEADER, user.toHeaderValue())
            .queryParam(OFFICE, OFFICE_ID)
            .queryParam(PARAMETER_ID, tspParserIndexed.getKeyParameter())
            .queryParam(VERSION, "Raw")
            .queryParam(VERSION_DATE, tspInstance.getVersionDate().toEpochMilli())
            .queryParam(TIMEZONE, "UTC")
            .queryParam(START, Instant.parse("2018-07-09T19:06:20.00Z").toEpochMilli())
            .queryParam(END, Instant.parse("2025-07-09T19:06:20.00Z").toEpochMilli())
            .queryParam(START_INCLUSIVE, true)
            .queryParam(END_INCLUSIVE, true)
            .queryParam(UNIT, "m,F")
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .get("/timeseries/instance/" + tspInstance.getTimeSeriesProfile().getLocationId().getName())
        .then()
            .log().ifValidationFails(LogDetail.ALL, true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_NOT_FOUND))
        ;
    }

    @Test
    void test_delete_nonExistent_TimeSeriesProfileInstance() {
        // Delete instance
        given()
            .log().ifValidationFails(LogDetail.ALL, true)
            .accept(Formats.JSONV2)
            .contentType(Formats.JSONV2)
            .header(AUTH_HEADER, user.toHeaderValue())
            .queryParam(OFFICE, OFFICE_ID)
            .queryParam(PARAMETER_ID, tspInstance.getTimeSeriesProfile().getKeyParameter())
            .queryParam(VERSION, tspInstance.getVersion())
            .queryParam(VERSION_DATE, tspInstance.getVersionDate().toEpochMilli())
            .queryParam(TIMEZONE, "UTC")
            .queryParam(OVERRIDE_PROTECTION, false)
            .queryParam(DATE, tspInstance.getFirstDate().toEpochMilli())
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .delete("/timeseries/instance/nonexistent")
        .then()
            .log().ifValidationFails(LogDetail.ALL, true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_NOT_FOUND))
        ;
    }

    @Test
    void test_retrieve_all_TimeSeriesProfileInstance_Columnar() throws Exception {
        storeParser(null, tspParserColumnar);

        // Create instance
        given()
            .log().ifValidationFails(LogDetail.ALL, true)
            .accept(Formats.JSONV2)
            .contentType(Formats.JSONV2)
            .body(tspData)
            .header(AUTH_HEADER, user.toHeaderValue())
            .queryParam(METHOD, StoreRule.REPLACE_ALL)
            .queryParam(OVERRIDE_PROTECTION, false)
            .queryParam(VERSION_DATE, Instant.parse("2024-07-09T12:00:00.00Z").toEpochMilli())
            .queryParam(PROFILE_DATA, tsProfileDataColumnar)
            .queryParam(VERSION, tspInstance.getVersion())
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .post("/timeseries/instance")
        .then()
            .log().ifValidationFails(LogDetail.ALL, true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_CREATED))
        ;

        // Create instance
        given()
            .log().ifValidationFails(LogDetail.ALL, true)
            .accept(Formats.JSONV2)
            .contentType(Formats.JSONV2)
            .body(tspData)
            .header(AUTH_HEADER, user.toHeaderValue())
            .queryParam(METHOD, StoreRule.REPLACE_ALL)
            .queryParam(OVERRIDE_PROTECTION, false)
            .queryParam(VERSION_DATE, Instant.parse("2024-07-09T12:00:00.00Z").toEpochMilli())
            .queryParam(PROFILE_DATA, tsProfileDataColumnar2)
            .queryParam(VERSION, tspInstance.getVersion())
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .post("/timeseries/instance")
        .then()
            .log().ifValidationFails(LogDetail.ALL, true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_CREATED))
        ;

        // Retrieve all instances
        given()
            .log().ifValidationFails(LogDetail.ALL, true)
            .accept(Formats.JSONV2)
            .contentType(Formats.JSONV2)
            .header(AUTH_HEADER, user.toHeaderValue())
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .get("/timeseries/instance/")
        .then()
            .log().ifValidationFails(LogDetail.ALL, true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_OK))
            .body("[0].version", equalTo(tspInstance.getVersion()))
            .body("[0].version-date", equalTo(Instant.parse("2024-07-09T12:00:00.00Z").toEpochMilli()))
            .body("[0].time-series-profile.key-parameter", equalTo("Depth"))
            .body("[0].time-series-profile.parameter-list.size()", is(0))
            .body("[1].version", equalTo("VERSION"))
            .body("[1].version-date", equalTo(Instant.parse("2024-07-09T12:00:00.00Z").toEpochMilli()))
            .body("[1].time-series-profile.key-parameter", equalTo("Depth"))
            .body("[1].time-series-profile.parameter-list.size()", is(0))
        ;
    }

    @Test
    void test_retrieve_all_TimeSeriesProfileInstance_Indexed() throws Exception {
        storeParser(tspParserIndexed, null);

        // Create instance
        given()
            .log().ifValidationFails(LogDetail.ALL, true)
            .accept(Formats.JSONV2)
            .contentType(Formats.JSONV2)
            .body(tspData)
            .header(AUTH_HEADER, user.toHeaderValue())
            .queryParam(METHOD, StoreRule.REPLACE_ALL)
            .queryParam(OVERRIDE_PROTECTION, false)
            .queryParam(VERSION_DATE, Instant.parse("2024-07-09T12:00:00.00Z").toEpochMilli())
            .queryParam(VERSION, "USGS-Raw")
            .queryParam(PROFILE_DATA, tsProfileDataIndexed)
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .post("/timeseries/instance")
        .then()
            .log().ifValidationFails(LogDetail.ALL, true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_CREATED))
        ;

        given()
            .log().ifValidationFails(LogDetail.ALL, true)
            .accept(Formats.JSONV2)
            .contentType(Formats.JSONV2)
            .body(tspData)
            .header(AUTH_HEADER, user.toHeaderValue())
            .queryParam(METHOD, StoreRule.REPLACE_ALL)
            .queryParam(OVERRIDE_PROTECTION, false)
            .queryParam(VERSION_DATE, Instant.parse("2024-07-09T12:00:00.00Z").toEpochMilli())
            .queryParam(PROFILE_DATA, tsProfileDataIndexed2)
            .queryParam(VERSION, "USGS-Raw")
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .post("/timeseries/instance/")
        .then()
            .log().ifValidationFails(LogDetail.ALL, true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_CREATED))
        ;

        // Retrieve all instances
        given()
            .log().ifValidationFails(LogDetail.ALL, true)
            .accept(Formats.JSONV2)
            .contentType(Formats.JSONV2)
            .header(AUTH_HEADER, user.toHeaderValue())
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .get("/timeseries/instance/")
        .then()
            .log().ifValidationFails(LogDetail.ALL, true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_OK))
            .body("[0].version", equalTo("USGS-Raw"))
            .body("[0].version-date", equalTo(Instant.parse("2024-07-09T12:00:00.00Z").toEpochMilli()))
            .body("[0].time-series-profile.key-parameter", equalTo("Depth"))
            .body("[0].time-series-profile.parameter-list.size()", is(0))
            .body("[1].version", equalTo("USGS-Raw"))
            .body("[1].version-date", equalTo(Instant.parse("2024-07-09T12:00:00.00Z").toEpochMilli()))
            .body("[1].time-series-profile.key-parameter", equalTo("Depth"))
            .body("[1].time-series-profile.parameter-list.size()", is(0))
        ;

        // Current DAO implementation does not return the parameter list data!!!
    }

    private void cleanupParser(DSLContext dsl, String locationId, String parameterId) {
        try {
            TimeSeriesProfileParserDao dao = new TimeSeriesProfileParserDao(dsl);
            dao.deleteTimeSeriesProfileParser(locationId, parameterId, OFFICE_ID);
        } catch (NotFoundException e) {
            // Ignore
        }
    }

    private void cleanupTS(DSLContext dsl, String locationId, String keyParameter) {
        try {
            TimeSeriesProfileDao dao = new TimeSeriesProfileDao(dsl);
            dao.deleteTimeSeriesProfile(locationId, keyParameter, OFFICE_ID);
        } catch (NotFoundException e) {
            // Ignore
        }
    }

    private void cleanupInstance(DSLContext dsl, CwmsId locationId, String keyParameter, String version,
            Instant firstDate, String timeZone, boolean overrideProt, Instant versionDate) {
        try {
            TimeSeriesProfileInstanceDao dao = new TimeSeriesProfileInstanceDao(dsl);
            dao.deleteTimeSeriesProfileInstance(locationId, keyParameter, version, firstDate, timeZone,
                    overrideProt, versionDate);
        } catch (NotFoundException e) {
            // Ignore
        }
    }

    private void storeParser(TimeSeriesProfileParserIndexed parserI, TimeSeriesProfileParserColumnar parserC)
            throws Exception {
        CwmsDatabaseContainer<?> db = CwmsDataApiSetupCallback.getDatabaseLink();
        db.connection(c -> {
            DSLContext dsl = dslContext(c, OFFICE_ID);
            TimeSeriesProfileParserDao dao = new TimeSeriesProfileParserDao(dsl);
            if (parserI != null) {
                dao.storeTimeSeriesProfileParser(parserI, false);
            } else {
                dao.storeTimeSeriesProfileParser(parserC, false);
            }
        });
    }
}
