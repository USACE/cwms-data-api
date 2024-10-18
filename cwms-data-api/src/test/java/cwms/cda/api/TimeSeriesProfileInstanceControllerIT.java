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
import java.sql.SQLException;
import java.time.Instant;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.servlet.http.HttpServletResponse;

import io.restassured.response.ExtractableResponse;
import io.restassured.response.Response;
import mil.army.usace.hec.test.database.CwmsDatabaseContainer;
import org.apache.commons.io.IOUtils;
import org.jooq.DSLContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;


@Tag("integration")
final class TimeSeriesProfileInstanceControllerIT extends DataApiTestIT {
    public static final Logger LOGGER =
            Logger.getLogger(TimeSeriesProfileInstanceControllerIT.class.getName());
    private static final String OFFICE_ID = "SPK";
    private static final TestAccounts.KeyUser user = TestAccounts.KeyUser.SPK_NORMAL;
    private final InputStream resource = this.getClass()
            .getResourceAsStream("/cwms/cda/api/timeseriesprofile/ts_profile.json");
    private final InputStream resource2 = this.getClass()
            .getResourceAsStream("/cwms/cda/api/timeseriesprofile/ts_profile_3.json");
    private final InputStream resourceIndexed = this.getClass()
            .getResourceAsStream("/cwms/cda/api/timeseriesprofile/ts_profile_parser_indexed.json");
    private final InputStream resourceIndexed2 = this.getClass()
            .getResourceAsStream("/cwms/cda/api/timeseriesprofile/ts_profile_parser_indexed_2.json");
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
    private TimeSeriesProfile tsProfile2;
    private TimeSeriesProfileParserIndexed tspParserIndexed;
    private TimeSeriesProfileParserIndexed tspParserIndexed2;
    private TimeSeriesProfileParserColumnar tspParserColumnar;
    private TimeSeriesProfileInstance tspInstance;
    private TimeSeriesProfileInstance tspInstance2;
    private String tsProfileDataColumnar;
    private String tsProfileDataIndexed;
    private String tsProfileDataColumnar2;
    private String tsProfileDataIndexed2;
    private String tspData;
    private String tspData2;
    private final String units = "m,F";

    @BeforeEach
    public void setup() throws Exception {
        assertNotNull(resource);
        assertNotNull(resource2);
        assertNotNull(resourceIndexed);
        assertNotNull(resourceIndexed2);
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
        tspData2 = IOUtils.toString(resource2, StandardCharsets.UTF_8);
        tsProfile = Formats.parseContent(Formats.parseHeader(Formats.JSONV1,
                TimeSeriesProfile.class), tspData, TimeSeriesProfile.class);
        tsProfile2 = Formats.parseContent(Formats.parseHeader(Formats.JSONV1,
                TimeSeriesProfile.class), tspData2, TimeSeriesProfile.class);
        tspParserIndexed = Formats.parseContent(Formats.parseHeader(Formats.JSONV1,
                TimeSeriesProfileParserIndexed.class), resourceIndexed, TimeSeriesProfileParserIndexed.class);
        tspParserIndexed2 = Formats.parseContent(Formats.parseHeader(Formats.JSONV1,
                TimeSeriesProfileParserIndexed.class), resourceIndexed2, TimeSeriesProfileParserIndexed.class);
        tspParserColumnar = Formats.parseContent(Formats.parseHeader(Formats.JSONV1,
                TimeSeriesProfileParserColumnar.class), resourceColumnar, TimeSeriesProfileParserColumnar.class);
        tspInstance = Formats.parseContent(Formats.parseHeader(Formats.JSONV1,
                TimeSeriesProfileInstance.class), tsDataInstance, TimeSeriesProfileInstance.class);
        tspInstance2 = new TimeSeriesProfileInstance.Builder().withTimeSeriesProfile(tsProfile)
                .withVersion(tspInstance.getVersion()).withVersionDate(Instant.parse("2023-07-09T12:00:00.00Z"))
                .withParameterColumns(tspInstance.getParameterColumns())
                .withDataColumns(tspInstance.getDataColumns())
                .withLocationTimeZone("UTC")
                .withPageSize(tspInstance.getPageSize())
                .withFirstDate(tspInstance.getFirstDate())
                .withLastDate(tspInstance.getLastDate())
                .build();
        createLocation(tsProfile.getLocationId().getName(), true, OFFICE_ID, "SITE");
        createLocation(tsProfile2.getLocationId().getName(), true, OFFICE_ID, "SITE");
        CwmsDatabaseContainer<?> db = CwmsDataApiSetupCallback.getDatabaseLink();
        db.connection(c -> {
            DSLContext dsl = dslContext(c, OFFICE_ID);
            TimeSeriesProfileDao dao = new TimeSeriesProfileDao(dsl);
            dao.storeTimeSeriesProfile(tsProfile, false);
            try {
                createTimeseries(OFFICE_ID, tsProfile2.getReferenceTsId().getName());
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
            dao.storeTimeSeriesProfile(tsProfile2, false);
        }, CwmsDataApiSetupCallback.getWebUser());
    }

    @AfterEach
    public void tearDown() throws Exception {
        CwmsDatabaseContainer<?> db = CwmsDataApiSetupCallback.getDatabaseLink();
        db.connection(c -> {
            DSLContext dsl = dslContext(c, OFFICE_ID);
            if (tspInstance != null) {
                cleanupInstance(dsl, tspInstance.getTimeSeriesProfile().getLocationId(),
                        tspInstance.getTimeSeriesProfile().getKeyParameter(), tspInstance.getVersion(),
                        tspInstance.getFirstDate(), "UTC",
                        false, tspInstance.getVersionDate());
            }
            if (tspInstance2 != null) {
                cleanupInstance(dsl, tspInstance2.getTimeSeriesProfile().getLocationId(),
                        tspInstance2.getTimeSeriesProfile().getKeyParameter(), tspInstance2.getVersion(),
                        tspInstance2.getFirstDate(), "UTC",
                        false, tspInstance2.getVersionDate());
            }
            if (tspParserIndexed != null) {
                cleanupParser(dsl, tspParserIndexed.getLocationId().getName(),
                        tspParserIndexed.getKeyParameter());
            }
            if (tspParserIndexed2 != null) {
                cleanupParser(dsl, tspParserIndexed2.getLocationId().getName(),
                        tspParserIndexed2.getKeyParameter());
            }
            if (tspParserColumnar != null) {
                cleanupParser(dsl, tspParserColumnar.getLocationId().getName(),
                        tspParserColumnar.getKeyParameter());
            }
            if (tsProfile != null) {
                cleanupTS(dsl, tsProfile.getLocationId().getName(), tsProfile.getKeyParameter());
            }
            if (tsProfile2 != null) {
                cleanupTS(dsl, tsProfile2.getLocationId().getName(), tsProfile2.getKeyParameter());
            }
        }, CwmsDataApiSetupCallback.getWebUser());
    }

    @Test
    void test_create_retrieve_TimeSeriesProfileInstance_Columnar() throws Exception {

        storeParser(null, tspParserColumnar);

        // Create instance
        given()
            .log().ifValidationFails(LogDetail.ALL, true)
            .accept(Formats.JSONV1)
            .contentType(Formats.JSONV1)
            .body(tspData)
            .header(AUTH_HEADER, user.toHeaderValue())
            .queryParam(METHOD, StoreRule.REPLACE_ALL)
            .queryParam(OVERRIDE_PROTECTION, false)
            .queryParam(VERSION_DATE, "2024-07-09T12:00:00.00Z")
            .queryParam(PROFILE_DATA, tsProfileDataColumnar)
            .queryParam(VERSION, "OBS")
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .post("/timeseries/profile-instance/")
        .then()
            .log().ifValidationFails(LogDetail.ALL, true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_CREATED))
        ;

        // Retrieve instance
        given()
            .log().ifValidationFails(LogDetail.ALL, true)
            .accept(Formats.JSONV1)
            .contentType(Formats.JSONV1)
            .header(AUTH_HEADER, user.toHeaderValue())
            .queryParam(OFFICE, OFFICE_ID)
            .queryParam(VERSION_DATE, "2024-07-09T12:00:00.00Z")
            .queryParam(TIMEZONE, "UTC")
            .queryParam(START, "2018-07-09T19:06:20.00Z")
            .queryParam(END, "2025-07-09T19:06:20.00Z")
            .queryParam(START_TIME_INCLUSIVE, true)
            .queryParam(END_TIME_INCLUSIVE, true)
            .queryParam(UNIT, units)
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .get("/timeseries/profile-instance/" + tspInstance.getTimeSeriesProfile().getLocationId().getName()
                    + "/" + tspInstance.getTimeSeriesProfile().getKeyParameter() + "/" + "OBS")
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
            .body("time-series-profile.parameter-list.size()", equalTo(2))
            .body("time-series-list.size()", equalTo(3))
        ;
    }

    @Test
    void store_retrieve_instance_with_ref_TS() throws Exception {
        storeParser(tspParserIndexed2, null);

        // Create instance
        given()
            .log().ifValidationFails(LogDetail.ALL, true)
            .accept(Formats.JSONV1)
            .contentType(Formats.JSONV1)
            .body(tspData2)
            .header(AUTH_HEADER, user.toHeaderValue())
            .queryParam(METHOD, StoreRule.REPLACE_ALL)
            .queryParam(OVERRIDE_PROTECTION, true)
            .queryParam(VERSION_DATE, "2024-07-09T12:00:00.00Z")
            .queryParam(PROFILE_DATA, tsProfileDataIndexed2)
            .queryParam(VERSION, "Raw")
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .post("/timeseries/profile-instance/")
        .then()
            .log().ifValidationFails(LogDetail.ALL, true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_CREATED))
        ;

        // Retrieve instance
        given()
            .log().ifValidationFails(LogDetail.ALL, true)
            .accept(Formats.JSONV1)
            .contentType(Formats.JSONV1)
            .header(AUTH_HEADER, user.toHeaderValue())
            .queryParam(OFFICE, OFFICE_ID)
            .queryParam(VERSION_DATE, "2024-07-09T12:00:00.00Z")
            .queryParam(TIMEZONE, "UTC")
            .queryParam(START, "2018-07-09T19:06:20.00Z")
            .queryParam(END, "2025-07-09T19:06:20.00Z")
            .queryParam(START_TIME_INCLUSIVE, true)
            .queryParam(END_TIME_INCLUSIVE, true)
            .queryParam(UNIT, "F,m")
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .get("/timeseries/profile-instance/" + tspParserIndexed2.getLocationId().getName()
                    + "/" + tspParserIndexed2.getKeyParameter() + "/" + "Raw")
        .then()
            .log().ifValidationFails(LogDetail.ALL, true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_OK))
            .body("version", equalTo("Raw"))
            .body("version-date", equalTo(Instant.parse("2024-07-09T12:00:00.00Z").toEpochMilli()))
            .body("time-series-profile.location-id.name",
                    equalTo(tspParserIndexed2.getLocationId().getName()))
            .body("time-series-profile.key-parameter",
                    equalTo(tspParserIndexed2.getKeyParameter()))
            .body("time-series-profile.parameter-list[0]", equalTo("Depth"))
            .body("time-series-list.size()", equalTo(26))
            .body("time-series-profile.parameter-list[1]", equalTo("Temp-Water"))
            .body("time-series-profile.reference-ts-id.name", equalTo(tsProfile2.getReferenceTsId().getName()))
        ;
    }

    @Test
    void test_create_retrieve_TimeSeriesProfileInstance_Indexed() throws Exception {

        storeParser(tspParserIndexed, null);

        // Create instance
        given()
            .log().ifValidationFails(LogDetail.ALL, true)
            .accept(Formats.JSONV1)
            .contentType(Formats.JSONV1)
            .body(tspData)
            .header(AUTH_HEADER, user.toHeaderValue())
            .queryParam(METHOD, StoreRule.REPLACE_ALL)
            .queryParam(OVERRIDE_PROTECTION, true)
            .queryParam(VERSION_DATE, "2024-07-09T12:00:00.00Z")
            .queryParam(PROFILE_DATA, tsProfileDataIndexed)
            .queryParam(VERSION, "USGS-Raw")
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .post("/timeseries/profile-instance/")
        .then()
            .log().ifValidationFails(LogDetail.ALL, true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_CREATED))
        ;

        // Retrieve instance
        given()
            .log().ifValidationFails(LogDetail.ALL, true)
            .accept(Formats.JSONV1)
            .contentType(Formats.JSONV1)
            .header(AUTH_HEADER, user.toHeaderValue())
            .queryParam(OFFICE, OFFICE_ID)
            .queryParam(VERSION_DATE, "2024-07-09T12:00:00.00Z")
            .queryParam(TIMEZONE, "UTC")
            .queryParam(START, "2018-07-09T19:06:20.00Z")
            .queryParam(END, "2025-07-09T19:06:20.00Z")
            .queryParam(START_TIME_INCLUSIVE, true)
            .queryParam(END_TIME_INCLUSIVE, true)
            .queryParam(UNIT, units)
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .get("/timeseries/profile-instance/" + tspInstance.getTimeSeriesProfile().getLocationId().getName()
                    + "/" + tspParserIndexed.getKeyParameter() + "/" + "USGS-Raw")
        .then()
            .log().ifValidationFails(LogDetail.ALL, true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_OK))
            .body("version", equalTo("USGS-Raw"))
            .body("version-date", equalTo(Instant.parse("2024-07-09T12:00:00.00Z").toEpochMilli()))
            .body("time-series-profile.location-id.name",
                    equalTo(tspInstance.getTimeSeriesProfile().getLocationId().getName()))
            .body("time-series-profile.key-parameter",
                    equalTo(tspInstance.getTimeSeriesProfile().getKeyParameter()))
            .body("time-series-profile.parameter-list[0]", equalTo("Depth"))
            .body("time-series-list.size()", equalTo(26))
            .body("time-series-profile.parameter-list[1]", equalTo("Temp-Water"))
        ;
    }

    @Test
    void test_create_retrieve_paged_TimeSeriesProfileInstance_Indexed() throws Exception {

        storeParser(tspParserIndexed, null);

        // Create instance
        given()
            .log().ifValidationFails(LogDetail.ALL, true)
            .accept(Formats.JSONV1)
            .contentType(Formats.JSONV1)
            .body(tspData)
            .header(AUTH_HEADER, user.toHeaderValue())
            .queryParam(METHOD, StoreRule.REPLACE_ALL)
            .queryParam(OVERRIDE_PROTECTION, true)
            .queryParam(VERSION_DATE, "2024-07-09T12:00:00.00Z")
            .queryParam(PROFILE_DATA, tsProfileDataIndexed)
            .queryParam(VERSION, "Raw")
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .post("/timeseries/profile-instance/")
        .then()
            .log().ifValidationFails(LogDetail.ALL, true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_CREATED))
        ;

        // Retrieve instance
        ExtractableResponse<Response> extractableResponse = given()
            .log().ifValidationFails(LogDetail.ALL, true)
            .accept(Formats.JSONV1)
            .contentType(Formats.JSONV1)
            .header(AUTH_HEADER, user.toHeaderValue())
            .queryParam(OFFICE, OFFICE_ID)
            .queryParam(VERSION_DATE, "2024-07-09T12:00:00.00Z")
            .queryParam(TIMEZONE, "UTC")
            .queryParam(START, "2018-07-09T19:06:20.00Z")
            .queryParam(END, "2025-07-09T19:06:20.00Z")
            .queryParam(START_TIME_INCLUSIVE, true)
            .queryParam(END_TIME_INCLUSIVE, true)
            .queryParam(UNIT, units)
            .queryParam(PAGE_SIZE, 10)
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .get("/timeseries/profile-instance/" + tspInstance.getTimeSeriesProfile().getLocationId().getName()
                    + "/" + tspParserIndexed.getKeyParameter() + "/" + "Raw")
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
            .body("time-series-profile.parameter-list.size()", equalTo(2))
            .body("time-series-list.size()", equalTo(10))
            .body("time-series-list[\"1568033682000\"].size()", equalTo(2))
            .body("time-series-list[\"1568033337000\"].size()", equalTo(2))
            .body("time-series-list[\"1568033937000\"].size()", equalTo(2))
            .body("time-series-list[\"1568033750000\"].size()", equalTo(2))
            .body("time-series-list[\"1568033787000\"].size()", equalTo(2))
            .extract()
        ;

        String cursor = extractableResponse.path("next-page");
        assertNotNull(cursor);

        given()
            .log().ifValidationFails(LogDetail.ALL, true)
            .accept(Formats.JSONV1)
            .contentType(Formats.JSONV1)
            .header(AUTH_HEADER, user.toHeaderValue())
            .queryParam(OFFICE, OFFICE_ID)
            .queryParam(VERSION_DATE, "2024-07-09T12:00:00.00Z")
            .queryParam(TIMEZONE, "UTC")
            .queryParam(START, "2018-07-09T19:06:20.00Z")
            .queryParam(END, "2025-07-09T19:06:20.00Z")
            .queryParam(START_TIME_INCLUSIVE, true)
            .queryParam(END_TIME_INCLUSIVE, true)
            .queryParam(UNIT, units)
            .queryParam(PAGE, cursor)
            .queryParam(PAGE_SIZE, 10)
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .get("/timeseries/profile-instance/" + tspInstance.getTimeSeriesProfile().getLocationId().getName()
                    + "/" + tspParserIndexed.getKeyParameter() + "/" + "Raw")
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
            .body("time-series-profile.parameter-list.size()", equalTo(2))
            .body("time-series-list.size()", equalTo(10))
            .body("time-series-list[\"1568033971000\"].size()", equalTo(2))
            .body("time-series-list[\"1568034068000\"].size()", equalTo(2))
            .body("time-series-list[\"1568034586000\"].size()", equalTo(2))
        ;
    }

    @Test
    void test_create_retrieve_paged_TimeSeriesProfileInstance_Columnar() throws Exception {

        storeParser(null, tspParserColumnar);

        // Create instance
        given()
            .log().ifValidationFails(LogDetail.ALL, true)
            .accept(Formats.JSONV1)
            .contentType(Formats.JSONV1)
            .body(tspData)
            .header(AUTH_HEADER, user.toHeaderValue())
            .queryParam(METHOD, StoreRule.REPLACE_ALL)
            .queryParam(OVERRIDE_PROTECTION, false)
            .queryParam(VERSION_DATE, "2024-07-09T12:00:00.00Z")
            .queryParam(PROFILE_DATA, tsProfileDataColumnar)
            .queryParam(VERSION, "TEST-raw")
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .post("/timeseries/profile-instance/")
        .then()
            .log().ifValidationFails(LogDetail.ALL, true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_CREATED))
        ;

        // Retrieve instance
        ExtractableResponse<Response> extractableResponse = given()
            .log().ifValidationFails(LogDetail.ALL, true)
            .accept(Formats.JSONV1)
            .contentType(Formats.JSONV1)
            .header(AUTH_HEADER, user.toHeaderValue())
            .queryParam(OFFICE, OFFICE_ID)
            .queryParam(VERSION_DATE, "2024-07-09T12:00:00.00Z")
            .queryParam(TIMEZONE, "UTC")
            .queryParam(START, "2018-07-09T19:06:20.00Z")
            .queryParam(END, "2025-07-09T19:06:20.00Z")
            .queryParam(START_TIME_INCLUSIVE, true)
            .queryParam(END_TIME_INCLUSIVE, true)
            .queryParam(UNIT, units)
            .queryParam(PAGE_SIZE, 2)
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .get("/timeseries/profile-instance/" + tspInstance.getTimeSeriesProfile().getLocationId().getName()
                    + "/" + tspParserColumnar.getKeyParameter() + "/" + "TEST-raw")
        .then()
            .log().ifValidationFails(LogDetail.ALL, true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_OK))
            .body("version", equalTo("TEST-raw"))
            .body("version-date", equalTo(Instant.parse("2024-07-09T12:00:00.00Z").toEpochMilli()))
            .body("time-series-profile.location-id.name",
                    equalTo(tspInstance.getTimeSeriesProfile().getLocationId().getName()))
            .body("time-series-profile.key-parameter",
                    equalTo(tspInstance.getTimeSeriesProfile().getKeyParameter()))
            .body("time-series-profile.parameter-list.size()", equalTo(2))
            .body("time-series-list.size()", equalTo(2))
            .body("time-series-list[\"1568033347000\"].size()", equalTo(2))
            .body("time-series-list[\"1568033359000\"].size()", equalTo(2))
            .extract()
        ;

        String cursor = extractableResponse.path("next-page");
        assertNotNull(cursor);

        given()
            .log().ifValidationFails(LogDetail.ALL, true)
            .accept(Formats.JSONV1)
            .contentType(Formats.JSONV1)
            .header(AUTH_HEADER, user.toHeaderValue())
            .queryParam(OFFICE, OFFICE_ID)
            .queryParam(VERSION_DATE, "2024-07-09T12:00:00.00Z")
            .queryParam(TIMEZONE, "UTC")
            .queryParam(START, "2018-07-09T19:06:20.00Z")
            .queryParam(END, "2025-07-09T19:06:20.00Z")
            .queryParam(START_TIME_INCLUSIVE, true)
            .queryParam(END_TIME_INCLUSIVE, true)
            .queryParam(UNIT, units)
            .queryParam(PAGE, cursor)
            .queryParam(PAGE_SIZE, 3)
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .get("/timeseries/profile-instance/" + tspInstance.getTimeSeriesProfile().getLocationId().getName()
                    + "/" + tspParserColumnar.getKeyParameter() + "/" + "TEST-raw")
        .then()
            .log().ifValidationFails(LogDetail.ALL, true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_OK))
            .body("version", equalTo("TEST-raw"))
            .body("version-date", equalTo(Instant.parse("2024-07-09T12:00:00.00Z").toEpochMilli()))
            .body("time-series-profile.location-id.name",
                    equalTo(tspInstance.getTimeSeriesProfile().getLocationId().getName()))
            .body("time-series-profile.key-parameter",
                    equalTo(tspInstance.getTimeSeriesProfile().getKeyParameter()))
            .body("time-series-profile.parameter-list.size()", equalTo(2))
            .body("time-series-list.size()", equalTo(1))
            .body("time-series-list[\"1568035040000\"].size()", equalTo(2))
        ;
    }

    @Test
    void test_retrieve_TimeSeriesProfileInstance_Columnar_maxVersion() throws Exception {
        storeParser(null, tspParserColumnar);

        // Create instance
        given()
            .log().ifValidationFails(LogDetail.ALL, true)
            .accept(Formats.JSONV1)
            .contentType(Formats.JSONV1)
            .body(tspData)
            .header(AUTH_HEADER, user.toHeaderValue())
            .queryParam(METHOD, StoreRule.REPLACE_ALL)
            .queryParam(OVERRIDE_PROTECTION, false)
            .queryParam(VERSION_DATE, "2024-07-09T12:00:00.00Z")
            .queryParam(PROFILE_DATA, tsProfileDataColumnar)
            .queryParam(VERSION, "OBS")
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .post("/timeseries/profile-instance/")
        .then()
            .log().ifValidationFails(LogDetail.ALL, true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_CREATED))
        ;

        // Create instance with different version date
        given()
            .log().ifValidationFails(LogDetail.ALL, true)
            .accept(Formats.JSONV1)
            .contentType(Formats.JSONV1)
            .body(tspData)
            .header(AUTH_HEADER, user.toHeaderValue())
            .queryParam(METHOD, StoreRule.REPLACE_ALL)
            .queryParam(OVERRIDE_PROTECTION, false)
            .queryParam(VERSION_DATE, "2023-07-09T12:00:00.00Z")
            .queryParam(PROFILE_DATA, tsProfileDataColumnar2)
            .queryParam(VERSION, "OBS")
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .post("/timeseries/profile-instance/")
        .then()
            .log().ifValidationFails(LogDetail.ALL, true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_CREATED))
        ;

        // Retrieve instance with max version set and provided version date (throws error)
        given()
            .log().ifValidationFails(LogDetail.ALL, true)
            .accept(Formats.JSONV1)
            .contentType(Formats.JSONV1)
            .header(AUTH_HEADER, user.toHeaderValue())
            .queryParam(OFFICE, OFFICE_ID)
            .queryParam(PARAMETER_ID, tspParserColumnar.getKeyParameter())
            .queryParam(VERSION, "OBS")
            .queryParam(VERSION_DATE, Instant.parse("2023-07-09T12:00:00.00Z"))
            .queryParam(TIMEZONE, "UTC")
            .queryParam(START, Instant.parse("2018-07-09T19:06:20.00Z"))
            .queryParam(END, Instant.parse("2025-07-09T19:06:20.00Z"))
            .queryParam(START_TIME_INCLUSIVE, true)
            .queryParam(END_TIME_INCLUSIVE, true)
            .queryParam(MAX_VERSION, true)
            .queryParam(UNIT, units)
            .queryParam(PAGE_SIZE, 3)
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .get("/timeseries/profile-instance/" + tspInstance.getTimeSeriesProfile().getLocationId().getName()
                    + "/" + tspInstance.getTimeSeriesProfile().getKeyParameter() + "/" + tspInstance.getVersion())
        .then()
            .log().ifValidationFails(LogDetail.ALL, true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_BAD_REQUEST))
        ;

        // retrieve with no max version (should return the specified version)
        given()
            .log().ifValidationFails(LogDetail.ALL, true)
            .accept(Formats.JSONV1)
            .contentType(Formats.JSONV1)
            .header(AUTH_HEADER, user.toHeaderValue())
            .queryParam(OFFICE, OFFICE_ID)
            .queryParam(VERSION_DATE, "2024-07-09T12:00:00.00Z")
            .queryParam(TIMEZONE, "UTC")
            .queryParam(START, "2018-07-09T19:06:20.00Z")
            .queryParam(END, "2025-07-09T19:06:20.00Z")
            .queryParam(START_TIME_INCLUSIVE, true)
            .queryParam(END_TIME_INCLUSIVE, true)
            .queryParam(UNIT, units)
            .queryParam(MAX_VERSION, false)
            .queryParam(PAGE_SIZE, 3)
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .get("/timeseries/profile-instance/" + tspInstance.getTimeSeriesProfile().getLocationId().getName()
                    + "/" + tspParserColumnar.getKeyParameter() + "/" + "OBS")
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
            .body("time-series-profile.parameter-list.size()", equalTo(2))
            .body("time-series-list.size()", equalTo(3))
            .body("time-series-list[\"1568033347000\"].size()", equalTo(2))
            .body("time-series-list[\"1568033359000\"].size()", equalTo(2))
        ;

        // retrieve with max version (should return the max version)
        given()
            .log().ifValidationFails(LogDetail.ALL, true)
            .accept(Formats.JSONV1)
            .contentType(Formats.JSONV1)
            .header(AUTH_HEADER, user.toHeaderValue())
            .queryParam(OFFICE, OFFICE_ID)
            .queryParam(TIMEZONE, "UTC")
            .queryParam(START, "2018-07-09T19:06:20.00Z")
            .queryParam(END, "2025-07-09T19:06:20.00Z")
            .queryParam(START_TIME_INCLUSIVE, true)
            .queryParam(END_TIME_INCLUSIVE, true)
            .queryParam(UNIT, units)
            .queryParam(MAX_VERSION, true)
            .queryParam(PAGE_SIZE, 3)
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .get("/timeseries/profile-instance/" + tspInstance.getTimeSeriesProfile().getLocationId().getName()
                    + "/" + tspParserColumnar.getKeyParameter() + "/" + "OBS")
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
            .body("time-series-profile.parameter-list.size()", equalTo(2))
            .body("time-series-list.size()", equalTo(3))
            .body("time-series-list[\"1568033347000\"].size()", equalTo(2))
            .body("time-series-list[\"1568033359000\"].size()", equalTo(2))
        ;

        // retrieve with no max version (should return the min version)
        given()
            .log().ifValidationFails(LogDetail.ALL, true)
            .accept(Formats.JSONV1)
            .contentType(Formats.JSONV1)
            .header(AUTH_HEADER, user.toHeaderValue())
            .queryParam(OFFICE, OFFICE_ID)
            .queryParam(TIMEZONE, "UTC")
            .queryParam(START, "2018-07-09T19:06:20.00Z")
            .queryParam(END, "2025-07-09T19:06:20.00Z")
            .queryParam(START_TIME_INCLUSIVE, true)
            .queryParam(END_TIME_INCLUSIVE, true)
            .queryParam(UNIT, units)
            .queryParam(MAX_VERSION, false)
            .queryParam(PAGE_SIZE, 3)
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .get("/timeseries/profile-instance/" + tspInstance.getTimeSeriesProfile().getLocationId().getName()
                    + "/" + tspParserColumnar.getKeyParameter() + "/" + "OBS")
        .then()
            .log().ifValidationFails(LogDetail.ALL, true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_OK))
            .body("version", equalTo("OBS"))
            .body("version-date", equalTo(Instant.parse("2023-07-09T12:00:00.00Z").toEpochMilli()))
            .body("time-series-profile.location-id.name",
                    equalTo(tspInstance.getTimeSeriesProfile().getLocationId().getName()))
            .body("time-series-profile.key-parameter",
                    equalTo(tspInstance.getTimeSeriesProfile().getKeyParameter()))
            .body("time-series-profile.parameter-list.size()", equalTo(2))
            .body("time-series-list.size()", equalTo(3))
            .body("time-series-list[\"1599659347000\"].size()", equalTo(2))
            .body("time-series-list[\"1599659359000\"].size()", equalTo(2))
        ;
    }

    // This test needs the functionality to be confirmed - unsure if this is how it should work
    @Test
    void test_previous_next_TimeSeriesProfileInstance_Indexed() throws Exception {
        storeParser(tspParserIndexed, null);

        // Create instance
        given()
            .log().ifValidationFails(LogDetail.ALL, true)
            .accept(Formats.JSONV1)
            .contentType(Formats.JSONV1)
            .body(tspData)
            .header(AUTH_HEADER, user.toHeaderValue())
            .queryParam(METHOD, StoreRule.REPLACE_ALL)
            .queryParam(OVERRIDE_PROTECTION, true)
            .queryParam(VERSION_DATE,"2024-07-09T12:00:00.00Z")
            .queryParam(PROFILE_DATA, tsProfileDataIndexed)
            .queryParam(VERSION, "USGS-Obs")
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .post("/timeseries/profile-instance/")
        .then()
            .log().ifValidationFails(LogDetail.ALL, true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_CREATED))
        ;

        // Retrieve instance with next and previous set (should return values from the next and previous time windows)
        given()
            .log().ifValidationFails(LogDetail.ALL, true)
            .accept(Formats.JSONV1)
            .contentType(Formats.JSONV1)
            .header(AUTH_HEADER, user.toHeaderValue())
            .queryParam(OFFICE, OFFICE_ID)
            .queryParam(VERSION_DATE, "2024-07-09T12:00:00.00Z")
            .queryParam(TIMEZONE, "UTC")
            .queryParam(START, "2019-09-09T12:45:00.00Z")
            .queryParam(END, "2019-09-09T14:45:00.00Z")
            .queryParam(START_TIME_INCLUSIVE, true)
            .queryParam(PREVIOUS, true)
            .queryParam(NEXT, true)
            .queryParam(END_TIME_INCLUSIVE, true)
            .queryParam(UNIT, units)
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .get("/timeseries/profile-instance/" + tspInstance.getTimeSeriesProfile().getLocationId().getName()
                    + "/" + tspParserIndexed.getKeyParameter() + "/" + "USGS-Obs")
        .then()
            .log().ifValidationFails(LogDetail.ALL, true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_OK))
            .body("version", equalTo("USGS-Obs"))
            .body("version-date", equalTo(Instant.parse("2024-07-09T12:00:00.00Z").toEpochMilli()))
            .body("time-series-profile.location-id.name",
                    equalTo(tspInstance.getTimeSeriesProfile().getLocationId().getName()))
            .body("time-series-profile.key-parameter",
                    equalTo(tspInstance.getTimeSeriesProfile().getKeyParameter()))
            .body("time-series-profile.parameter-list.size()", equalTo(2))
            .body("time-series-list.size()", equalTo(26))
            .body("time-series-list[\"1568033682000\"].size()", equalTo(2))
            .body("time-series-list[\"1568033337000\"].size()", equalTo(2))
        ;

        // Retrieve instance with next set to true
        given()
            .log().ifValidationFails(LogDetail.ALL, true)
            .accept(Formats.JSONV1)
            .contentType(Formats.JSONV1)
            .header(AUTH_HEADER, user.toHeaderValue())
            .queryParam(OFFICE, OFFICE_ID)
            .queryParam(VERSION_DATE, "2024-07-09T12:00:00.00Z")
            .queryParam(TIMEZONE, "UTC")
            .queryParam(START, "2019-09-09T12:45:00.00Z")
            .queryParam(END, "2019-09-09T14:45:00.00Z")
            .queryParam(START_TIME_INCLUSIVE, true)
            .queryParam(END_TIME_INCLUSIVE, true)
            .queryParam(NEXT, true)
            .queryParam(UNIT, units)
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .get("/timeseries/profile-instance/" + tspInstance.getTimeSeriesProfile().getLocationId().getName()
                    + "/" + tspParserIndexed.getKeyParameter() + "/" + "USGS-Obs")
        .then()
            .log().ifValidationFails(LogDetail.ALL, true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_OK))
            .body("version", equalTo("USGS-Obs"))
            .body("version-date", equalTo(Instant.parse("2024-07-09T12:00:00.00Z").toEpochMilli()))
            .body("time-series-profile.location-id.name",
                    equalTo(tspInstance.getTimeSeriesProfile().getLocationId().getName()))
            .body("time-series-profile.key-parameter",
                    equalTo(tspInstance.getTimeSeriesProfile().getKeyParameter()))
            .body("time-series-profile.parameter-list.size()", equalTo(2))
            .body("time-series-list.size()", equalTo(26))
            .body("time-series-list[\"1568033937000\"].size()", equalTo(2))
            .body("time-series-list[\"1568033750000\"].size()", equalTo(2))
            .body("time-series-list[\"1568033787000\"].size()", equalTo(2))
        ;

        // Retrieve instance with both next and previous set to false (should return the first page)
        given()
            .log().ifValidationFails(LogDetail.ALL, true)
            .accept(Formats.JSONV1)
            .contentType(Formats.JSONV1)
            .header(AUTH_HEADER, user.toHeaderValue())
            .queryParam(OFFICE, OFFICE_ID)
            .queryParam(VERSION_DATE, "2024-07-09T12:00:00.00Z")
            .queryParam(TIMEZONE, "UTC")
            .queryParam(START, "2019-09-09T12:45:00.00Z")
            .queryParam(END, "2019-09-09T14:45:00.00Z")
            .queryParam(START_TIME_INCLUSIVE, true)
            .queryParam(END_TIME_INCLUSIVE, true)
            .queryParam(PREVIOUS, true)
            .queryParam(UNIT, units)
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .get("/timeseries/profile-instance/" + tspInstance.getTimeSeriesProfile().getLocationId().getName()
                    + "/" + tspParserIndexed.getKeyParameter() + "/" + "USGS-Obs")
        .then()
            .log().ifValidationFails(LogDetail.ALL, true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_OK))
            .body("version", equalTo("USGS-Obs"))
            .body("version-date", equalTo(Instant.parse("2024-07-09T12:00:00.00Z").toEpochMilli()))
            .body("time-series-profile.location-id.name",
                    equalTo(tspInstance.getTimeSeriesProfile().getLocationId().getName()))
            .body("time-series-profile.key-parameter",
                    equalTo(tspInstance.getTimeSeriesProfile().getKeyParameter()))
            .body("time-series-profile.parameter-list.size()", equalTo(2))
            .body("time-series-list.size()", equalTo(26))
            .body("time-series-list[\"1568033937000\"].size()", equalTo(2))
            .body("time-series-list[\"1568033750000\"].size()", equalTo(2))
            .body("time-series-list[\"1568033787000\"].size()", equalTo(2))
        ;
    }

    @Test
    void test_delete_TimeSeriesProfileInstance_Columnar() throws Exception {

        storeParser(null, tspParserColumnar);
        // Create instance
        given()
            .log().ifValidationFails(LogDetail.ALL, true)
            .accept(Formats.JSONV1)
            .contentType(Formats.JSONV1)
            .body(tspData)
            .header(AUTH_HEADER, user.toHeaderValue())
            .queryParam(METHOD, StoreRule.REPLACE_ALL)
            .queryParam(OVERRIDE_PROTECTION, false)
            .queryParam(VERSION_DATE, tspInstance.getVersionDate().toString())
            .queryParam(PROFILE_DATA, tsProfileDataColumnar)
            .queryParam(VERSION, tspInstance.getVersion())
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .post("/timeseries/profile-instance/")
        .then()
            .log().ifValidationFails(LogDetail.ALL, true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_CREATED))
        ;

        // Delete instance
        given()
            .log().ifValidationFails(LogDetail.ALL, true)
            .accept(Formats.JSONV1)
            .contentType(Formats.JSONV1)
            .header(AUTH_HEADER, user.toHeaderValue())
            .queryParam(OFFICE, OFFICE_ID)
            .queryParam(VERSION_DATE, tspInstance.getVersionDate().toString())
            .queryParam(TIMEZONE, "UTC")
            .queryParam(OVERRIDE_PROTECTION, false)
            .queryParam(DATE, "2019-09-09T12:49:07Z")
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .delete("/timeseries/profile-instance/" + tspInstance.getTimeSeriesProfile().getLocationId().getName()
                    + "/" + tspInstance.getTimeSeriesProfile().getKeyParameter() + "/" + tspInstance.getVersion())
        .then()
            .log().ifValidationFails(LogDetail.ALL, true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_NO_CONTENT))
        ;

        // Retrieve instance and assert it does not exist
        given()
            .log().ifValidationFails(LogDetail.ALL, true)
            .accept(Formats.JSONV1)
            .contentType(Formats.JSONV1)
            .header(AUTH_HEADER, user.toHeaderValue())
            .queryParam(OFFICE, OFFICE_ID)
            .queryParam(PARAMETER_ID, tspParserIndexed.getKeyParameter())
            .queryParam(VERSION, tspInstance.getVersion())
            .queryParam(VERSION_DATE, tspInstance.getVersionDate().toString())
            .queryParam(TIMEZONE, "UTC")
            .queryParam(START, "2018-07-09T19:06:20.00Z")
            .queryParam(END, "2025-07-09T19:06:20.00Z")
            .queryParam(START_TIME_INCLUSIVE, true)
            .queryParam(END_TIME_INCLUSIVE, true)
            .queryParam(UNIT, units)
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .get("/timeseries/profile-instance/" + tspInstance.getTimeSeriesProfile().getLocationId().getName()
                    + "/" + tspInstance.getTimeSeriesProfile().getKeyParameter() + "/" + tspInstance.getVersion())
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
            .accept(Formats.JSONV1)
            .contentType(Formats.JSONV1)
            .body(tspData)
            .header(AUTH_HEADER, user.toHeaderValue())
            .queryParam(METHOD, StoreRule.REPLACE_ALL)
            .queryParam(OVERRIDE_PROTECTION, false)
            .queryParam(VERSION_DATE, tspInstance.getVersionDate().toString())
            .queryParam(PROFILE_DATA, tsProfileDataIndexed)
            .queryParam(VERSION, "Raw")
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .post("/timeseries/profile-instance/")
        .then()
            .log().ifValidationFails(LogDetail.ALL, true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_CREATED))
        ;

        // Delete instance
        given()
            .log().ifValidationFails(LogDetail.ALL, true)
            .accept(Formats.JSONV1)
            .contentType(Formats.JSONV1)
            .header(AUTH_HEADER, user.toHeaderValue())
            .queryParam(OFFICE, OFFICE_ID)
            .queryParam(VERSION_DATE, tspInstance.getVersionDate().toString())
            .queryParam(TIMEZONE, "UTC")
            .queryParam(OVERRIDE_PROTECTION, false)
            .queryParam(DATE, "2019-09-09T12:48:57Z")
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .delete("/timeseries/profile-instance/" + tspInstance.getTimeSeriesProfile().getLocationId().getName()
                    + "/" + tspInstance.getTimeSeriesProfile().getKeyParameter() + "/" + "Raw")
        .then()
            .log().ifValidationFails(LogDetail.ALL, true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_NO_CONTENT))
        ;

        // Retrieve instance and assert it does not exist
        given()
            .log().ifValidationFails(LogDetail.ALL, true)
            .accept(Formats.JSONV1)
            .contentType(Formats.JSONV1)
            .header(AUTH_HEADER, user.toHeaderValue())
            .queryParam(OFFICE, OFFICE_ID)
            .queryParam(VERSION_DATE, tspInstance.getVersionDate().toString())
            .queryParam(TIMEZONE, "UTC")
            .queryParam(START, "2018-07-09T19:06:20.00Z")
            .queryParam(END, "2025-07-09T19:06:20.00Z")
            .queryParam(START_TIME_INCLUSIVE, true)
            .queryParam(END_TIME_INCLUSIVE, true)
            .queryParam(UNIT, units)
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .get("/timeseries/profile-instance/" + tspInstance.getTimeSeriesProfile().getLocationId().getName()
                    + "/" + tspInstance.getTimeSeriesProfile().getKeyParameter() + "/" + "Raw")
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
            .accept(Formats.JSONV1)
            .contentType(Formats.JSONV1)
            .header(AUTH_HEADER, user.toHeaderValue())
            .queryParam(OFFICE, OFFICE_ID)
            .queryParam(PARAMETER_ID, tspInstance.getTimeSeriesProfile().getKeyParameter())
            .queryParam(VERSION, tspInstance.getVersion())
            .queryParam(VERSION_DATE, tspInstance.getVersionDate().toString())
            .queryParam(TIMEZONE, "UTC")
            .queryParam(OVERRIDE_PROTECTION, false)
            .queryParam(DATE, tspInstance.getFirstDate().toString())
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .delete("/timeseries/profile-instance/nonexistent/fakeParam/fakeVersion")
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
            .accept(Formats.JSONV1)
            .contentType(Formats.JSONV1)
            .body(tspData)
            .header(AUTH_HEADER, user.toHeaderValue())
            .queryParam(METHOD, StoreRule.REPLACE_ALL)
            .queryParam(OVERRIDE_PROTECTION, false)
            .queryParam(VERSION_DATE, "2024-07-09T12:00:00.00Z")
            .queryParam(PROFILE_DATA, tsProfileDataColumnar)
            .queryParam(VERSION, tspInstance.getVersion())
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .post("/timeseries/profile-instance/")
        .then()
            .log().ifValidationFails(LogDetail.ALL, true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_CREATED))
        ;

        // Create instance
        given()
            .log().ifValidationFails(LogDetail.ALL, true)
            .accept(Formats.JSONV1)
            .contentType(Formats.JSONV1)
            .body(tspData)
            .header(AUTH_HEADER, user.toHeaderValue())
            .queryParam(METHOD, StoreRule.REPLACE_ALL)
            .queryParam(OVERRIDE_PROTECTION, false)
            .queryParam(VERSION_DATE, "2024-07-09T12:00:00.00Z")
            .queryParam(PROFILE_DATA, tsProfileDataColumnar2)
            .queryParam(VERSION, tspInstance.getVersion())
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .post("/timeseries/profile-instance/")
        .then()
            .log().ifValidationFails(LogDetail.ALL, true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_CREATED))
        ;

        // Retrieve all instances
        given()
            .log().ifValidationFails(LogDetail.ALL, true)
            .accept(Formats.JSONV1)
            .contentType(Formats.JSONV1)
            .header(AUTH_HEADER, user.toHeaderValue())
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .get("/timeseries/profile-instance/")
        .then()
            .log().ifValidationFails(LogDetail.ALL, true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_OK))
            .body("[0].version", equalTo(tspInstance.getVersion()))
            .body("[0].version-date", equalTo(Instant.parse("2024-07-09T12:00:00.00Z").toEpochMilli()))
            .body("[0].time-series-profile.key-parameter", equalTo("Depth"))
            .body("[0].time-series-profile.parameter-list.size()", is(0))
            .body("[1].version", equalTo("DSS-Obs"))
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
            .accept(Formats.JSONV1)
            .contentType(Formats.JSONV1)
            .body(tspData)
            .header(AUTH_HEADER, user.toHeaderValue())
            .queryParam(METHOD, StoreRule.REPLACE_ALL)
            .queryParam(OVERRIDE_PROTECTION, false)
            .queryParam(VERSION_DATE, "2024-07-09T12:00:00.00Z")
            .queryParam(VERSION, "USGS-Raw")
            .queryParam(PROFILE_DATA, tsProfileDataIndexed)
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .post("/timeseries/profile-instance/")
        .then()
            .log().ifValidationFails(LogDetail.ALL, true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_CREATED))
        ;

        given()
            .log().ifValidationFails(LogDetail.ALL, true)
            .accept(Formats.JSONV1)
            .contentType(Formats.JSONV1)
            .body(tspData)
            .header(AUTH_HEADER, user.toHeaderValue())
            .queryParam(METHOD, StoreRule.REPLACE_ALL)
            .queryParam(OVERRIDE_PROTECTION, false)
            .queryParam(VERSION_DATE, "2024-07-09T12:00:00.00Z")
            .queryParam(PROFILE_DATA, tsProfileDataIndexed2)
            .queryParam(VERSION, "USGS-Raw")
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .post("/timeseries/profile-instance/")
        .then()
            .log().ifValidationFails(LogDetail.ALL, true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_CREATED))
        ;

        // Retrieve all instances
        given()
            .log().ifValidationFails(LogDetail.ALL, true)
            .accept(Formats.JSONV1)
            .contentType(Formats.JSONV1)
            .header(AUTH_HEADER, user.toHeaderValue())
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .get("/timeseries/profile-instance/")
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
            LOGGER.log(Level.CONFIG, "Cleanup failed for parser - no matching parser found in DB", e);
        }
    }

    private void cleanupTS(DSLContext dsl, String locationId, String keyParameter) {
        try {
            TimeSeriesProfileDao dao = new TimeSeriesProfileDao(dsl);
            dao.deleteTimeSeriesProfile(locationId, keyParameter, OFFICE_ID);
        } catch (NotFoundException e) {
            LOGGER.log(Level.CONFIG, "Cleanup failed for Timeseries - no matching TS found in DB", e);
        }
    }

    private void cleanupInstance(DSLContext dsl, CwmsId locationId, String keyParameter, String version,
            Instant firstDate, String timeZone, boolean overrideProt, Instant versionDate) {
        try {
            TimeSeriesProfileInstanceDao dao = new TimeSeriesProfileInstanceDao(dsl);
            dao.deleteTimeSeriesProfileInstance(locationId, keyParameter, version, firstDate, timeZone,
                    overrideProt, versionDate);
        } catch (NotFoundException e) {
            LOGGER.log(Level.CONFIG, "Cleanup failed for instance - no matching instance found in DB", e);
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
