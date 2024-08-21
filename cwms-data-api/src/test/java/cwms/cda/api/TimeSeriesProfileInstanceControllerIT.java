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
import java.time.Instant;

import static cwms.cda.api.Controllers.*;
import static cwms.cda.api.timeseriesprofile.TimeSeriesProfileInstanceController.*;
import static cwms.cda.security.KeyAccessManager.AUTH_HEADER;
import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertNotNull;


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
    private final InputStream profileResource = this.getClass()
            .getResourceAsStream("/cwms/cda/api/timeseriesprofile/ts_profile_data_columnar.txt");
    private TimeSeriesProfile tsProfile;
    private TimeSeriesProfileParserIndexed tspParserIndexed;
    private TimeSeriesProfileParserColumnar tspParserColumnar;
    private TimeSeriesProfileInstance tspInstance;
    private String tsProfileData;
    private String tspData;

    @BeforeEach
    public void setup() throws Exception {
        assertNotNull(resource);
        assertNotNull(resourceIndexed);
        assertNotNull(resourceInstance);
        assertNotNull(profileResource);
        assertNotNull(resourceColumnar);
        String tsDataInstance = IOUtils.toString(resourceInstance, StandardCharsets.UTF_8);
        tsProfileData = IOUtils.toString(profileResource, StandardCharsets.UTF_8);
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
            TimeSeriesProfileParserDao parserDao = new TimeSeriesProfileParserDao(dsl);
            parserDao.storeTimeSeriesProfileParser(tspParserIndexed, false);
            parserDao.storeTimeSeriesProfileParser(tspParserColumnar, false);
        }, CwmsDataApiSetupCallback.getWebUser());
    }

    @AfterEach
    public void tearDown() throws Exception {
        CwmsDatabaseContainer<?> db = CwmsDataApiSetupCallback.getDatabaseLink();
        db.connection(c -> {
            DSLContext dsl = dslContext(c, OFFICE_ID);
            cleanupTS(dsl, tsProfile.getLocationId().getName(), tsProfile.getKeyParameter());
            cleanupParser(dsl, tspParserIndexed.getLocationId().getName(),
                    tspParserIndexed.getKeyParameter());
            cleanupParser(dsl, tspParserColumnar.getLocationId().getName(),
                    tspParserColumnar.getKeyParameter());
            cleanupInstance(dsl, tspInstance.getTimeSeriesProfile().getLocationId(),
                    tspInstance.getTimeSeriesProfile().getKeyParameter(), tspInstance.getVersion(),
                    tspInstance.getFirstDate(), tspInstance.getTimeSeriesList().get(0).getTimeZone(),
                    false, tspInstance.getVersionDate());
        }, CwmsDataApiSetupCallback.getWebUser());
    }

    @Test
    void test_create_retrieve_TimeSeriesProfileInstance() {

        // Create instance
        given()
            .log().ifValidationFails(LogDetail.ALL, true)
            .accept(Formats.JSONV2)
            .contentType(Formats.JSONV2)
            .body(tspData)
            .header(AUTH_HEADER, user.toHeaderValue())
            .queryParam(METHOD, StoreRule.REPLACE_ALL)
            .queryParam(OVERRIDE_PROTECTION, true)
            .queryParam(VERSION_DATE, tspInstance.getVersionDate().toEpochMilli())
            .queryParam(PROFILE_DATA, tsProfileData)
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

        // Retrieve instance
        given()
            .log().ifValidationFails(LogDetail.ALL, true)
            .accept(Formats.JSONV2)
            .contentType(Formats.JSONV2)
            .header(AUTH_HEADER, user.toHeaderValue())
            .queryParam(OFFICE, OFFICE_ID)
            .queryParam(PARAMETER_ID, tspInstance.getTimeSeriesProfile().getKeyParameter())
            .queryParam(VERSION, tspInstance.getVersion())
            .queryParam(VERSION_DATE, tspInstance.getVersionDate().toEpochMilli())
            .queryParam(TIMEZONE, tspInstance.getTimeSeriesList().get(0).getTimeZone())
            .queryParam(START, tspInstance.getFirstDate().toEpochMilli())
            .queryParam(END, tspInstance.getLastDate().toEpochMilli())
            .queryParam(START_INCLUSIVE, true)
            .queryParam(END_INCLUSIVE, true)
            .queryParam(UNIT, "ft,F")
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .get("/timeseries/instance/" + tspInstance.getTimeSeriesProfile().getLocationId().getName())
        .then()
            .log().ifValidationFails(LogDetail.ALL, true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_OK))
        ;
    }

    @Test
    void test_delete_TimeSeriesProfileInstance() {
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
            .queryParam(PROFILE_DATA, tsProfileData)
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
            .queryParam(PARAMETER_ID, tspInstance.getTimeSeriesProfile().getKeyParameter())
            .queryParam(VERSION, tspInstance.getVersion())
            .queryParam(VERSION_DATE, tspInstance.getVersionDate().toEpochMilli())
            .queryParam(TIMEZONE, tspInstance.getTimeSeriesList().get(0).getTimeZone())
            .queryParam(OVERRIDE_PROTECTION, false)
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .delete("/timeseries/instance/" + tspInstance.getTimeSeriesProfile().getLocationId().getName())
        .then()
            .log().ifValidationFails(LogDetail.ALL, true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_NO_CONTENT))
        ;
    }

    @Test
    void test_delete_nonExistent_TimeSeriesProfileInstance() {
        // Retrieve instance
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
    void test_retrieve_all_TimeSeriesProfileInstance() {
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
            .queryParam(PROFILE_DATA, tsProfileData)
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
            .queryParam(VERSION_DATE, tspInstance.getVersionDate().toEpochMilli())
            .queryParam(PROFILE_DATA, tsProfileData)
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
        ;
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
}
