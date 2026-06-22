/*
 * MIT License
 *
 * Copyright (c) 2026 Hydrologic Engineering Center
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

import static cwms.cda.api.Controllers.BEGIN;
import static cwms.cda.api.Controllers.END;
import static cwms.cda.api.Controllers.NAME;
import static cwms.cda.api.Controllers.OFFICE;
import static cwms.cda.api.Controllers.PAGE_SIZE;
import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import cwms.cda.formatters.Formats;
import fixtures.TestAccounts;
import io.restassured.filter.log.LogDetail;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.sql.Timestamp;
import java.time.ZonedDateTime;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.io.IOUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
final class TimeSeriesVersionsControllerTestIT extends DataApiTestIT {

    private static final String OFFICE_ID = "SPK";
    private static final String TS_ID = "TestTS.Temp-Water.Inst.1Day.0.cda-test";

    @BeforeAll
    void setup() throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        InputStream resource = this.getClass().getResourceAsStream(
                "/cwms/cda/api/lrl/1day_offset_version_date.json");
        assertNotNull(resource);
        String tsData = IOUtils.toString(resource, StandardCharsets.UTF_8);
        tsData = tsData.replace("Buckhorn", TS_ID.split("\\.")[0]);

        JsonNode ts = mapper.readTree(tsData);
        String location = ts.get(NAME).asText().split("\\.")[0];
        String officeId = ts.get("office-id").asText();

        createLocation(location, true, officeId);

        TestAccounts.KeyUser user = TestAccounts.KeyUser.SPK_NORMAL;

        // inserting the time series
        given()
            .log().ifValidationFails(LogDetail.ALL, true)
            .accept(Formats.JSONV2)
            .contentType(Formats.JSONV2)
            .body(tsData)
            .header("Authorization",user.toHeaderValue())
            .queryParam(OFFICE, officeId)
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .post("/timeseries/")
        .then()
            .log().ifValidationFails(LogDetail.ALL,true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_OK));

        String secondVersionDate = "1604786000000";
        tsData = tsData.replace("1594786000000", secondVersionDate).replace("35,", "47.5,");
        // inserting the second time series
        given()
            .log().ifValidationFails(LogDetail.ALL, true)
            .accept(Formats.JSONV2)
            .contentType(Formats.JSONV2)
            .body(tsData)
            .header("Authorization",user.toHeaderValue())
            .queryParam(OFFICE, officeId)
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .post("/timeseries/")
        .then()
            .log().ifValidationFails(LogDetail.ALL,true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_OK));
    }

    @AfterAll
    void cleanup() throws Exception {
        TestAccounts.KeyUser user = TestAccounts.KeyUser.SPK_NORMAL;
        given()
            .log().ifValidationFails(LogDetail.ALL, true)
            .accept(Formats.JSONV2)
            .contentType(Formats.JSONV2)
            .header("Authorization",user.toHeaderValue())
            .queryParam(OFFICE, OFFICE_ID)
            .queryParam(BEGIN, "2019-07-15T00:00:00Z")
            .queryParam(END,"2024-07-15T00:00:00Z")
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .delete("/timeseries/" + TS_ID)
        .then()
            .log().ifValidationFails(LogDetail.ALL,true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_OK));

        deleteLocation(TS_ID.split("\\.")[0], OFFICE_ID);
    }

    @Test
    void test_get_versions() {
        given()
            .log().ifValidationFails(LogDetail.ALL, true)
            .accept(Formats.JSONV1)
            .queryParam(NAME, TS_ID)
            .queryParam(OFFICE, OFFICE_ID)
        .when()
            .get("/timeseries/versions/")
        .then()
            .log().ifValidationFails(LogDetail.ALL, true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_OK))
            .body("ts-id.name", equalTo(TS_ID))
            .body("ts-id.office-id", equalTo(OFFICE_ID))
            .body("versions", notNullValue())
            .body("versions.size()", is(2));
    }

    @Test
    void test_get_versions_filtered() {
        given()
            .log().ifValidationFails(LogDetail.ALL, true)
            .accept(Formats.JSONV1)
            .queryParam(NAME, TS_ID)
            .queryParam(OFFICE, OFFICE_ID)
            .queryParam(BEGIN, "2020-07-15T00:00:00Z")
            .queryParam(END, "2020-07-16T00:00:00Z")
        .when()
            .get("/timeseries/versions/")
        .then()
            .log().ifValidationFails(LogDetail.ALL, true)
            .assertThat()
            .statusCode(is(HttpServletResponse.SC_OK))
            .body("versions.size()", is(1))
            .body("versions[0].version-time", equalTo("2020-07-15T04:06:40Z"));
    }

    @Test
    void test_get_versions_not_found() {
        given()
            .log().ifValidationFails(LogDetail.ALL, true)
            .accept(Formats.JSONV1)
            .queryParam(NAME, "NonExistent.Flow.Inst.1Hour.0.Test")
            .queryParam(OFFICE, OFFICE_ID)
        .when()
            .get("/timeseries/versions/")
        .then()
            .log().ifValidationFails(LogDetail.ALL, true)
            .assertThat()
            .statusCode(is(HttpServletResponse.SC_NOT_FOUND));
    }

    @Test
    void test_get_versions_pagination() {
        // Page 1
        String nextPage = given()
            .log().ifValidationFails(LogDetail.ALL, true)
            .accept(Formats.JSONV1)
            .queryParam(NAME, TS_ID)
            .queryParam(OFFICE, OFFICE_ID)
            .queryParam(PAGE_SIZE, 1)
        .when()
            .get("/timeseries/versions/")
        .then()
            .log().ifValidationFails(LogDetail.ALL, true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_OK))
            .body("versions.size()", is(1))
            .body("next-page", notNullValue())
            .extract().path("next-page");

        // Page 2
        given()
            .log().ifValidationFails(LogDetail.ALL, true)
            .accept(Formats.JSONV1)
            .queryParam(NAME, TS_ID)
            .queryParam(OFFICE, OFFICE_ID)
            .queryParam("page", nextPage)
        .when()
            .get("/timeseries/versions/")
        .then()
            .log().ifValidationFails(LogDetail.ALL, true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_OK))
            .body("versions.size()", is(1))
            .body("next-page", nullValue());
    }
}
