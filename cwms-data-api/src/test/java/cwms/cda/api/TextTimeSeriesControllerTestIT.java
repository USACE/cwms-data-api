/*
 * MIT License
 *
 * Copyright (c) 2023 Hydrologic Engineering Center
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

import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import com.google.common.flogger.FluentLogger;
import cwms.cda.data.dao.texttimeseries.TimeSeriesTextMode;
import cwms.cda.formatters.Formats;
import fixtures.TestAccounts;
import io.javalin.core.validation.JavalinValidation;
import io.restassured.filter.log.LogDetail;
import java.nio.charset.StandardCharsets;
import javax.servlet.http.HttpServletResponse;
import org.apache.commons.io.IOUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag("integration")
public class TextTimeSeriesControllerTestIT extends DataApiTestIT {

    public static final String OFFICE = "SPK";
    private static final FluentLogger logger = FluentLogger.forEnclosingClass();
    public static final String REG_LOAD_RESOURCE = "cwms/cda/data/sql/store_reg_text_timeseries.sql";
    public static final String REG_DELETE_RESOURCE = "cwms/cda/data/sql/delete_reg_text_timeseries.sql";
    public static final String STD_LOAD_RESOURCE = "cwms/cda/data/sql/store_std_text_timeseries.sql";
    public static final String STD_DELETE_RESOURCE = "cwms/cda/data/sql/delete_std_text_timeseries.sql";
    public static final String EXPECTED_TEXT_VALUE = "my awesome text ts";  // must match
    // store_reg_text_timeseries.sql

    private static final String locationId = "TsTextTestLoc";
    private static final String tsId = locationId + ".Flow.Inst.1Hour.0.raw";

    @BeforeAll
    public static void create() throws Exception {
        createLocation(locationId, true, OFFICE);

        createTimeseries(OFFICE, tsId, 0);  // offset needs to be valid for 1Hour
    }

    @BeforeEach
    public void load_data() throws Exception {
        loadSqlDataFromResource(REG_LOAD_RESOURCE);
        loadSqlDataFromResource(STD_LOAD_RESOURCE);
    }

    @AfterEach
    public void deload_data() throws Exception {
        loadSqlDataFromResource(REG_DELETE_RESOURCE);
        loadSqlDataFromResource(STD_DELETE_RESOURCE);
    }


    @Test
    void test_create_standard() throws Exception {
        String tsData = IOUtils.toString(this.getClass().getResourceAsStream("/cwms/cda/api/spk/text_ts_create_std.json"), StandardCharsets.UTF_8);
        assertNotNull(tsData);

        JavalinValidation.register(TimeSeriesTextMode.class, TimeSeriesTextMode::getMode);

        // The basic structure of the test is to:
        // 1.  make sure the row doesn't exist
        // 2.  create/store the row
        // 3.  retrieve the row and verify it is there

        // make sure it doesn't exist
        // this will return 200 but the lists should be empty.
        given()
            .log().ifValidationFails(LogDetail.ALL,true)
            .accept(Formats.JSONV2)
            .queryParam("office", OFFICE)
            .queryParam("ts-id", tsId)
            .queryParam("start","2005-02-01T15:00:00Z")
            .queryParam("end","2005-02-01T23:00:00Z")
                .queryParam("max-version","false")
                .queryParam("mode", "STANDARD")
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .get("/timeseries/text")
        .then()
            .log().ifValidationFails(LogDetail.ALL,true)
            .assertThat()
                .body("standard-text-values", is(empty()))
                .body("regular-text-values", nullValue())
            .statusCode(is(HttpServletResponse.SC_OK));

        // create
        TestAccounts.KeyUser user = TestAccounts.KeyUser.SPK_NORMAL;
        given()
            .log().ifValidationFails(LogDetail.ALL,true)
            .accept(Formats.JSONV2)
            .contentType(Formats.JSONV2)
            .body(tsData)
            .header("Authorization", user.toHeaderValue())
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .post("/timeseries/text")
        .then()
            .log().ifValidationFails(LogDetail.ALL,true)
            .assertThat()
            .statusCode(is(HttpServletResponse.SC_ACCEPTED));

        //retrieve and verify
        given()
                .log().ifValidationFails(LogDetail.ALL,true)
                .accept(Formats.JSONV2)
                .queryParam("office", OFFICE)
                .queryParam("ts-id", tsId)
                .queryParam("start","2005-02-01T15:00:00Z")
                .queryParam("end","2005-02-01T23:00:00Z")
                .queryParam("max-version","false")
                .queryParam("mode", "STANDARD")
            .when()
                .redirects().follow(true)
                .redirects().max(3)
                .get("/timeseries/text")
            .then()
                .log().ifValidationFails(LogDetail.ALL,true)
                .assertThat()
                .body("standard-text-values.size()", equalTo(1))
                .body("standard-text-catalog.size()", equalTo(1))
                .statusCode(is(HttpServletResponse.SC_OK));

    }

    @Test
    void test_create_regular() throws Exception {
        String tsData = IOUtils.toString(this.getClass().getResourceAsStream("/cwms/cda/api/spk/text_ts_create_reg.json"), StandardCharsets.UTF_8);
        assertNotNull(tsData);

        JavalinValidation.register(TimeSeriesTextMode.class, TimeSeriesTextMode::getMode);

        // The basic structure of the test is to:
        // 1.  make sure the row doesn't exist
        // 2.  create/store the row
        // 3.  retrieve the row and verify it is there

        // make sure it doesn't exist
        // this will return 200 but the lists should be empty.
        String startStr = "2005-01-01T08:00:00Z";
        String endStr = "2005-01-01T14:00:00Z";
        given()
                .log().ifValidationFails(LogDetail.ALL,true)
                .accept(Formats.JSONV2)
                .queryParam("office", OFFICE)
                .queryParam("ts-id", tsId)
                .queryParam("start", startStr)
                .queryParam("end", endStr)
                .queryParam("max-version","false")
                .queryParam("mode", "REGULAR")
                .when()
                .redirects().follow(true)
                .redirects().max(3)
                .get("/timeseries/text")
                .then()
                .log().ifValidationFails(LogDetail.ALL,true)
                .assertThat()
                .body("standard-text-values", nullValue())
                .body("regular-text-values", is(empty()))
                .statusCode(is(HttpServletResponse.SC_OK));

        // create
        TestAccounts.KeyUser user = TestAccounts.KeyUser.SPK_NORMAL;
        given()
                .log().ifValidationFails(LogDetail.ALL,true)
                .accept(Formats.JSONV2)
                .contentType(Formats.JSONV2)
                .body(tsData)
                .header("Authorization", user.toHeaderValue())
                .when()
                .redirects().follow(true)
                .redirects().max(3)
                .post("/timeseries/text")
                .then()
                .log().ifValidationFails(LogDetail.ALL,true)
                .assertThat()
                .statusCode(is(HttpServletResponse.SC_ACCEPTED));

        //retrieve and verify
        given()
                .log().ifValidationFails(LogDetail.ALL,true)
                .accept(Formats.JSONV2)
                .queryParam("office", OFFICE)
                .queryParam("ts-id", tsId)
                .queryParam("start",startStr)
                .queryParam("end",endStr)
                .queryParam("max-version","false")
                .queryParam("mode", "REGULAR")
                .when()
                .redirects().follow(true)
                .redirects().max(3)
                .get("/timeseries/text")
                .then()
                .log().ifValidationFails(LogDetail.ALL,true)
                .assertThat()
                .body("regular-text-values.size()", equalTo(1))
                .body("standard-text-catalog", nullValue())
                .body("standard-text-values", nullValue())
                .statusCode(is(HttpServletResponse.SC_OK));

    }

}
