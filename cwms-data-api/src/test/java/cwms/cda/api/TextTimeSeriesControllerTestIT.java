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

import static cwms.cda.data.dao.DaoTest.getDslContext;
import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import com.google.common.flogger.FluentLogger;
import cwms.cda.formatters.Formats;
import cwms.cda.helpers.DateUtils;
import fixtures.CwmsDataApiSetupCallback;
import fixtures.TestAccounts;
import io.restassured.filter.log.LogDetail;
import io.restassured.response.ValidatableResponse;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.time.ZonedDateTime;
import javax.servlet.http.HttpServletResponse;
import mil.army.usace.hec.test.database.CwmsDatabaseContainer;
import org.apache.commons.io.IOUtils;
import org.jooq.DSLContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import usace.cwms.db.jooq.codegen.packages.CWMS_TS_PACKAGE;

@Tag("integration")
public class TextTimeSeriesControllerTestIT extends DataApiTestIT {
    private static final FluentLogger logger = FluentLogger.forEnclosingClass();

    public static final String OFFICE = "SPK";
    public static final String REG_LOAD_RESOURCE = "cwms/cda/data/sql/store_reg_text_timeseries.sql";
    public static final String REG_DELETE_RESOURCE = "cwms/cda/data/sql/delete_reg_text_timeseries.sql";

    public static final String EXPECTED_TEXT_VALUE = "my awesome text ts";  // must match
    // store_reg_text_timeseries.sql

    private static final String locationId = "TsTextTestLoc";
    private static final String tsId = locationId + ".Flow.Inst.1Hour.0.raw";

    public static final String AUTHORIZATION = "Authorization";

    @BeforeAll
    public static void create() throws Exception {
        createLocation(locationId, true, OFFICE);

        createTimeseries(OFFICE, tsId, 0);  // offset needs to be valid for 1Hour
        CwmsDatabaseContainer<?> databaseLink = CwmsDataApiSetupCallback.getDatabaseLink();
        databaseLink.connection(c -> {
            DSLContext dsl = getDslContext(c, OFFICE);
            CWMS_TS_PACKAGE.call_SET_TSID_VERSIONED(dsl.configuration(), tsId, "T", OFFICE);
        });

    }

    @BeforeEach
    public void load_data() throws Exception {

        loadSqlDataFromResource(REG_LOAD_RESOURCE);
    }

    @AfterEach
    public void deload_data() throws Exception {
        loadSqlDataFromResource(REG_DELETE_RESOURCE);

    }



    @Test
    void test_create_regular() throws Exception {



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
                .queryParam(Controllers.OFFICE, OFFICE)
                .queryParam(Controllers.NAME, tsId)
                .queryParam(Controllers.BEGIN,startStr)
                .queryParam(Controllers.END,endStr)
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
        InputStream resource = this.getClass().getResourceAsStream("/cwms/cda/api/spk/text_ts_create_reg.json");
        assertNotNull(resource);
        String tsData = IOUtils.toString(resource, StandardCharsets.UTF_8);
        assertNotNull(tsData);

        TestAccounts.KeyUser user = TestAccounts.KeyUser.SPK_NORMAL;
        given()
                .log().ifValidationFails(LogDetail.ALL,true)
                .accept(Formats.JSONV2)
                .contentType(Formats.JSONV2)
                .body(tsData)
                .header(AUTHORIZATION, user.toHeaderValue())
            .when()
                .redirects().follow(true)
                .redirects().max(3)
                .post("/timeseries/text")
            .then()
                .log().ifValidationFails(LogDetail.ALL,true)
                .assertThat()
                .statusCode(is(HttpServletResponse.SC_CREATED));

        //retrieve and verify
        given()
                .log().ifValidationFails(LogDetail.ALL,true)
                .accept(Formats.JSONV2)
                .queryParam(Controllers.OFFICE, OFFICE)
                .queryParam(Controllers.NAME, tsId)
                .queryParam(Controllers.BEGIN,startStr)
                .queryParam(Controllers.END,endStr)
            .when()
                .redirects().follow(true)
                .redirects().max(3)
                .get("/timeseries/text")
            .then()
                .log().ifValidationFails(LogDetail.ALL,true)
                .assertThat()
                .body("standard-text-catalog", nullValue())
                .body("standard-text-values", nullValue())
                .body("regular-text-values.size()", equalTo(1))
                .body("regular-text-values[0].text-value", equalTo("newly created text value"))
                .statusCode(is(HttpServletResponse.SC_OK));

    }

    @Test
    void test_retrieve_regular() {
        String startStr = "2005-01-01T03:00:00Z";
        String endStr = "2005-01-01T07:00:00Z";

        given()
                .log().ifValidationFails(LogDetail.ALL,true)
                .accept(Formats.JSONV2)
                .queryParam(Controllers.OFFICE, OFFICE)
                .queryParam(Controllers.NAME, tsId)
                .queryParam(Controllers.BEGIN,startStr)
                .queryParam(Controllers.END,endStr)
            .when()
                .redirects().follow(true)
                .redirects().max(3)
                .get("/timeseries/text")
            .then()
                .log().ifValidationFails(LogDetail.ALL,true)
                .assertThat()
                .body("standard-text-catalog", nullValue())
                .body("standard-text-values", nullValue())
                .body("regular-text-values.size()", equalTo(5))
                .statusCode(is(HttpServletResponse.SC_OK));

    }



    @Test
    void test_update_regular() throws Exception {
        // The basic structure of the test is to:
        // 1)retrieve and verify
        // 2)update
        // 3)retrieve and verify
        String startStr = "2005-01-01T03:00:00Z";
        String endStr = "2005-01-01T07:00:00Z";

        given()
                .log().ifValidationFails(LogDetail.ALL,true)
                .accept(Formats.JSONV2)
                .queryParam(Controllers.OFFICE, OFFICE)
                .queryParam(Controllers.NAME, tsId)
                .queryParam(Controllers.BEGIN,startStr)
                .queryParam(Controllers.END,endStr)
                .when()
                .redirects().follow(true)
                .redirects().max(3)
                .get("/timeseries/text")
                .then()
                .log().ifValidationFails(LogDetail.ALL,true)
                .assertThat()
                .body("standard-text-catalog", nullValue())
                .body("standard-text-values", nullValue())
                .body("regular-text-values.size()", equalTo(5))
                .statusCode(is(HttpServletResponse.SC_OK));


        //2) update
        InputStream resource = this.getClass().getResourceAsStream("/cwms/cda/api/spk/text_ts_update_reg.json");
        assertNotNull(resource);
        String tsData = IOUtils.toString(resource, StandardCharsets.UTF_8);
        assertNotNull(tsData);
        TestAccounts.KeyUser user = TestAccounts.KeyUser.SPK_NORMAL;
        given()
                .log().ifValidationFails(LogDetail.ALL,true)
                .accept(Formats.JSONV2)
                .queryParam(TextTimeSeriesController.REPLACE_ALL, "true")
                .contentType(Formats.JSONV2)
                .body(tsData)
                .header(AUTHORIZATION, user.toHeaderValue())
            .when()
                .redirects().follow(true)
                .redirects().max(3)
                .patch("/timeseries/text/" + tsId)
            .then()
                .log().ifValidationFails(LogDetail.ALL,true)
                .assertThat()
                .statusCode(is(HttpServletResponse.SC_OK));

        //3)retrieve and verify
        ValidatableResponse response = given()
                .log().ifValidationFails(LogDetail.ALL,true)
                .accept(Formats.JSONV2)
                .queryParam(Controllers.OFFICE, OFFICE)
                .queryParam(Controllers.NAME, tsId)
                .queryParam(Controllers.BEGIN,startStr)
                .queryParam(Controllers.END,endStr)
            .when()
                .redirects().follow(true)
                .redirects().max(3)
                .get("/timeseries/text")
            .then()
                .log().ifValidationFails(LogDetail.ALL,true)
                .assertThat()
                .body("standard-text-catalog", nullValue())
                .body("standard-text-values", nullValue())
                .body("regular-text-values.size()", equalTo(5))
                .body("regular-text-values[0].text-value", equalTo("still great"))
                .statusCode(is(HttpServletResponse.SC_OK));

    }


    @Test
    void test_delete_regular() {
        // Structure of the test is:
        // 1) retrieve some data and verify its there
        // 2) delete it
        // 3) retrieve it again and verify its gone


        // Step 1: retrieve some data
        String startStr = "2005-01-01T03:00:00Z";
        String endStr = "2005-01-01T04:00:00Z";

        ZonedDateTime startZdt = DateUtils.parseUserDate(startStr, "UTC");
        ZonedDateTime endZdt = DateUtils.parseUserDate(endStr, "UTC");

        // 1)retrieve and verify
        given()
                .log().ifValidationFails(LogDetail.ALL, true)
                .accept(Formats.JSONV2)
                .queryParam(Controllers.OFFICE, OFFICE)
                .queryParam(Controllers.NAME, tsId)
                .queryParam(Controllers.BEGIN,startStr)
                .queryParam(Controllers.END,endStr)
            .when()
                .redirects().follow(true)
                .redirects().max(3)
                .get("/timeseries/text")
            .then()
                .log().ifValidationFails(LogDetail.ALL, true)
                .assertThat()
                .body("standard-text-catalog", nullValue())
                .body("standard-text-values", nullValue())
                .body("regular-text-values.size()", equalTo(2))
                // assert the first regular-text-value item is as expected
                .body("regular-text-values[0].text-value", equalTo(EXPECTED_TEXT_VALUE))
                .statusCode(is(HttpServletResponse.SC_OK));

        // Step 2: delete it
        TestAccounts.KeyUser user = TestAccounts.KeyUser.SPK_NORMAL;
        given()
                .log().ifValidationFails(LogDetail.ALL, true)
                .accept(Formats.JSONV2)
                .header(AUTHORIZATION, user.toHeaderValue())
                .queryParam(Controllers.OFFICE, OFFICE)
                .queryParam(Controllers.NAME, tsId)
                .queryParam(Controllers.TEXT_MASK, "*")
                .queryParam(Controllers.BEGIN, startStr)
                .queryParam(Controllers.END, endStr)
            .when()
                .redirects().follow(true)
                .redirects().max(3)
                .delete("/timeseries/text/" + tsId )
            .then()
                .log().ifValidationFails(LogDetail.ALL, true)
                .assertThat()
                .statusCode(is(HttpServletResponse.SC_NO_CONTENT));


        // Step 3: retrieve it again and verify its gone
        given()
                .log().ifValidationFails(LogDetail.ALL, true)
                .accept(Formats.JSONV2)
                .queryParam(Controllers.OFFICE, OFFICE)
                .queryParam(Controllers.NAME, tsId)
                .queryParam(Controllers.BEGIN,startStr)
                .queryParam(Controllers.END,endStr)
            .when()
                .redirects().follow(true)
                .redirects().max(3)
                .get("/timeseries/text")
            .then()
                .log().ifValidationFails(LogDetail.ALL, true)
                .assertThat()
                .body("standard-text-catalog", nullValue())
                .body("standard-text-values", nullValue())
                .body("regular-text-values.size()", equalTo(0))
                .statusCode(is(HttpServletResponse.SC_OK));

    }


}
