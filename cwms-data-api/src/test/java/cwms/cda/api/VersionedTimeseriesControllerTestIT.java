package cwms.cda.api;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import javax.servlet.http.HttpServletResponse;

import com.fasterxml.jackson.databind.ObjectMapper;
import cwms.cda.api.enums.VersionType;
import cwms.cda.data.dto.TimeSeries;
import cwms.cda.formatters.Formats;
import cwms.cda.formatters.json.JsonV2;
import fixtures.TestAccounts;

import io.javalin.core.validation.JavalinValidation;
import io.restassured.filter.log.LogDetail;
import org.apache.commons.io.IOUtils;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import static cwms.cda.api.Controllers.BEGIN;
import static cwms.cda.api.Controllers.END;
import static cwms.cda.api.Controllers.NAME;
import static cwms.cda.api.Controllers.OFFICE;
import static cwms.cda.api.Controllers.UNIT;
import static cwms.cda.api.Controllers.VERSION_DATE;
import static cwms.cda.api.Controllers.VERSION_TYPE;

@Tag("integration")
public class VersionedTimeseriesControllerTestIT extends DataApiTestIT {

    private static final String OFFICE_STR = "SWT";
    private static final String locationId = "TsVersionedTestLoc";
    private static final String tsId = locationId + ".Flow.Inst.1Hour.0.raw";
    private static final String BEGIN_STR = "2008-05-01T15:00:00Z";
    private static final String END_STR = "2008-05-01T17:00:00Z";


    @BeforeAll
    public static void create() throws Exception {
        createLocation(locationId, true, OFFICE_STR);
        createTimeseries(OFFICE_STR, tsId, 0);
    }

    @Test
    void test_create_get_delete_get_versioned() throws Exception {
        // Post versioned time series
        InputStream resource = this.getClass().getResourceAsStream("/cwms/cda/api/swt/num_ts_create.json");
        assertNotNull(resource);
        String tsData = IOUtils.toString(resource, StandardCharsets.UTF_8);
        assertNotNull(tsData);

        // Map json to object
        TimeSeries ts;
        ObjectMapper om = JsonV2.buildObjectMapper();
        ts = om.readValue(tsData, TimeSeries.class);

        TestAccounts.KeyUser user = TestAccounts.KeyUser.SWT_NORMAL;

        given()
                .log().ifValidationFails(LogDetail.ALL,true)
                .accept(Formats.JSONV2)
                .contentType(Formats.JSONV2)
                .body(tsData)
                .header("Authorization", user.toHeaderValue())
                .when()
                .redirects().follow(true)
                .redirects().max(3)
                .post("/timeseries/")
                .then()
                .log().ifValidationFails(LogDetail.ALL,true)
                .assertThat()
                .statusCode(is(HttpServletResponse.SC_OK));

        // Get versioned time series
        given()
                .log().ifValidationFails(LogDetail.ALL,true)
                .accept(Formats.JSONV2)
                .contentType(Formats.JSONV2)
                .header("Authorization", user.toHeaderValue())
                .queryParam(OFFICE, ts.getOfficeId())
                .queryParam(NAME, ts.getName())
                .queryParam(UNIT, ts.getUnits())
                .queryParam(VERSION_DATE, ts.getVersionDate().toString())
                .queryParam(BEGIN, BEGIN_STR)
                .queryParam(END, END_STR)
                .queryParam(VERSION_TYPE, VersionType.SINGLE_VERSION.getValue())
                .when()
                .redirects().follow(true)
                .redirects().max(3)
                .get("/timeseries/")
                .then()
                .log().ifValidationFails(LogDetail.ALL,true)
                .assertThat()
                .statusCode(is(HttpServletResponse.SC_OK))
                .body("values.size()", equalTo(3))
                .body("values[0][1]", equalTo(1.0F))
                .body("values[1][1]", equalTo(1.0F))
                .body("values[2][1]", equalTo(1.0F))
                .body("version-date", equalTo("2021-06-21T08:00:00+0000[UTC]"))
                .body("date-version-type", equalTo(VersionType.SINGLE_VERSION.getValue()));

        // Delete all values from timeseries
        given()
                .log().ifValidationFails(LogDetail.ALL,true)
                .accept(Formats.JSONV2)
                .contentType(Formats.JSONV2)
                .header("Authorization", user.toHeaderValue())
                .queryParam(OFFICE, ts.getOfficeId())
                .queryParam(NAME, ts.getName())
                .queryParam(UNIT, ts.getUnits())
                .queryParam(VERSION_DATE, ts.getVersionDate().toString())
                .queryParam(BEGIN, BEGIN_STR)
                .queryParam(END, "2008-05-01T18:00:00Z") // need to increment end by one hour to delete all values
                .queryParam(VERSION_TYPE, VersionType.SINGLE_VERSION.getValue())
                .when()
                .redirects().follow(true)
                .redirects().max(3)
                .delete("/timeseries/" + ts.getName())
                .then()
                .log().ifValidationFails(LogDetail.ALL,true)
                .assertThat()
                .statusCode(is(HttpServletResponse.SC_OK));

        // Get versioned time series
        given()
                .log().ifValidationFails(LogDetail.ALL,true)
                .accept(Formats.JSONV2)
                .contentType(Formats.JSONV2)
                .header("Authorization", user.toHeaderValue())
                .queryParam(OFFICE, ts.getOfficeId())
                .queryParam(NAME, ts.getName())
                .queryParam(UNIT, ts.getUnits())
                .queryParam(VERSION_DATE, ts.getVersionDate().toString())
                .queryParam(BEGIN, BEGIN_STR)
                .queryParam(END, END_STR) // put old end date back in map
                .queryParam(VERSION_TYPE, VersionType.SINGLE_VERSION.getValue())
                .when()
                .redirects().follow(true)
                .redirects().max(3)
                .get("/timeseries/")
                .then()
                .log().ifValidationFails(LogDetail.ALL,true)
                .assertThat()
                .statusCode(is(HttpServletResponse.SC_OK))
                .body("values.size()", equalTo(3))
                .body("values[0][1]", equalTo(null))
                .body("values[1][1]", equalTo(null))
                .body("values[2][1]", equalTo(null))
                .body("version-date", equalTo("2021-06-21T08:00:00+0000[UTC]"))
                .body("date-version-type", equalTo(VersionType.SINGLE_VERSION.getValue()));
    }

    @Test
    void test_create_get_param_conflict() throws Exception {
        InputStream resource = this.getClass().getResourceAsStream("/cwms/cda/api/swt/num_ts_create.json");
        assertNotNull(resource);
        String tsData = IOUtils.toString(resource, StandardCharsets.UTF_8);
        assertNotNull(tsData);

        TimeSeries ts;
        ObjectMapper om = JsonV2.buildObjectMapper();
        ts = om.readValue(tsData, TimeSeries.class);

        TestAccounts.KeyUser user = TestAccounts.KeyUser.SWT_NORMAL;

        given()
                .log().ifValidationFails(LogDetail.ALL,true)
                .accept(Formats.JSONV2)
                .contentType(Formats.JSONV2)
                .body(tsData)
                .header("Authorization", user.toHeaderValue())
                .when()
                .redirects().follow(true)
                .redirects().max(3)
                .post("/timeseries/")
                .then()
                .log().ifValidationFails(LogDetail.ALL,true)
                .assertThat()
                .statusCode(is(HttpServletResponse.SC_OK));

        // Get versioned time series with bad request parameters
        // Cannot call MAX_AGGREGATE version type with a non-null version date
        given()
                .log().ifValidationFails(LogDetail.ALL,true)
                .accept(Formats.JSONV2)
                .contentType(Formats.JSONV2)
                .header("Authorization", user.toHeaderValue())
                .queryParam(OFFICE, ts.getOfficeId())
                .queryParam(NAME, ts.getName())
                .queryParam(UNIT, ts.getUnits())
                .queryParam(VERSION_DATE, ts.getVersionDate().toString())
                .queryParam(BEGIN, BEGIN_STR)
                .queryParam(END, END_STR)
                .queryParam(VERSION_TYPE, VersionType.MAX_AGGREGATE.getValue())
                .when()
                .redirects().follow(true)
                .redirects().max(3)
                .get("/timeseries/")
                .then()
                .log().ifValidationFails(LogDetail.ALL,true)
                .assertThat()
                .statusCode(is(HttpServletResponse.SC_BAD_REQUEST));


        // Get versioned time series with bad request parameters
        // Cannot call SINGLE_VERSION version type with null version date
        given()
                .log().ifValidationFails(LogDetail.ALL,true)
                .accept(Formats.JSONV2)
                .contentType(Formats.JSONV2)
                .header("Authorization", user.toHeaderValue())
                .queryParam(OFFICE, ts.getOfficeId())
                .queryParam(NAME, ts.getName())
                .queryParam(UNIT, ts.getUnits())
                .queryParam(BEGIN, BEGIN_STR)
                .queryParam(END, END_STR)
                .queryParam(VERSION_TYPE, VersionType.SINGLE_VERSION.getValue())
                .when()
                .redirects().follow(true)
                .redirects().max(3)
                .get("/timeseries/")
                .then()
                .log().ifValidationFails(LogDetail.ALL,true)
                .assertThat()
                .statusCode(is(HttpServletResponse.SC_BAD_REQUEST));
    }

    @Test
    void test_create_get_update_get_delete_get_versioned() throws Exception {
        // Post versioned time series
        InputStream resource = this.getClass().getResourceAsStream("/cwms/cda/api/swt/num_ts_create.json");
        assertNotNull(resource);
        String tsData = IOUtils.toString(resource, StandardCharsets.UTF_8);
        assertNotNull(tsData);

        TimeSeries ts;
        ObjectMapper om = JsonV2.buildObjectMapper();
        ts = om.readValue(tsData, TimeSeries.class);

        TestAccounts.KeyUser user = TestAccounts.KeyUser.SWT_NORMAL;

        given()
                .log().ifValidationFails(LogDetail.ALL,true)
                .accept(Formats.JSONV2)
                .contentType(Formats.JSONV2)
                .body(tsData)
                .header("Authorization", user.toHeaderValue())
                .when()
                .redirects().follow(true)
                .redirects().max(3)
                .post("/timeseries/")
                .then()
                .log().ifValidationFails(LogDetail.ALL,true)
                .assertThat()
                .statusCode(is(HttpServletResponse.SC_OK));


        // Get versioned time series
        given()
                .log().ifValidationFails(LogDetail.ALL,true)
                .accept(Formats.JSONV2)
                .contentType(Formats.JSONV2)
                .header("Authorization", user.toHeaderValue())
                .queryParam(OFFICE, ts.getOfficeId())
                .queryParam(NAME, ts.getName())
                .queryParam(UNIT, ts.getUnits())
                .queryParam(VERSION_DATE, ts.getVersionDate().toString())
                .queryParam(BEGIN, BEGIN_STR)
                .queryParam(END, END_STR)
                .queryParam(VERSION_TYPE, VersionType.SINGLE_VERSION.getValue())
                .when()
                .redirects().follow(true)
                .redirects().max(3)
                .get("/timeseries/")
                .then()
                .log().ifValidationFails(LogDetail.ALL,true)
                .assertThat()
                .statusCode(is(HttpServletResponse.SC_OK))
                .body("values.size()", equalTo(3))
                .body("values[0][1]", equalTo(1.0F))
                .body("values[1][1]", equalTo(1.0F))
                .body("values[2][1]", equalTo(1.0F))
                .body("version-date", equalTo("2021-06-21T08:00:00+0000[UTC]"))
                .body("date-version-type", equalTo(VersionType.SINGLE_VERSION.getValue()));

        // Update versioned time series
        InputStream updated_resource = this.getClass().getResourceAsStream("/cwms/cda/api/swt/num_ts_update.json");
        assertNotNull(updated_resource);
        String tsUpdatedData = IOUtils.toString(updated_resource, StandardCharsets.UTF_8);
        assertNotNull(tsUpdatedData);

        given()
                .log().ifValidationFails(LogDetail.ALL,true)
                .accept(Formats.JSONV2)
                .contentType(Formats.JSONV2)
                .body(tsUpdatedData)
                .header("Authorization", user.toHeaderValue())
                .when()
                .redirects().follow(true)
                .redirects().max(3)
                .patch("/timeseries/" + ts.getName())
                .then()
                .log().ifValidationFails(LogDetail.ALL,true)
                .assertThat()
                .statusCode(is(HttpServletResponse.SC_OK));


        // Get updated versioned time series
        given()
                .log().ifValidationFails(LogDetail.ALL,true)
                .accept(Formats.JSONV2)
                .contentType(Formats.JSONV2)
                .header("Authorization", user.toHeaderValue())
                .queryParam(OFFICE, ts.getOfficeId())
                .queryParam(NAME, ts.getName())
                .queryParam(UNIT, ts.getUnits())
                .queryParam(VERSION_DATE, ts.getVersionDate().toString())
                .queryParam(BEGIN, BEGIN_STR)
                .queryParam(END, END_STR)
                .queryParam(VERSION_TYPE, VersionType.SINGLE_VERSION.getValue())
                .when()
                .redirects().follow(true)
                .redirects().max(3)
                .get("/timeseries/")
                .then()
                .log().ifValidationFails(LogDetail.ALL,true)
                .assertThat()
                .statusCode(is(HttpServletResponse.SC_OK))
                .body("values.size()", equalTo(3))
                .body("values[0][1]", equalTo(2.0F))
                .body("values[1][1]", equalTo(2.0F))
                .body("values[2][1]", equalTo(2.0F))
                .body("version-date", equalTo("2021-06-21T08:00:00+0000[UTC]"))
                .body("date-version-type", equalTo(VersionType.SINGLE_VERSION.getValue()));;

        // Delete all values from timeseries
        given()
                .log().ifValidationFails(LogDetail.ALL,true)
                .accept(Formats.JSONV2)
                .contentType(Formats.JSONV2)
                .header("Authorization", user.toHeaderValue())
                .queryParam(OFFICE, ts.getOfficeId())
                .queryParam(NAME, ts.getName())
                .queryParam(UNIT, ts.getUnits())
                .queryParam(VERSION_DATE, ts.getVersionDate().toString())
                .queryParam(BEGIN, BEGIN_STR)
                .queryParam(END, "2008-05-01T18:00:00Z")
                .queryParam(VERSION_TYPE, VersionType.SINGLE_VERSION.getValue())
                .when()
                .redirects().follow(true)
                .redirects().max(3)
                .delete("/timeseries/" + ts.getName())
                .then()
                .log().ifValidationFails(LogDetail.ALL,true)
                .assertThat()
                .statusCode(is(HttpServletResponse.SC_OK));

        // Get versioned time series
        given()
                .log().ifValidationFails(LogDetail.ALL,true)
                .accept(Formats.JSONV2)
                .contentType(Formats.JSONV2)
                .header("Authorization", user.toHeaderValue())
                .queryParam(OFFICE, ts.getOfficeId())
                .queryParam(NAME, ts.getName())
                .queryParam(UNIT, ts.getUnits())
                .queryParam(VERSION_DATE, ts.getVersionDate().toString())
                .queryParam(BEGIN, BEGIN_STR)
                .queryParam(END, END_STR)
                .queryParam(VERSION_TYPE, VersionType.SINGLE_VERSION.getValue())
                .when()
                .redirects().follow(true)
                .redirects().max(3)
                .get("/timeseries/")
                .then()
                .log().ifValidationFails(LogDetail.ALL,true)
                .assertThat()
                .statusCode(is(HttpServletResponse.SC_OK))
                .body("values.size()", equalTo(3))
                .body("values[0][1]", equalTo(null))
                .body("values[1][1]", equalTo(null))
                .body("values[2][1]", equalTo(null))
                .body("version-date", equalTo("2021-06-21T08:00:00+0000[UTC]"))
                .body("date-version-type", equalTo(VersionType.SINGLE_VERSION.getValue()));
    }

    @Test
    void test_create_get_update_get_delete_get_unversioned() throws Exception {
        // Post versioned time series
        InputStream resource = this.getClass().getResourceAsStream("/cwms/cda/api/swt/num_ts_create_unversioned.json");
        assertNotNull(resource);
        String tsData = IOUtils.toString(resource, StandardCharsets.UTF_8);
        assertNotNull(tsData);

        TimeSeries ts;
        ObjectMapper om = JsonV2.buildObjectMapper();
        ts = om.readValue(tsData, TimeSeries.class);

        TestAccounts.KeyUser user = TestAccounts.KeyUser.SWT_NORMAL;

        given()
                .log().ifValidationFails(LogDetail.ALL,true)
                .accept(Formats.JSONV2)
                .contentType(Formats.JSONV2)
                .body(tsData)
                .header("Authorization", user.toHeaderValue())
                .when()
                .redirects().follow(true)
                .redirects().max(3)
                .post("/timeseries/")
                .then()
                .log().ifValidationFails(LogDetail.ALL,true)
                .assertThat()
                .statusCode(is(HttpServletResponse.SC_OK));

        // Get versioned time series
        given()
                .log().ifValidationFails(LogDetail.ALL,true)
                .accept(Formats.JSONV2)
                .contentType(Formats.JSONV2)
                .header("Authorization", user.toHeaderValue())
                .queryParam(OFFICE, ts.getOfficeId())
                .queryParam(NAME, ts.getName())
                .queryParam(UNIT, ts.getUnits())
                .queryParam(BEGIN, BEGIN_STR)
                .queryParam(END, END_STR)
                .queryParam(VERSION_TYPE, VersionType.UNVERSIONED.getValue())
                .when()
                .redirects().follow(true)
                .redirects().max(3)
                .get("/timeseries/")
                .then()
                .log().ifValidationFails(LogDetail.ALL,true)
                .assertThat()
                .statusCode(is(HttpServletResponse.SC_OK))
                .body("values.size()", equalTo(3))
                .body("values[0][1]", equalTo(1.0F))
                .body("values[1][1]", equalTo(1.0F))
                .body("values[2][1]", equalTo(1.0F))
                .body("version-date", equalTo(null))
                .body("date-version-type", equalTo(VersionType.UNVERSIONED.getValue()));

        // Update versioned time series
        InputStream updated_resource = this.getClass().getResourceAsStream("/cwms/cda/api/swt/num_ts_update_unversioned.json");
        assertNotNull(updated_resource);
        String tsUpdatedData = IOUtils.toString(updated_resource, StandardCharsets.UTF_8);
        assertNotNull(tsUpdatedData);

        given()
                .log().ifValidationFails(LogDetail.ALL,true)
                .accept(Formats.JSONV2)
                .contentType(Formats.JSONV2)
                .body(tsUpdatedData)
                .header("Authorization", user.toHeaderValue())
                .when()
                .redirects().follow(true)
                .redirects().max(3)
                .patch("/timeseries/" + ts.getName())
                .then()
                .log().ifValidationFails(LogDetail.ALL,true)
                .assertThat()
                .statusCode(is(HttpServletResponse.SC_OK));

        // Get updated versioned time series
        given()
                .log().ifValidationFails(LogDetail.ALL,true)
                .accept(Formats.JSONV2)
                .contentType(Formats.JSONV2)
                .header("Authorization", user.toHeaderValue())
                .queryParam(OFFICE, ts.getOfficeId())
                .queryParam(NAME, ts.getName())
                .queryParam(UNIT, ts.getUnits())
                .queryParam(BEGIN, BEGIN_STR)
                .queryParam(END, END_STR)
                .queryParam(VERSION_TYPE, VersionType.UNVERSIONED.getValue())
                .when()
                .redirects().follow(true)
                .redirects().max(3)
                .get("/timeseries/")
                .then()
                .log().ifValidationFails(LogDetail.ALL,true)
                .assertThat()
                .statusCode(is(HttpServletResponse.SC_OK))
                .body("values.size()", equalTo(3))
                .body("values[0][1]", equalTo(2.0F))
                .body("values[1][1]", equalTo(2.0F))
                .body("values[2][1]", equalTo(2.0F))
                .body("version-date", equalTo(null))
                .body("date-version-type", equalTo(VersionType.UNVERSIONED.getValue()));

        // Delete all values from timeseries
        given()
                .log().ifValidationFails(LogDetail.ALL,true)
                .accept(Formats.JSONV2)
                .contentType(Formats.JSONV2)
                .header("Authorization", user.toHeaderValue())
                .queryParam(OFFICE, ts.getOfficeId())
                .queryParam(NAME, ts.getName())
                .queryParam(UNIT, ts.getUnits())
                .queryParam(BEGIN, BEGIN_STR)
                .queryParam(END, "2008-05-01T18:00:00Z")
                .queryParam(VERSION_TYPE, VersionType.UNVERSIONED.getValue())
                .when()
                .redirects().follow(true)
                .redirects().max(3)
                .delete("/timeseries/" + ts.getName())
                .then()
                .log().ifValidationFails(LogDetail.ALL,true)
                .assertThat()
                .statusCode(is(HttpServletResponse.SC_OK));

        // Get versioned time series
        given()
                .log().ifValidationFails(LogDetail.ALL,true)
                .accept(Formats.JSONV2)
                .contentType(Formats.JSONV2)
                .header("Authorization", user.toHeaderValue())
                .queryParam(OFFICE, ts.getOfficeId())
                .queryParam(NAME, ts.getName())
                .queryParam(UNIT, ts.getUnits())
                .queryParam(BEGIN, BEGIN_STR)
                .queryParam(END, END_STR)
                .queryParam(VERSION_TYPE, VersionType.UNVERSIONED.getValue())
                .when()
                .redirects().follow(true)
                .redirects().max(3)
                .get("/timeseries/")
                .then()
                .log().ifValidationFails(LogDetail.ALL,true)
                .assertThat()
                .statusCode(is(HttpServletResponse.SC_OK))
                .body("values.size()", equalTo(3))
                .body("values[0][1]", equalTo(null))
                .body("values[1][1]", equalTo(null))
                .body("values[2][1]", equalTo(null))
                .body("version-date", equalTo(null))
                .body("date-version-type", equalTo(VersionType.UNVERSIONED.getValue()));
    }

    @Test
    void test_create_get_update_get_delete_get_max_agg() throws Exception {
        // Post 2 versioned time series
        InputStream resource = this.getClass().getResourceAsStream("/cwms/cda/api/swt/num_ts_create.json");
        assertNotNull(resource);
        String tsData = IOUtils.toString(resource, StandardCharsets.UTF_8);
        assertNotNull(tsData);

        // Only need to deserialize one timeseries for query params
        // such as office, name, etc, Since these parameters are shared
        // between both timeseries
        TimeSeries ts;
        ObjectMapper om = JsonV2.buildObjectMapper();
        ts = om.readValue(tsData, TimeSeries.class);

        InputStream resource2 = this.getClass().getResourceAsStream("/cwms/cda/api/swt/num_ts_create2.json");
        assertNotNull(resource2);
        String tsData2 = IOUtils.toString(resource2, StandardCharsets.UTF_8);
        assertNotNull(tsData2);

        TestAccounts.KeyUser user = TestAccounts.KeyUser.SWT_NORMAL;

        given()
                .log().ifValidationFails(LogDetail.ALL,true)
                .accept(Formats.JSONV2)
                .contentType(Formats.JSONV2)
                .body(tsData)
                .header("Authorization", user.toHeaderValue())
                .when()
                .redirects().follow(true)
                .redirects().max(3)
                .post("/timeseries/")
                .then()
                .log().ifValidationFails(LogDetail.ALL,true)
                .assertThat()
                .statusCode(is(HttpServletResponse.SC_OK));

        given()
                .log().ifValidationFails(LogDetail.ALL,true)
                .accept(Formats.JSONV2)
                .contentType(Formats.JSONV2)
                .body(tsData2)
                .header("Authorization", user.toHeaderValue())
                .when()
                .redirects().follow(true)
                .redirects().max(3)
                .post("/timeseries/")
                .then()
                .log().ifValidationFails(LogDetail.ALL,true)
                .assertThat()
                .statusCode(is(HttpServletResponse.SC_OK));

        // Get Max Aggregate
        given()
                .log().ifValidationFails(LogDetail.ALL,true)
                .accept(Formats.JSONV2)
                .contentType(Formats.JSONV2)
                .header("Authorization", user.toHeaderValue())
                .queryParam(OFFICE, ts.getOfficeId())
                .queryParam(NAME, ts.getName())
                .queryParam(UNIT, ts.getUnits())
                .queryParam(BEGIN, BEGIN_STR)
                .queryParam(END, "2008-05-01T18:00:00Z")
                .queryParam(VERSION_TYPE, VersionType.MAX_AGGREGATE.getValue())
                .when()
                .redirects().follow(true)
                .redirects().max(3)
                .get("/timeseries/")
                .then()
                .log().ifValidationFails(LogDetail.ALL,true)
                .assertThat()
                .statusCode(is(HttpServletResponse.SC_OK))
                .body("values.size()", equalTo(4))
                .body("values[0][1]", equalTo(1.0F))
                .body("values[1][1]", equalTo(1.0F))
                .body("values[2][1]", equalTo(1.0F))
                .body("values[3][1]", equalTo(3.0F))
                .body("version-date", equalTo(null))
                .body("date-version-type", equalTo(VersionType.MAX_AGGREGATE.getValue()));


        // Update versioned time series
        InputStream updated_resource = this.getClass().getResourceAsStream("/cwms/cda/api/swt/num_ts_update.json");
        assertNotNull(updated_resource);
        String tsUpdatedData = IOUtils.toString(updated_resource, StandardCharsets.UTF_8);
        assertNotNull(tsUpdatedData);

        given()
                .log().ifValidationFails(LogDetail.ALL,true)
                .accept(Formats.JSONV2)
                .contentType(Formats.JSONV2)
                .body(tsUpdatedData)
                .header("Authorization", user.toHeaderValue())
                .when()
                .redirects().follow(true)
                .redirects().max(3)
                .patch("/timeseries/" + ts.getName())
                .then()
                .log().ifValidationFails(LogDetail.ALL,true)
                .assertThat()
                .statusCode(is(HttpServletResponse.SC_OK));


        // Get updated max aggregate
        given()
                .log().ifValidationFails(LogDetail.ALL,true)
                .accept(Formats.JSONV2)
                .contentType(Formats.JSONV2)
                .header("Authorization", user.toHeaderValue())
                .queryParam(OFFICE, ts.getOfficeId())
                .queryParam(NAME, ts.getName())
                .queryParam(UNIT, ts.getUnits())
                .queryParam(BEGIN, BEGIN_STR)
                .queryParam(END, "2008-05-01T18:00:00Z")
                .queryParam(VERSION_TYPE, VersionType.MAX_AGGREGATE.getValue())
                .when()
                .redirects().follow(true)
                .redirects().max(3)
                .get("/timeseries/")
                .then()
                .log().ifValidationFails(LogDetail.ALL,true)
                .assertThat()
                .statusCode(is(HttpServletResponse.SC_OK))
                .body("values.size()", equalTo(4))
                .body("values[0][1]", equalTo(2.0F))
                .body("values[1][1]", equalTo(2.0F))
                .body("values[2][1]", equalTo(2.0F))
                .body("values[3][1]", equalTo(3.0F))
                .body("version-date", equalTo(null))
                .body("date-version-type", equalTo(VersionType.MAX_AGGREGATE.getValue()));

        // Delete all values from one version date
        given()
                .log().ifValidationFails(LogDetail.ALL,true)
                .accept(Formats.JSONV2)
                .contentType(Formats.JSONV2)
                .header("Authorization", user.toHeaderValue())
                .queryParam(OFFICE, ts.getOfficeId())
                .queryParam(NAME, ts.getName())
                .queryParam(UNIT, ts.getUnits())
                .queryParam(BEGIN, BEGIN_STR)
                .queryParam(END, "2008-05-01T18:00:00Z")
                .queryParam(VERSION_DATE, ts.getVersionDate().toString())
                .when()
                .redirects().follow(true)
                .redirects().max(3)
                .delete("/timeseries/" + ts.getName())
                .then()
                .log().ifValidationFails(LogDetail.ALL,true)
                .assertThat()
                .statusCode(is(HttpServletResponse.SC_OK));

        // Get max aggregate
        given()
                .log().ifValidationFails(LogDetail.ALL,true)
                .accept(Formats.JSONV2)
                .contentType(Formats.JSONV2)
                .header("Authorization", user.toHeaderValue())
                .queryParam(OFFICE, ts.getOfficeId())
                .queryParam(NAME, ts.getName())
                .queryParam(UNIT, ts.getUnits())
                .queryParam(BEGIN, BEGIN_STR)
                .queryParam(END, "2008-05-01T18:00:00Z")
                .queryParam(VERSION_TYPE, VersionType.MAX_AGGREGATE.getValue())
                .when()
                .redirects().follow(true)
                .redirects().max(3)
                .get("/timeseries/")
                .then()
                .log().ifValidationFails(LogDetail.ALL,true)
                .assertThat()
                .statusCode(is(HttpServletResponse.SC_OK))
                .body("values.size()", equalTo(4))
                .body("values[0][1]", equalTo(4.0F))
                .body("values[1][1]", equalTo(4.0F))
                .body("values[2][1]", equalTo(4.0F))
                .body("values[3][1]", equalTo(3.0F))
                .body("version-date", equalTo(null))
                .body("date-version-type", equalTo(VersionType.MAX_AGGREGATE.getValue()));
    }

    @Test
    void testVersionTypeValidationRegistration() throws ClassNotFoundException {

        // Trigger static initialization of Controllers class
        Class<?> ignored = Class.forName("cwms.cda.api.Controllers");
        assertNotNull(ignored);

        assertTrue(JavalinValidation.INSTANCE.hasConverter(VersionType.class));
        assertEquals(VersionType.MAX_AGGREGATE, JavalinValidation.INSTANCE.convertValue(VersionType.class, "MAX_AGGREGATE"));
        assertEquals(VersionType.SINGLE_VERSION, JavalinValidation.INSTANCE.convertValue(VersionType.class, "SINGLE_VERSION"));
        assertEquals(VersionType.UNDEF, JavalinValidation.INSTANCE.convertValue(VersionType.class, "UNDEFINED"));
        assertEquals(VersionType.UNVERSIONED, JavalinValidation.INSTANCE.convertValue(VersionType.class, "UNVERSIONED"));
    }
}
