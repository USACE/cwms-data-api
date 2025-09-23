package cwms.cda.api;

import com.google.common.flogger.FluentLogger;
import cwms.cda.ApiServlet;
import cwms.cda.data.dao.DeleteRule;
import cwms.cda.data.dao.JooqDao;
import cwms.cda.formatters.Formats;
import fixtures.CwmsDataApiSetupCallback;
import fixtures.TestAccounts;
import io.restassured.filter.log.LogDetail;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import usace.cwms.db.jooq.codegen.packages.CWMS_FCST_PACKAGE;

import org.apache.commons.io.IOUtils;
import org.jooq.exception.DataAccessException;
import org.jooq.impl.DSL;
import org.jooq.util.oracle.OracleDSL;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;

import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;

import static cwms.cda.api.Controllers.ID_MASK;
import static cwms.cda.security.ApiKeyIdentityProvider.AUTH_HEADER;
import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.isEmptyOrNullString;
import static org.junit.jupiter.api.Assertions.assertNotNull;

@Tag("integration")
final class ForecastSpecControllerTestIT extends DataApiTestIT {
    private static final FluentLogger LOGGER = FluentLogger.forEnclosingClass();
    private static final String OFFICE = "SPK";
    private static final String SPEC_ID = "TEST-SPEC";
    private static final String locationId = "TsBinTestLoc";
    private static final String locationId2 = "TsBinTestLoc2";
    private static final String designator = "designator";

    public static final String PATH = "/forecast-spec/";

    @BeforeAll
    static void create() throws Exception {
        createLocation(locationId, true, OFFICE);
        createLocation(locationId2, true, OFFICE);
        createTimeSeries(locationId);
        createTimeseries(OFFICE, "TsBinTestLoc.Elev.Inst.~1Day.0.SPK-cavi-fct");
        createTimeseries(OFFICE, "TsBinTestLoc.Flow-Outflow.Inst.~1Day.0.SPK-cavi-fct");
        createTimeseries(OFFICE, "TsBinTestLoc2.Elev.Inst.~1Day.0.SPK-cavi-fct");
        createTimeseries(OFFICE, "TsBinTestLoc2.Flow-Outflow.Inst.~1Day.0.SPK-cavi-fct");
    }

    static void createTimeSeries(String locationId) throws SQLException {
        //This shouldn't be needed after db update
        createTimeseries(OFFICE, locationId + ".Flow.Ave.1Day.1Day.tsid1");
        createTimeseries(OFFICE, locationId + ".Flow.Ave.1Day.1Day.tsid2");
        createTimeseries(OFFICE, locationId + ".Flow.Ave.1Day.1Day.tsid3");
        createTimeseries(OFFICE, locationId + ".Flow.Ave.1Day.1Day.tsid4");
        createTimeseries(OFFICE, locationId + ".Flow.Ave.1Day.1Day.tsid5");
        createTimeseries(OFFICE, locationId + ".Flow.Ave.1Day.1Day.tsid6");
    }

    @AfterEach
    void tearDown() throws Exception {
        truncateFcstTimeSeries();
        deleteSpec();
    }

    static void truncateFcstTimeSeries() throws SQLException {
        //fixing circular reference between spec, time series, and locations
        CwmsDataApiSetupCallback.getDatabaseLink()
                .connection(c -> {
                    OracleDSL.using(c).truncateTable(DSL.table("CWMS_20.AT_FCST_TIME_SERIES"))
                            .execute();
                    OracleDSL.using(c).truncateTable(DSL.table("CWMS_20.AT_FCST_INFO"))
                            .execute();
                    OracleDSL.using(c).truncateTable(DSL.table("CWMS_20.AT_FCST_INST"))
                            .execute();
                }, "CWMS_20");
    }

    static void deleteSpec() throws SQLException {
       try {
           CwmsDataApiSetupCallback.getDatabaseLink()
                   .connection(c -> {
                       CWMS_FCST_PACKAGE.call_DELETE_FCST_SPEC(OracleDSL.using(c).configuration(), SPEC_ID, "designator",
                               DeleteRule.DELETE_ALL.getRule(), OFFICE);
                       CWMS_FCST_PACKAGE.call_DELETE_FCST_SPEC(OracleDSL.using(c).configuration(), SPEC_ID + "-NULL-DESIGNATOR", null,
                               DeleteRule.DELETE_ALL.getRule(), OFFICE);
                   });
       } catch (DataAccessException e) {
           LOGGER.atFine().withCause(e).log("Couldn't clean up forecast spec before executing tests. Probably didn't exist");
       }
    }


    @ParameterizedTest
    @ValueSource(strings = {Formats.JSONV2, Formats.DEFAULT})
    void test_get_create_get(String format) throws IOException {

        // Structure of test:
        // 1)Retrieve a ForecastSpec and assert that it does not exist
        // 2)Create the ForecastSpec
        // 3)Retrieve the ForecastSpec and assert that it exists

        // Step 1)
        // Retrieve a ForecastSpec and assert that it does not exist
        //Read
        given()
            .log().ifValidationFails(LogDetail.ALL, true)
            .accept(format)
            .queryParam(Controllers.OFFICE, OFFICE)
            .queryParam(Controllers.DESIGNATOR, designator)
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .get(PATH + SPEC_ID)
        .then()
            .log().ifValidationFails(LogDetail.ALL, true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_NOT_FOUND))
        ;

        // Step 2)
        // Create the ForecastSpec

        InputStream resource = this.getClass().getResourceAsStream("/cwms/cda/api/spk/forecast_spec_create.json");
        assertNotNull(resource);
        String tsData = IOUtils.toString(resource, StandardCharsets.UTF_8);
        assertNotNull(tsData);

        TestAccounts.KeyUser user = TestAccounts.KeyUser.SPK_NORMAL;

        given()
            .log().ifValidationFails(LogDetail.ALL, true)
            .accept(format)
            .contentType(Formats.JSONV2)
            .body(tsData)
            .header(AUTH_HEADER, user.toHeaderValue())
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .post(PATH)
        .then()
            .log().ifValidationFails(LogDetail.ALL, true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_CREATED));

        // Step 3)
        // Retrieve the spec and assert that it exists

        given()
            .log().ifValidationFails(LogDetail.ALL, true)
            .accept(format)
            .queryParam(Controllers.OFFICE, OFFICE)
            .queryParam(Controllers.DESIGNATOR, designator)
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .get(PATH + SPEC_ID)
        .then()
            .log().ifValidationFails(LogDetail.ALL, true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_OK))
            .body("designator", equalTo(designator))
            .body("time-series-ids.size()", equalTo(3))
        ;


    }


    @ParameterizedTest
    @ValueSource(strings = {Formats.JSONV2, Formats.DEFAULT})
    void test_get_create_get_null_designator(String format) throws IOException {

        // Structure of test:
        // 1)Retrieve a ForecastSpec and assert that it does not exist
        // 2)Create the ForecastSpec
        // 3)Retrieve the ForecastSpec and assert that it exists
        // 4)Delete the ForecastSpec if it exists

        // Step 1)
        // Retrieve a ForecastSpec and assert that it does not exist
        //Read
        given()
            .log().ifValidationFails(LogDetail.ALL, true)
            .accept(format)
            .queryParam(Controllers.OFFICE, OFFICE)
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .get(PATH + SPEC_ID + "-NULL-DESIGNATOR")
        .then()
            .log().ifValidationFails(LogDetail.ALL, true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_NOT_FOUND))
        ;

        // Step 2)
        // Create the ForecastSpec

        InputStream resource = this.getClass().getResourceAsStream("/cwms/cda/api/spk/forecast_spec_create_null_designator.json");
        assertNotNull(resource);
        String tsData = IOUtils.toString(resource, StandardCharsets.UTF_8);
        assertNotNull(tsData);

        TestAccounts.KeyUser user = TestAccounts.KeyUser.SPK_NORMAL;

        given()
            .log().ifValidationFails(LogDetail.ALL, true)
            .accept(format)
            .contentType(Formats.JSONV2)
            .body(tsData)
            .header(AUTH_HEADER, user.toHeaderValue())
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .post(PATH)
        .then()
            .log().ifValidationFails(LogDetail.ALL, true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_CREATED));

        // Step 3)
        // Retrieve the spec and assert that it exists

        given()
            .log().ifValidationFails(LogDetail.ALL, true)
            .accept(format)
            .queryParam(Controllers.OFFICE, OFFICE)
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .get(PATH + SPEC_ID + "-NULL-DESIGNATOR")
        .then()
            .log().ifValidationFails(LogDetail.ALL, true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_OK))
            .body("designator", isEmptyOrNullString())
            .body("time-series-ids.size()", equalTo(3))
        ;

        given()
            .log().ifValidationFails(LogDetail.ALL, true)
            .accept(format)
            .queryParam(Controllers.OFFICE, OFFICE)
            .queryParam(ID_MASK, SPEC_ID + "-NULL-DESIGNATOR")
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .get(PATH)
        .then()
            .log().ifValidationFails(LogDetail.ALL, true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_OK))
            .body("[0].designator", isEmptyOrNullString())
            .body("[0].time-series-ids.size()", equalTo(3))
        ;

        // Step 4)
        // Delete the spec
        given()
            .log().ifValidationFails(LogDetail.ALL, true)
            .queryParam(Controllers.OFFICE, OFFICE)
            .header(AUTH_HEADER, user.toHeaderValue())
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .delete(PATH + SPEC_ID + "-NULL-DESIGNATOR")
        .then()
            .log().ifValidationFails(LogDetail.ALL, true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_NO_CONTENT))
        ;
    }

    @ParameterizedTest
    @ValueSource(strings = {Formats.JSONV2, Formats.DEFAULT})
    void test_create_get_delete_get(String format) throws Exception {

        // Structure of test:
        //
        // 1)Create the spec
        // 2)Retrieve the spec and assert that it exists
        // 3)Delete the spec
        // 4)Retrieve the spec and assert that it does not exist


        // Step 1)
        // Create the spec
        InputStream resource = this.getClass().getResourceAsStream("/cwms/cda/api/spk/forecast_spec_create.json");
        assertNotNull(resource);
        String tsData = IOUtils.toString(resource, StandardCharsets.UTF_8);
        assertNotNull(tsData);

        TestAccounts.KeyUser user = TestAccounts.KeyUser.SPK_NORMAL;

        given()
            .log().ifValidationFails(LogDetail.ALL, true)
            .accept(format)
            .contentType(Formats.JSONV2)
            .body(tsData)
            .header(AUTH_HEADER, user.toHeaderValue())
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .post(PATH)
        .then()
            .log().ifValidationFails(LogDetail.ALL, true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_CREATED));

        // Step 2)
        // Retrieve the spec and assert that it exists
        given()
            .log().ifValidationFails(LogDetail.ALL, true)
            .accept(format)
            .queryParam(Controllers.OFFICE, OFFICE)
            .queryParam(Controllers.DESIGNATOR, designator)
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .get(PATH + SPEC_ID)
        .then()
            .log().ifValidationFails(LogDetail.ALL, true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_OK))
            .body("designator", equalTo(designator))
            .body("time-series-ids.size()", equalTo(3))
        ;
        truncateFcstTimeSeries();
        // Step 3)
        // Delete the spec
        given()
            .log().ifValidationFails(LogDetail.ALL, true)
            .accept(format)
            .header(AUTH_HEADER, user.toHeaderValue())
            .queryParam(Controllers.OFFICE, OFFICE)
            .queryParam(Controllers.NAME, SPEC_ID)
            .queryParam(Controllers.DESIGNATOR, designator)
            .queryParam(Controllers.METHOD, JooqDao.DeleteMethod.DELETE_ALL)
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .delete(PATH + SPEC_ID)
        .then()
            .log().ifValidationFails(LogDetail.ALL, true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_NO_CONTENT));

        // Step 4)
        // Retrieve the spec and assert that it does not exist
        given()
            .log().ifValidationFails(LogDetail.ALL, true)
            .accept(format)
            .queryParam(Controllers.OFFICE, OFFICE)
            .queryParam(Controllers.DESIGNATOR, designator)
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .get(PATH + SPEC_ID)
        .then()
            .log().ifValidationFails(LogDetail.ALL, true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_NOT_FOUND))
        ;
    }

    @Test
    void test_create_get_delete_get_permissions_issue() throws Exception {

        // Create the spec
        InputStream resource = this.getClass().getResourceAsStream("/cwms/cda/api/spk/forecast_spec_save.json");
        assertNotNull(resource);
        String tsData = IOUtils.toString(resource, StandardCharsets.UTF_8);
        assertNotNull(tsData);

        TestAccounts.KeyUser user = TestAccounts.KeyUser.SPK_NEW_USER;

        given()
                .log().ifValidationFails(LogDetail.ALL, true)
                .accept(Formats.JSONV2)
                .contentType(Formats.JSONV2)
                .body(tsData)
                .header(AUTH_HEADER, user.toHeaderValue())
        .when()
                .redirects().follow(true)
                .redirects().max(3)
                .post(PATH)
        .then()
                .log().ifValidationFails(LogDetail.ALL, true)
        .assertThat()
                .statusCode(is(HttpServletResponse.SC_CREATED));

        truncateFcstTimeSeries();
        // Delete the spec
        given()
                .log().ifValidationFails(LogDetail.ALL, true)
                .accept(Formats.JSONV2)
                .header(AUTH_HEADER, user.toHeaderValue())
                .queryParam(Controllers.OFFICE, OFFICE)
                .queryParam(Controllers.NAME, "SPK-Daily-UKY-Test")
                .queryParam(Controllers.DESIGNATOR, designator)
                .queryParam(Controllers.METHOD, JooqDao.DeleteMethod.DELETE_ALL)
        .when()
                .redirects().follow(true)
                .redirects().max(3)
                .delete(PATH + "SPK-Daily-UKY-Test")
        .then()
                .log().ifValidationFails(LogDetail.ALL, true)
        .assertThat()
                .statusCode(is(HttpServletResponse.SC_NO_CONTENT));

        // Retrieve the spec and assert that it does not exist
        given()
                .log().ifValidationFails(LogDetail.ALL, true)
                .accept(Formats.JSONV2)
                .queryParam(Controllers.OFFICE, OFFICE)
                .queryParam(Controllers.DESIGNATOR, designator)
        .when()
                .redirects().follow(true)
                .redirects().max(3)
                .get(PATH + "SPK-Daily-UKY-Test")
        .then()
                .log().ifValidationFails(LogDetail.ALL, true)
        .assertThat()
                .statusCode(is(HttpServletResponse.SC_NOT_FOUND))
        ;
    }

    @ParameterizedTest
    @ValueSource(strings = {Formats.JSONV2, Formats.DEFAULT})
    void test_create_get_delete_get_lrts(String format) throws Exception {
        // Structure of test:
        // 1) Create the spec
        // 2) Retrieve the spec and assert that it exists
        // 3) Delete the spec
        // 4) Retrieve the spec and assert that it does not exist

        String specId = "TEST-SPEC-LRTS";
        TestAccounts.KeyUser user = TestAccounts.KeyUser.SPK_NORMAL;

        // Step 1)
        // Create the spec
        InputStream resource = this.getClass().getResourceAsStream("/cwms/cda/api/spk/forecast_spec_create_lrts.json");
        assertNotNull(resource);
        String specData = IOUtils.toString(resource, StandardCharsets.UTF_8);
        assertNotNull(specData);

        createTimeseriesWithNewLRTSInterval(OFFICE, "TsBinTestLoc.Flow.Ave.1DayLocal.1Day.tsid1", 0);
        createTimeseriesWithNewLRTSInterval(OFFICE, "TsBinTestLoc.Flow.Ave.1DayLocal.1Day.tsid2", 0);
        createTimeseriesWithNewLRTSInterval(OFFICE, "TsBinTestLoc.Flow.Ave.1DayLocal.1Day.tsid3", 0);

        given()
            .log().ifValidationFails(LogDetail.ALL, true)
            .accept(format)
            .contentType(Formats.JSONV2)
            .body(specData)
            .header(AUTH_HEADER, user.toHeaderValue())
            .header(ApiServlet.IS_NEW_LRTS, true)
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .post(PATH)
        .then()
            .log().ifValidationFails(LogDetail.ALL, true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_CREATED));

        // Step 2)
        // Retrieve the spec and assert that it exists
        given()
            .log().ifValidationFails(LogDetail.ALL, true)
            .accept(format)
            .queryParam(Controllers.OFFICE, OFFICE)
            .queryParam(Controllers.DESIGNATOR, designator)
            .header(ApiServlet.IS_NEW_LRTS, true)
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .get(PATH + specId)
        .then()
            .log().ifValidationFails(LogDetail.ALL, true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_OK))
            .body("designator", equalTo(designator))
            .body("time-series-ids.size()", equalTo(3))
            .body("time-series-ids[0]", equalTo("TsBinTestLoc.Flow.Ave.1DayLocal.1Day.tsid1"))
            .body("time-series-ids[1]", equalTo("TsBinTestLoc.Flow.Ave.1DayLocal.1Day.tsid2"))
            .body("time-series-ids[2]", equalTo("TsBinTestLoc.Flow.Ave.1DayLocal.1Day.tsid3"))
        ;

        truncateFcstTimeSeries();

        // Step 3)
        // Delete the spec
        given()
            .log().ifValidationFails(LogDetail.ALL, true)
            .accept(format)
            .header(AUTH_HEADER, user.toHeaderValue())
            .queryParam(Controllers.OFFICE, OFFICE)
            .queryParam(Controllers.NAME, specId)
            .queryParam(Controllers.DESIGNATOR, designator)
            .queryParam(Controllers.METHOD, JooqDao.DeleteMethod.DELETE_ALL)
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .delete(PATH + specId)
        .then()
            .log().ifValidationFails(LogDetail.ALL, true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_NO_CONTENT));

        // Step 4)
        // Retrieve the spec and assert that it does not exist
        given()
            .log().ifValidationFails(LogDetail.ALL, true)
            .accept(format)
            .queryParam(Controllers.OFFICE, OFFICE)
            .queryParam(Controllers.DESIGNATOR, designator)
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .get(PATH + specId)
        .then()
            .log().ifValidationFails(LogDetail.ALL, true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_NOT_FOUND))
        ;
    }

    @ParameterizedTest
    @ValueSource(strings = {Formats.JSONV2, Formats.DEFAULT})
    void test_create_get_update_get(String format) throws IOException {

        // Structure of test:
        // 1)Retrieve spec
        // 2)Create the spec
        // 3)Retrieve the spec and assert that it exists
        // 4)Update the spec
        // 5)Retrieve the spec and assert that its changed


        // Step 1)
        // Retrieve a spec and assert that it does not exist
        //Read
        given()
            .log().ifValidationFails(LogDetail.ALL, true)
            .accept(format)
            .queryParam(Controllers.OFFICE, OFFICE)
            .queryParam(Controllers.DESIGNATOR, designator)
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .get(PATH + SPEC_ID)
        .then()
            .log().ifValidationFails(LogDetail.ALL, true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_NOT_FOUND))
        ;

        // Step 2)
        // Create the ForecastSpec

        InputStream resource = this.getClass().getResourceAsStream("/cwms/cda/api/spk/forecast_spec_create.json");
        assertNotNull(resource);
        String tsData = IOUtils.toString(resource, StandardCharsets.UTF_8);
        assertNotNull(tsData);

        TestAccounts.KeyUser user = TestAccounts.KeyUser.SPK_NORMAL;

        given()
            .log().ifValidationFails(LogDetail.ALL, true)
            .accept(format)
            .contentType(Formats.JSONV2)
            .body(tsData)
            .header(AUTH_HEADER, user.toHeaderValue())
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .post(PATH)
        .then()
            .log().ifValidationFails(LogDetail.ALL, true)
            .assertThat()
            .statusCode(is(HttpServletResponse.SC_CREATED));

        // Step 3)
        // Retrieve the spec and assert that it exists
        given()
            .log().ifValidationFails(LogDetail.ALL, true)
            .accept(format)
            .queryParam(Controllers.OFFICE, OFFICE)
            .queryParam(Controllers.DESIGNATOR, designator)
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .get(PATH + SPEC_ID)
        .then()
            .log().ifValidationFails(LogDetail.ALL, true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_OK))
            .body("source-entity-id", equalTo("USACE"))
        ;

        // Step 4)
        // Update the spec
        resource = this.getClass().getResourceAsStream("/cwms/cda/api/spk/forecast_spec_update.json");
        assertNotNull(resource);
        tsData = IOUtils.toString(resource, StandardCharsets.UTF_8);
        assertNotNull(tsData);

        given()
            .log().ifValidationFails(LogDetail.ALL, true)
            .accept(format)
            .contentType(Formats.JSONV2)
            .body(tsData)
            .header(AUTH_HEADER, user.toHeaderValue())
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .patch(PATH + SPEC_ID)
        .then()
            .log().ifValidationFails(LogDetail.ALL, true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_OK));


        // Step 5)
        // Retrieve thespec and assert it changed
        given()
            .log().ifValidationFails(LogDetail.ALL, true)
            .accept(format)
            .queryParam(Controllers.OFFICE, OFFICE)
            .queryParam(Controllers.DESIGNATOR, designator)
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .get(PATH + SPEC_ID)
        .then()
            .log().ifValidationFails(LogDetail.ALL, true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_OK))
            .body("source-entity-id", equalTo("USGS"))
        ;
    }

}