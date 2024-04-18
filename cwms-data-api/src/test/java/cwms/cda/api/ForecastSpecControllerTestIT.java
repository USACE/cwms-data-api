package cwms.cda.api;

import com.google.common.flogger.FluentLogger;
import cwms.cda.data.dao.DeleteRule;
import cwms.cda.data.dao.JooqDao;
import cwms.cda.formatters.Formats;
import fixtures.CwmsDataApiSetupCallback;
import fixtures.TestAccounts;
import io.restassured.filter.log.LogDetail;
import org.apache.commons.io.IOUtils;
import org.jooq.exception.DataAccessException;
import org.jooq.impl.DSL;
import org.jooq.util.oracle.OracleDSL;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import usace.cwms.db.jooq.codegen.packages.CWMS_FCST_PACKAGE;
import usace.cwms.db.jooq.codegen.packages.CWMS_TS_PACKAGE;

import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;

import static cwms.cda.security.KeyAccessManager.AUTH_HEADER;
import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertNotNull;

@Tag("integration")
public class ForecastSpecControllerTestIT extends DataApiTestIT {
    private static final FluentLogger LOGGER = FluentLogger.forEnclosingClass();
    private static final String OFFICE = "SPK";
    private static final String SPEC_ID = "TEST-SPEC";
    private static final String locationId = "TsBinTestLoc";
    private static final String designator = "designator";

    public static final String PATH = "/forecast-spec/";

    @BeforeAll
    public static void create() throws Exception {
        createLocation(locationId, true, OFFICE);
        createTimeSeries();
    }

    private static void createTimeSeries() throws SQLException {
        //This shouldn't be needed after db update
        createTimeseries(OFFICE, "TsBinTestLoc.Flow.Ave.1Day.1Day.tsid1");
        createTimeseries(OFFICE, "TsBinTestLoc.Flow.Ave.1Day.1Day.tsid2");
        createTimeseries(OFFICE, "TsBinTestLoc.Flow.Ave.1Day.1Day.tsid3");
        createTimeseries(OFFICE, "TsBinTestLoc.Flow.Ave.1Day.1Day.tsid4");
        createTimeseries(OFFICE, "TsBinTestLoc.Flow.Ave.1Day.1Day.tsid5");
        createTimeseries(OFFICE, "TsBinTestLoc.Flow.Ave.1Day.1Day.tsid6");
    }

    @AfterEach
    public void tearDown() throws Exception {
        truncateFcstTimeSeries();
        deleteSpec();
    }

    private static void truncateFcstTimeSeries() throws SQLException {
        //fixing circular reference between spec, time series, and locations
        CwmsDataApiSetupCallback.getDatabaseLink()
                .connection(c -> {
                    OracleDSL.using(c).truncateTable(DSL.table("CWMS_20.AT_FCST_TIME_SERIES"))
                            .execute();
                }, "CWMS_20");
    }

    private static void deleteSpec() throws SQLException {
        try {
            CwmsDataApiSetupCallback.getDatabaseLink()
                    .connection(c -> {
                        CWMS_FCST_PACKAGE.call_DELETE_FCST_SPEC(OracleDSL.using(c).configuration(), SPEC_ID, designator,
                                DeleteRule.DELETE_ALL.getRule(), OFFICE);
                    });
        } catch (DataAccessException e) {
            LOGGER.atFine().withCause(e).log("Couldn't clean up forecast spec before executing tests. Probably didn't exist");
        }
    }


    @Test
    void test_get_create_get() throws IOException {


        // Structure of test:
        // 1)Retrieve a ForecastSpec and assert that it does not exist
        // 2)Create the ForecastSpec
        // 3)Retrieve the ForecastSpec and assert that it exists

        // Step 1)
        // Retrieve a ForecastSpec and assert that it does not exist
        //Read
        given()
                .log().ifValidationFails(LogDetail.ALL, true)
                .accept(Formats.JSONV2)
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

        // Step 3)
        // Retrieve the spec and assert that it exists

        given()
                .log().ifValidationFails(LogDetail.ALL, true)
                .accept(Formats.JSONV2)
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

    @Test
    void test_create_get_delete_get() throws Exception {

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

        // Step 2)
        // Retrieve the spec and assert that it exists
        given()
                .log().ifValidationFails(LogDetail.ALL, true)
                .accept(Formats.JSONV2)
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
                .accept(Formats.JSONV2)
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
                .accept(Formats.JSONV2)
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
    void test_create_get_update_get() throws IOException {

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
                .accept(Formats.JSONV2)
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

        // Step 3)
        // Retrieve the spec and assert that it exists
        given()
                .log().ifValidationFails(LogDetail.ALL, true)
                .accept(Formats.JSONV2)
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
                .accept(Formats.JSONV2)
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
                .accept(Formats.JSONV2)
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