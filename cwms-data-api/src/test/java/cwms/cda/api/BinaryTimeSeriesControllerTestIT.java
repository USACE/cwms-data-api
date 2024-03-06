package cwms.cda.api;

import static cwms.cda.data.dao.DaoTest.getDslContext;
import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import cwms.cda.formatters.Formats;
import cwms.cda.helpers.DateUtils;
import fixtures.CwmsDataApiSetupCallback;
import fixtures.TestAccounts;
import io.restassured.filter.log.LogDetail;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.ZonedDateTime;
import java.util.Date;
import javax.servlet.http.HttpServletResponse;
import mil.army.usace.hec.test.database.CwmsDatabaseContainer;
import org.apache.commons.io.IOUtils;
import org.jooq.DSLContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import usace.cwms.db.jooq.codegen.packages.CWMS_TEXT_PACKAGE;

@Tag("integration")
public class BinaryTimeSeriesControllerTestIT extends DataApiTestIT {
    private static final String OFFICE = "SPK";
    private static final String locationId = "TsBinTestLoc";
    private static final String tsId = locationId + ".Flow.Inst.1Hour.0.raw";
    public static final String BEGIN_STR = "2008-05-01T15:00:00Z";
    public static final String END_STR = "2008-05-01T23:00:00Z";

    @BeforeAll
    public static void create() throws Exception {
        createLocation(locationId, true, OFFICE);
        createTimeseries(OFFICE, tsId, 0);
    }

    @AfterEach
    public void cleanup() throws Exception {
        CwmsDatabaseContainer<?> databaseLink = CwmsDataApiSetupCallback.getDatabaseLink();
        databaseLink.connection(c -> {
                    DSLContext dsl = getDslContext(c, OFFICE);

            ZonedDateTime startZdt = DateUtils.parseUserDate(BEGIN_STR, "UTC");
            ZonedDateTime endZdt = DateUtils.parseUserDate(END_STR, "UTC");

            CWMS_TEXT_PACKAGE.call_DELETE_TS_BINARY(dsl.configuration(),
                    tsId, "*", Timestamp.from(startZdt.toInstant()), Timestamp.from(endZdt.toInstant()), null, "UTC",
                   "T", null, null, OFFICE)
                   ;
                }
        );
    }


    @Test
    void test_get_create_get() throws IOException {

        // Structure of test:
        // 1)Retrieve a binary time series and assert that it does not exist
        // 2)Create the binary time series
        // 3)Retrieve the binary time series and assert that it exists

        // Step 1)
        // Retrieve a binary time series and assert that it does not exist
        //Read
        given()
            .log().ifValidationFails(LogDetail.ALL,true)
            .accept(Formats.JSONV2)
            .queryParam("office", OFFICE)
            .queryParam("name", tsId)
            .queryParam("begin", BEGIN_STR)
            .queryParam("end", END_STR)
            .queryParam("max-version","false")
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .get("/timeseries/binary/")
        .then()
            .log().ifValidationFails(LogDetail.ALL,true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_OK))
                .body("binary-values.size()", equalTo(0))
        ;

        // Step 2)
        // Create the binary time series

        InputStream resource = this.getClass().getResourceAsStream("/cwms/cda/api/spk/bin_ts_create.json");
        assertNotNull(resource);
        String tsData = IOUtils.toString(resource, StandardCharsets.UTF_8);
        assertNotNull(tsData);

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
            .post("/timeseries/binary/")
        .then()
            .log().ifValidationFails(LogDetail.ALL,true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_CREATED));

        // Step 3)
        // Retrieve the binary time series and assert that it exists

        given()
            .log().ifValidationFails(LogDetail.ALL,true)
            .accept(Formats.JSONV2)
            .queryParam("office", OFFICE)
            .queryParam("name", tsId)
            .queryParam("begin", BEGIN_STR)
            .queryParam("end", END_STR)
            .queryParam("max-version","false")
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .get("/timeseries/binary/")
        .then()
            .log().ifValidationFails(LogDetail.ALL,true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_OK))
            .body("binary-values.size()", equalTo(3))

        ;


    }

    @Test
    void test_create_get_delete_get() throws IOException {

        // Structure of test:
        //
        // 1)Create the binary time series
        // 2)Retrieve the binary time series and assert that it exists
        // 3)Delete the binary time series
        // 4)Retrieve the binary time series and assert that it does not exist


        // Step 1)
        // Create the binary time series

        InputStream resource = this.getClass().getResourceAsStream("/cwms/cda/api/spk/bin_ts_create.json");
        assertNotNull(resource);
        String tsData = IOUtils.toString(resource, StandardCharsets.UTF_8);
        assertNotNull(tsData);

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
            .post("/timeseries/binary/")
        .then()
            .log().ifValidationFails(LogDetail.ALL,true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_CREATED));

        // Step 2)
        // Retrieve the binary time series and assert that it exists

        given()
            .log().ifValidationFails(LogDetail.ALL,true)
            .accept(Formats.JSONV2)
            .queryParam("office", OFFICE)
            .queryParam("name", tsId)
            .queryParam("begin", BEGIN_STR)
            .queryParam("end", END_STR)
            .queryParam("max-version","false")
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .get("/timeseries/binary/")
        .then()
            .log().ifValidationFails(LogDetail.ALL,true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_OK))
            .body("binary-values.size()", equalTo(3))
        ;

        // Step 3)
        // Delete the binary time series
        given()
            .log().ifValidationFails(LogDetail.ALL, true)
            .accept(Formats.JSONV2)
            .header("Authorization", user.toHeaderValue())
            .queryParam("office", OFFICE)
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .queryParam("begin", BEGIN_STR)
            .queryParam("end", END_STR)
            .delete("/timeseries/binary/" + tsId)
        .then()
            .log().ifValidationFails(LogDetail.ALL, true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_NO_CONTENT));

        Date versionDate = new Date(1139911000L);
        Instant instant = versionDate.toInstant();
        String versionDateStr = instant.toString();

        // Step 4)
        // Retrieve the binary time series and assert that it does not exist
        given()
            .log().ifValidationFails(LogDetail.ALL,true)
            .accept(Formats.JSONV2)
            .queryParam("office", OFFICE)
            .queryParam("name", tsId)
            .queryParam("begin", BEGIN_STR)
            .queryParam("end", END_STR)
            .queryParam("version-date", versionDateStr)
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .get("/timeseries/binary/")
        .then()
            .log().ifValidationFails(LogDetail.ALL,true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_OK))
            .body("binary-values.size()", equalTo(0))
        ;
    }

    @Test
    void test_create_get_update_get() throws IOException {

        // Structure of test:
        // 1)Retrieve bts and make sure its empty
        // 2)Create the binary time series
        // 3)Retrieve the binary time series and assert that it exists
        // 4)Update the binary time series
        // 5)Retrieve the binary time series and assert that it does not exist


        // Step 1)
        // Retrieve a binary time series and assert that it does not exist
        //Read
        given()
                .log().ifValidationFails(LogDetail.ALL,true)
                .accept(Formats.JSONV2)
                .queryParam("office", OFFICE)
                .queryParam("name", tsId)
                .queryParam("begin", BEGIN_STR)
                .queryParam("end", END_STR)
                .queryParam("max-version","false")
                .when()
                .redirects().follow(true)
                .redirects().max(3)
                .get("/timeseries/binary/")
                .then()
                .log().ifValidationFails(LogDetail.ALL,true)
                .assertThat()
                .statusCode(is(HttpServletResponse.SC_OK))
                .body("binary-values.size()", equalTo(0))
        ;


        // Step 2)
        // Create the binary time series

        InputStream resource = this.getClass().getResourceAsStream("/cwms/cda/api/spk/bin_ts_create.json");
        assertNotNull(resource);
        String tsData = IOUtils.toString(resource, StandardCharsets.UTF_8);
        assertNotNull(tsData);

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
                .post("/timeseries/binary/")
                .then()
                .log().ifValidationFails(LogDetail.ALL,true)
                .assertThat()
                .statusCode(is(HttpServletResponse.SC_CREATED));

        // Step 3)
        // Retrieve the binary time series and assert that it exists

        given()
                .log().ifValidationFails(LogDetail.ALL,true)
                .accept(Formats.JSONV2)
                .queryParam(Controllers.OFFICE, OFFICE)
                .queryParam(Controllers.NAME, tsId)
                .queryParam(Controllers.BEGIN, BEGIN_STR)
                .queryParam(Controllers.END, END_STR)

                .when()
                .redirects().follow(true)
                .redirects().max(3)
                .get("/timeseries/binary/")
                .then()
                .log().ifValidationFails(LogDetail.ALL,true)
                .assertThat()
                .statusCode(is(HttpServletResponse.SC_OK))
                .body("binary-values.size()", equalTo(3))
        ;

        // Step 4)
        // Update the binary time series

        resource = this.getClass().getResourceAsStream("/cwms/cda/api/spk/bin_ts_update.json");
        assertNotNull(resource);
        tsData = IOUtils.toString(resource, StandardCharsets.UTF_8);
        assertNotNull(tsData);

        given()
            .log().ifValidationFails(LogDetail.ALL,true)
            .accept(Formats.JSONV2)
            .queryParam("replace-all", "true")
            .contentType(Formats.JSONV2)
            .body(tsData)
            .header("Authorization", user.toHeaderValue())
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .patch("/timeseries/binary/" + tsId)
        .then()
            .log().ifValidationFails(LogDetail.ALL,true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_OK));


        String newValue = "bmV3VmFsdWU=";
        // Step 5)
        // Retrieve the binary time series and assert that it does not exist
        given()
            .log().ifValidationFails(LogDetail.ALL,true)
            .accept(Formats.JSONV2)
            .queryParam("office", OFFICE)
            .queryParam("name", tsId)
            .queryParam("begin", BEGIN_STR)
            .queryParam("end", END_STR)
//                .queryParam("version-date", versionDateStr)
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .get("/timeseries/binary/")
        .then()
            .log().ifValidationFails(LogDetail.ALL,true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_OK))
            .body("binary-values.size()", equalTo(3))
            .body("binary-values[1].binary-value", equalTo(newValue))
        ;
    }

}