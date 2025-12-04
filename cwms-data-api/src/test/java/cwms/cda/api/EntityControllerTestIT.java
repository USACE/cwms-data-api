package cwms.cda.api;

import cwms.cda.formatters.Formats;
import fixtures.TestAccounts;
import io.restassured.filter.log.LogDetail;
import org.apache.commons.io.IOUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import javax.servlet.http.HttpServletResponse;
import java.io.InputStream;

import static cwms.cda.security.ApiKeyIdentityProvider.AUTH_HEADER;
import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.*;

@Tag("integration")
final class EntityControllerTestIT extends DataApiTestIT {
    private static final String OFFICE_ID = TestAccounts.KeyUser.SPK_NORMAL.getOperatingOffice();
    private static final String ENTITY_ID = "NWS";
    private static final String CASCADE_DELETE = "true";
    private static final String PARENT_ID = "NOAA";


    @AfterEach
    void tearDown() {
        given()
            .accept(Formats.JSONV2)
            .queryParam(Controllers.OFFICE, OFFICE_ID)
            .queryParam(Controllers.CASCADE_DELETE, true)
            .header(AUTH_HEADER, TestAccounts.KeyUser.SPK_NORMAL.toHeaderValue())
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .delete("/entity/" + ENTITY_ID)
        .then()
            .statusCode(isOneOf(
                    HttpServletResponse.SC_NO_CONTENT,
                    HttpServletResponse.SC_NOT_FOUND,
                    HttpServletResponse.SC_BAD_REQUEST
            ));
    }

    // Test CRUD  
    // create -> getOne -> update -> getOne to verify updated
    // delete -> getOne to verify deleted
    @ParameterizedTest
    @ValueSource(strings = {Formats.JSONV2, Formats.DEFAULT})
    void test_entity_create_get_update_delete(String format) throws Exception {

        TestAccounts.KeyUser user = TestAccounts.KeyUser.SPK_NORMAL;
        InputStream in = this.getClass().getResourceAsStream("/cwms/cda/data/dto/entity.json");
        Assertions.assertNotNull(in);
        String entityJson = IOUtils.toString(in, java.nio.charset.StandardCharsets.UTF_8);

        // CREATE
        given()
            .log().ifValidationFails(LogDetail.ALL, true)
            .contentType(Formats.JSONV2)
            .body(entityJson)
            .header(AUTH_HEADER, user.toHeaderValue())
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .post("/entity")
        .then()
            .log().ifValidationFails(LogDetail.ALL, true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_CREATED));

        // GET
        given()
            .log().ifValidationFails(LogDetail.ALL, true)
            .accept(format)
            .queryParam(Controllers.OFFICE, OFFICE_ID)
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .get("/entity/" + ENTITY_ID)
        .then()
            .log().ifValidationFails(LogDetail.ALL, true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_OK))
            .body("id.name", equalTo(ENTITY_ID))
            .body("id.office-id", equalTo(OFFICE_ID))
            .body("long-name", equalTo("National Weather Service"));

        // UPDATE — modify long-name to verify persistence
        String updatedEntityJson = entityJson.replace(
                "\"National Weather Service\"",
                "\"National Weather Service (Updated)\"");

        given()
            .contentType(Formats.JSONV2)
            .body(updatedEntityJson)
            .header(AUTH_HEADER, user.toHeaderValue())
            .queryParam(Controllers.OFFICE, OFFICE_ID)
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .patch("/entity/" + ENTITY_ID)
        .then()
            .log().ifValidationFails(LogDetail.ALL,true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_OK));

        // GET to confirm updated field persisted
        given()
            .log().ifValidationFails(LogDetail.ALL, true)
            .accept(format)
            .queryParam(Controllers.OFFICE, OFFICE_ID)
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .get("/entity/" + ENTITY_ID)
        .then()
            .log().ifValidationFails(LogDetail.ALL, true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_OK))
            .body("id.name", equalTo(ENTITY_ID))
            .body("id.office-id", equalTo(OFFICE_ID))
            .body("long-name", equalTo("National Weather Service (Updated)"));

        // DELETE
        given()
            .header(AUTH_HEADER, user.toHeaderValue())
            .queryParam(Controllers.OFFICE, OFFICE_ID)
            .queryParam(Controllers.CASCADE_DELETE, CASCADE_DELETE)
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .delete("/entity/" + ENTITY_ID)
        .then()
            .log().ifValidationFails(LogDetail.ALL,true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_NO_CONTENT));

        // verify deleted
        given()
            .accept(format)
            .queryParam(Controllers.OFFICE, OFFICE_ID)
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .get("/entity/" + ENTITY_ID)
        .then()
            .log().ifValidationFails(LogDetail.ALL, true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_NOT_FOUND));
    }


    // create fails if entity already exists
    @Test
    void create_duplicate_entity_bad_request() throws Exception {
        TestAccounts.KeyUser user = TestAccounts.KeyUser.SPK_NORMAL;
        InputStream in = this.getClass().getResourceAsStream("/cwms/cda/data/dto/entity.json");
        Assertions.assertNotNull(in);
        String entityJson = IOUtils.toString(in, java.nio.charset.StandardCharsets.UTF_8);

        // CREATE
        given()
            .log().ifValidationFails(LogDetail.ALL, true)
            .contentType(Formats.JSONV2)
            .body(entityJson)
            .header(AUTH_HEADER, user.toHeaderValue())
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .post("/entity")
        .then()
            .log().ifValidationFails(LogDetail.ALL, true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_CREATED));


        // CREATE
        given()
            .log().ifValidationFails(LogDetail.ALL, true)
            .contentType(Formats.JSONV2)
            .body(entityJson)
            .header(AUTH_HEADER, user.toHeaderValue())
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .post("/entity")
        .then()
            .log().ifValidationFails(LogDetail.ALL, true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_CONFLICT));

    }



    // Controller-owned validation: missing required query param
    @Test
    void get_one_missing_office_bad_request() {

        given()
            .log().ifValidationFails(LogDetail.ALL, true)
            .accept(Formats.JSONV2)
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .get("/entity/" + ENTITY_ID)
        .then()
            .log().ifValidationFails(LogDetail.ALL, true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_BAD_REQUEST));
    }

    // Entity ID in the URL must match the id.name in the request body
    @Test
    void update_mismatched_entity_id_bad_request() throws Exception {
        TestAccounts.KeyUser user = TestAccounts.KeyUser.SPK_NORMAL;
        InputStream in = this.getClass().getResourceAsStream("/cwms/cda/data/dto/entity.json");
        Assertions.assertNotNull(in);
        String entity = IOUtils.toString(in, java.nio.charset.StandardCharsets.UTF_8);

        // different entity id
        given()
            .log().ifValidationFails(LogDetail.ALL, true)
            .accept(Formats.JSONV2)
            .contentType(Formats.JSONV2)
            .body(entity)
            .header(AUTH_HEADER, user.toHeaderValue())
            .queryParam(Controllers.OFFICE, OFFICE_ID)
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .patch("/entity/" + "different")
        .then()
            .log().ifValidationFails(LogDetail.ALL, true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_NOT_FOUND));

        // missing office id
        given()
            .log().ifValidationFails(LogDetail.ALL, true)
            .accept(Formats.JSONV2)
            .contentType(Formats.JSONV2)
            .body(entity)
            .header(AUTH_HEADER, user.toHeaderValue())
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .patch("/entity/" + ENTITY_ID)
        .then()
            .log().ifValidationFails(LogDetail.ALL, true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_BAD_REQUEST));
    }

    // getAll with no query params: must return 200 and a list (empty allowed)
    @ParameterizedTest
    @ValueSource(strings = {Formats.JSONV2, Formats.DEFAULT})
    void getAll_no_params_returns_200_empty_list_ok(String format) {
        given()
            .log().ifValidationFails(LogDetail.ALL, true)
            .accept(format)
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .get("/entity")
        .then()
            .log().ifValidationFails(LogDetail.ALL, true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_OK))
            .body("entities", notNullValue())
            .body("entities.size()", greaterThanOrEqualTo(0));
    }

    // Show simple filtering works with getAll and parent entity id only
    @Test
    void getAll_with_parent_filter_returns_200_empty_list_ok() {
        given()
            .log().ifValidationFails(LogDetail.ALL, true)
            .accept(Formats.JSONV2)
            .queryParam(Controllers.PARENT_ENTITY_ID, PARENT_ID)
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .get("/entity")
        .then()
            .log().ifValidationFails(LogDetail.ALL, true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_OK));
    }
}