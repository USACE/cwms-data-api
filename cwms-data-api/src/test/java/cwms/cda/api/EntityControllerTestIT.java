package cwms.cda.api;

import cwms.cda.formatters.Formats;
import fixtures.TestAccounts;
import io.restassured.filter.log.LogDetail;
import io.restassured.path.json.JsonPath;
import org.apache.commons.io.IOUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import javax.servlet.http.HttpServletResponse;
import java.io.InputStream;
import java.util.List;
import java.util.Map;

import static cwms.cda.security.ApiKeyIdentityProvider.AUTH_HEADER;
import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.*;

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
        assertNotNull(in);
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
        assertNotNull(in);
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
    void update_non_existing_entity_id_or_missing_office_id_400_or_404() throws Exception {
        TestAccounts.KeyUser user = TestAccounts.KeyUser.SPK_NORMAL;
        InputStream in = this.getClass().getResourceAsStream("/cwms/cda/data/dto/entity.json");
        assertNotNull(in);
        String entity = IOUtils.toString(in, java.nio.charset.StandardCharsets.UTF_8);

        // UPDATE - non-existing entity id - 404
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

        // UPDATE - missing office id - 400
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
    
    
    @Test
    void getAll_match_null_parents_flag_() throws Exception {
        TestAccounts.KeyUser user = TestAccounts.KeyUser.SPK_NORMAL;
        InputStream in = this.getClass().getResourceAsStream("/cwms/cda/data/dto/entity.json");
        assertNotNull(in);
        String entity = IOUtils.toString(in, java.nio.charset.StandardCharsets.UTF_8);
        // make parent-entity-id null
        String nullParentEntity = entity.replace(
                "\"parent-entity-id\" : \"NOAA\",", "");

        // CREATE entity with null parent - default match-null-parents = true
        given()
            .log().ifValidationFails(LogDetail.ALL, true)
            .contentType(Formats.JSONV2)
            .body(nullParentEntity)
            .header(AUTH_HEADER, user.toHeaderValue())
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .post("/entity")
        .then()
            .log().ifValidationFails(LogDetail.ALL, true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_CREATED));

        // GET - verify getAll includes nullParentEntity when match-null-parents = true (default)
        String json =
            given()
                .log().ifValidationFails(LogDetail.ALL, true)
                .accept(Formats.JSONV2)
            .when()
                .redirects().follow(true)
                .redirects().max(3)
                .get("/entity")
            .then()
                .log().ifValidationFails(LogDetail.ALL, true)
                .statusCode(HttpServletResponse.SC_OK)
                .extract().asString();

        List<Map<String, Object>> items = JsonPath.from(json).getList("");
        Map<String, Object> target = null;
        for (Map<String, Object> m : items) {
            Map<?, ?> id = (Map<?, ?>) m.get("id");
            if (id != null
                    && "SPK".equals(id.get("office-id"))
                    && "NWS".equals(id.get("name"))) {
                target = m;
                break;
            }
        }

        assertNotNull(target, "Entity with null parent-id should be present when match-null-parents=true");
        assertTrue(!target.containsKey("parent-entity-id") || target.get("parent-entity-id") == null,
                "parent-entity-id should be null/absent"
        );

        // GET - verify getAll does NOT include nullParentEntity when match-null-parents = false
        String json2 =
            given()
                .log().ifValidationFails(LogDetail.ALL, true)
                .accept(Formats.JSONV2)
                .queryParam(Controllers.MATCH_NULL_PARENTS, false)
            .when()
                .redirects().follow(true)
                .redirects().max(3)
                .get("/entity")
            .then()
                .log().ifValidationFails(LogDetail.ALL, true)
                .statusCode(HttpServletResponse.SC_OK)
                .extract().asString();

        List<Map<String, Object>> items2 = JsonPath.from(json2).getList("");
        boolean present = false;
        for (Map<String, Object> m : items2) {
            Map<?, ?> id = (Map<?, ?>) m.get("id");
            if (id != null
                    && "SPK".equals(id.get("office-id"))
                    && "NWS".equals(id.get("name"))) {
                present = true;
                break;
            }
        }

        assertFalse(present, "Entity with null parent-id should be filtered out when match-null-parents=false");

        // DELETE nullParentEntity
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
    }
}