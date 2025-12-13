package cwms.cda.api;

import com.fasterxml.jackson.databind.ObjectMapper;
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
    private static final String OFFICE = Controllers.OFFICE;
    private static final String CASCADE_DELETE = Controllers.CASCADE_DELETE;


    @AfterEach
    void tearDown() throws Exception {

        for (Map<String, String> entityMap : loadTestEntityJsonList()) {
            String entityJson = new ObjectMapper().writeValueAsString(entityMap);
            String entityName = JsonPath.from(entityJson).getString("id.name");
            given()
                .queryParam(OFFICE, OFFICE_ID)
                .queryParam(CASCADE_DELETE, true)
                .header(AUTH_HEADER, TestAccounts.KeyUser.SPK_NORMAL.toHeaderValue())
            .when()
                .redirects().follow(true)
                .redirects().max(3)
                .delete("/entity/" + entityName)
            .then()
                .statusCode(isOneOf(
                        HttpServletResponse.SC_NO_CONTENT,
                        HttpServletResponse.SC_NOT_FOUND
                ));
        }
    }


    // Test CRUD  
    // create -> getOne -> update -> getOne to verify updated
    // delete -> getOne to verify deleted
    @ParameterizedTest
    @ValueSource(strings = {Formats.JSONV2, Formats.DEFAULT})
    void test_entity_create_get_update_delete(String format) throws Exception {

        TestAccounts.KeyUser user = TestAccounts.KeyUser.SPK_NORMAL;
        String entityJson = getUniqueTestEntityJsonByIndex(0);
        String entityName = JsonPath.from(entityJson).getString("id.name");
        String longName = JsonPath.from(entityJson).getString("long-name");

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
            .queryParam(OFFICE, OFFICE_ID)
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .get("/entity/" + entityName)
        .then()
            .log().ifValidationFails(LogDetail.ALL, true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_OK))
            .body("id.name", equalTo(entityName))
            .body("id.office-id", equalTo(OFFICE_ID))
            .body("long-name", equalTo(longName));

        // UPDATE — modify long-name to verify persistence
        String updatedEntityJson = entityJson.replace(
                "\"" + longName + "\"",
                "\"Updated long name\"");

        given()
            .contentType(Formats.JSONV2)
            .body(updatedEntityJson)
            .header(AUTH_HEADER, user.toHeaderValue())
            .queryParam(OFFICE, OFFICE_ID)
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .patch("/entity/" + entityName)
        .then()
            .log().ifValidationFails(LogDetail.ALL,true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_OK));

        // GET to confirm the updated field persisted
        given()
            .log().ifValidationFails(LogDetail.ALL, true)
            .accept(format)
            .queryParam(OFFICE, OFFICE_ID)
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .get("/entity/" + entityName)
        .then()
            .log().ifValidationFails(LogDetail.ALL, true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_OK))
            .body("id.name", equalTo(entityName))
            .body("id.office-id", equalTo(OFFICE_ID))
            .body("long-name", equalTo("Updated long name"));

        // DELETE
        given()
            .header(AUTH_HEADER, user.toHeaderValue())
            .queryParam(OFFICE, OFFICE_ID)
            .queryParam(CASCADE_DELETE, true)
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .delete("/entity/" + entityName)
        .then()
            .log().ifValidationFails(LogDetail.ALL,true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_NO_CONTENT));

        // verify deleted
        given()
            .accept(format)
            .queryParam(OFFICE, OFFICE_ID)
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .get("/entity/" + entityName)
        .then()
            .log().ifValidationFails(LogDetail.ALL, true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_NOT_FOUND));
    }


    // create fails if entity already exists
    @Test
    void create_duplicate_entity_bad_request() throws Exception {
        TestAccounts.KeyUser user = TestAccounts.KeyUser.SPK_NORMAL;
        String entityJson1 = getUniqueTestEntityJsonByIndex(1);

        // CREATE
        given()
            .log().ifValidationFails(LogDetail.ALL, true)
            .contentType(Formats.JSONV2)
            .body(entityJson1)
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
            .body(entityJson1)
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
    void get_one_missing_office_bad_request() throws Exception {
        TestAccounts.KeyUser user = TestAccounts.KeyUser.SPK_NORMAL;
        String entityJson2 = getUniqueTestEntityJsonByIndex(2);
        String entityName = JsonPath.from(entityJson2).getString("id.name");

        // CREATE
        given()
            .log().ifValidationFails(LogDetail.ALL, true)
            .contentType(Formats.JSONV2)
            .body(entityJson2)
            .header(AUTH_HEADER, user.toHeaderValue())
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .post("/entity")
        .then()
            .log().ifValidationFails(LogDetail.ALL, true)
            .assertThat()
            .statusCode(is(HttpServletResponse.SC_CREATED));

        // getOne with no office param
        given()
            .log().ifValidationFails(LogDetail.ALL, true)
            .accept(Formats.JSONV2)
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .get("/entity/" + entityName)
        .then()
            .log().ifValidationFails(LogDetail.ALL, true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_BAD_REQUEST));
    }


    // Entity ID in the URL must match the id.name in the request body
    @Test
    void update_non_existing_entity_id_or_missing_office_id_400_or_404() throws Exception {
        TestAccounts.KeyUser user = TestAccounts.KeyUser.SPK_NORMAL;
        String entityJson3 = getUniqueTestEntityJsonByIndex(3);
        String entityName = JsonPath.from(entityJson3).getString("id.name");

        // UPDATE - non-existing entity id - 404
        given()
            .log().ifValidationFails(LogDetail.ALL, true)
            .accept(Formats.JSONV2)
            .contentType(Formats.JSONV2)
            .body(entityJson3)
            .header(AUTH_HEADER, user.toHeaderValue())
            .queryParam(OFFICE, OFFICE_ID)
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .patch("/entity/" + entityName)
        .then()
            .log().ifValidationFails(LogDetail.ALL, true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_NOT_FOUND));

        // CREATE the non-existing entity id
        given()
            .log().ifValidationFails(LogDetail.ALL, true)
            .contentType(Formats.JSONV2)
            .body(entityJson3)
            .header(AUTH_HEADER, user.toHeaderValue())
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .post("/entity")
        .then()
            .log().ifValidationFails(LogDetail.ALL, true)
            .assertThat()
            .statusCode(is(HttpServletResponse.SC_CREATED));

        // UPDATE - missing office id - 400
        given()
            .log().ifValidationFails(LogDetail.ALL, true)
            .accept(Formats.JSONV2)
            .contentType(Formats.JSONV2)
            .body(entityJson3)
            .header(AUTH_HEADER, user.toHeaderValue())
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .patch("/entity/" + entityName)
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
            .queryParam(Controllers.PARENT_ENTITY_ID, "NOAA")
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
    void getAll_match_null_parents_flag() throws Exception {
        TestAccounts.KeyUser user = TestAccounts.KeyUser.SPK_NORMAL;
        String entityJson4 = getUniqueTestEntityJsonByIndex(4);
        String entityName = JsonPath.from(entityJson4).getString("id.name");

        // CREATE entity with null parent - default match-null-parents = true
        given()
            .log().ifValidationFails(LogDetail.ALL, true)
            .contentType(Formats.JSONV2)
            .body(entityJson4)
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
                    && OFFICE_ID.equals(id.get("office-id"))
                    && entityName.equals(id.get("name"))) {
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
                    && OFFICE_ID.equals(id.get("office-id"))
                    && entityName.equals(id.get("name"))) {
                present = true;
                break;
            }
        }

        assertFalse(present, "Entity with null parent-id should be filtered out when match-null-parents=false");

        // DELETE nullParentEntity
        given()
            .header(AUTH_HEADER, user.toHeaderValue())
            .queryParam(OFFICE, OFFICE_ID)
            .queryParam(CASCADE_DELETE, true)
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .delete("/entity/" + entityName)
        .then()
            .log().ifValidationFails(LogDetail.ALL,true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_NO_CONTENT));
    }

    // Helper to clean up test Entities
    private static List<Map<String, String>> loadTestEntityJsonList() throws Exception {
        InputStream in = EntityControllerTestIT.class.getResourceAsStream("/cwms/cda/data/dto/entity_test.json");
        assertNotNull(in);
        String json = IOUtils.toString(in, java.nio.charset.StandardCharsets.UTF_8);
        return JsonPath.from(json).getList("");
    }

    // Helper to create unique Test entities
    private static String getUniqueTestEntityJsonByIndex(int index) throws Exception {
        InputStream in = EntityControllerTestIT.class.getResourceAsStream("/cwms/cda/data/dto/entity_test.json");
        assertNotNull(in);
        String json = IOUtils.toString(in, java.nio.charset.StandardCharsets.UTF_8);
        List<Map<String, String>> entities = JsonPath.from(json).getList("");
        ObjectMapper mapper = new ObjectMapper();
        return mapper.writeValueAsString(entities.get(index));
    }
}
