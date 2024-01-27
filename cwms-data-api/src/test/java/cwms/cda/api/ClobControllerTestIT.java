package cwms.cda.api;

import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.is;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import cwms.cda.data.dto.Clob;
import cwms.cda.formatters.Formats;
import cwms.cda.formatters.json.JsonV2;
import fixtures.TestAccounts;
import io.restassured.filter.log.LogDetail;
import javax.servlet.http.HttpServletResponse;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag("integration")
public class ClobControllerTestIT extends DataApiTestIT {

    public static final String SPK = "SPK";

    @Test
    void test_getOne_not_found() {
        String clobId = "TEST";
        String urlencoded = java.net.URLEncoder.encode(clobId);

        given()
        .log().ifValidationFails(LogDetail.ALL,true)
            .accept(Formats.JSONV2)
            .queryParam(Controllers.OFFICE, SPK)
        .when()
            .get("/clobs/" + urlencoded)
        .then()
        .log().ifValidationFails(LogDetail.ALL,true)
            .assertThat()
            .statusCode(is(HttpServletResponse.SC_NOT_FOUND));
    }

    @Test
    void test_create_getOne() throws JsonProcessingException {
        String clobId = "TEST/TEST_CLOBIT2";

        String origDesc = "test description";
        String origValue = "test value";
        Clob clob = new Clob(SPK, clobId, origDesc, origValue);
        ObjectMapper om = JsonV2.buildObjectMapper();
        String serializedClob = om.writeValueAsString(clob);
        TestAccounts.KeyUser user = TestAccounts.KeyUser.SPK_NORMAL;

        given()
            .log().ifValidationFails(LogDetail.ALL,true)
            .accept(Formats.JSONV2)
            .contentType(Formats.JSONV2)
            .body(serializedClob)
            .header("Authorization",user.toHeaderValue())
            .queryParam("office",SPK)
            .queryParam("fail-if-exists",false)
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .post("/clobs/")
        .then()
            .log().ifValidationFails(LogDetail.ALL,true)
            .assertThat()
            .statusCode(is(HttpServletResponse.SC_CREATED));

//        /* There is an issue with how javalin handles / in the path that are actually part
//        of the object name (NOTE: good candidate for actually having a GUID or other "code"
//        as part of the path and the actual name as a query parameter.
//        */

        given()
            .accept(Formats.JSONV2)
            .log().ifValidationFails(LogDetail.ALL,true)
            .queryParam(Controllers.OFFICE, SPK)
            .queryParam(Controllers.CLOB_ID, clobId)
        .when()
            .get("/clobs/ignored")
        .then()
            .log().ifValidationFails(LogDetail.ALL,true)
            .assertThat()
            .statusCode(is(HttpServletResponse.SC_OK))
            .body("office-id", is(SPK))
            .body("id", is(clobId))
            .body("description", is(origDesc))
            .body("value", is(origValue));


        given()
                .accept("text/plain")
                .log().ifValidationFails(LogDetail.ALL,true)
                .queryParam(Controllers.OFFICE, SPK)
                .queryParam(Controllers.CLOB_ID, clobId)
                .when()
                .get("/clobs/ignored")
                .then()
                .log().ifValidationFails(LogDetail.ALL,true)
                .assertThat()
                .statusCode(is(HttpServletResponse.SC_OK))
                .body( is(origValue))
                ;

        // We can now do Range requests!
        given()
                .accept("text/plain")
                .log().ifValidationFails(LogDetail.ALL,true)
                .queryParam(Controllers.OFFICE, SPK)
                .queryParam(Controllers.CLOB_ID, clobId)
                .header("Range"," bytes=3-")
                .when()
                .get("/clobs/ignored")
                .then()
                .log().ifValidationFails(LogDetail.ALL,true)
                .assertThat()
                .statusCode(is(HttpServletResponse.SC_PARTIAL_CONTENT))
                .body( is("t value"))
        ;

    }
}
