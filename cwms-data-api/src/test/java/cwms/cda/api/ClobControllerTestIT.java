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
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import javax.servlet.http.HttpServletResponse;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

@Tag("integration")
public class ClobControllerTestIT extends DataApiTestIT {

    public static final String SPK = "SPK";
    private static final String EXISTING_CLOB_ID = "TEST/TEST_CLOBIT2";
    private static final String EXISTING_CLOB_VALUE = "test value";
    private static final String EXISTING_CLOB_DESC = "test description";

    @BeforeAll
    static void createExistingClob() throws Exception
    {
        Clob clob = new Clob(SPK, EXISTING_CLOB_ID, EXISTING_CLOB_DESC, EXISTING_CLOB_VALUE);
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
    }

    @Test
    void test_getOne_notFound() throws UnsupportedEncodingException {
        String clobId = "TEST";
        String urlencoded = URLEncoder.encode(clobId, "UTF-8");

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
    void test_getOne_jsonv2()
	{
        /* There is an issue with how javalin handles / in the path that are actually part
        of the object name (NOTE: good candidate for actually having a GUID or other "code"
        as part of the path and the actual name as a query parameter.
        */

        given()
            .accept(Formats.JSONV2)
            .log().ifValidationFails(LogDetail.ALL,true)
            .queryParam(Controllers.OFFICE, SPK)
            .queryParam(Controllers.CLOB_ID, EXISTING_CLOB_ID)
        .when()
            .get("/clobs/ignored")
        .then()
            .log().ifValidationFails(LogDetail.ALL,true)
            .assertThat()
            .statusCode(is(HttpServletResponse.SC_OK))
            .body("office-id", is(SPK))
            .body("id", is(EXISTING_CLOB_ID))
            .body("description", is(EXISTING_CLOB_DESC))
            .body("value", is(EXISTING_CLOB_VALUE));
    }

    @Test
    void test_getOne_plainText_withRange()
    {
        // We can now do Range requests!
        given()
            .accept("text/plain")
            .log().ifValidationFails(LogDetail.ALL,true)
            .queryParam(Controllers.OFFICE, SPK)
            .queryParam(Controllers.CLOB_ID, EXISTING_CLOB_ID)
            .header("Range"," bytes=3-")
        .when()
            .get("/clobs/ignored")
        .then()
            .log().ifValidationFails(LogDetail.ALL,true)
            .assertThat()
            .statusCode(is(HttpServletResponse.SC_PARTIAL_CONTENT))
            .body( is("t value"));
    }

    @Test
    void test_getOne_plain_text()
    {
        given()
            .accept("text/plain")
            .log().ifValidationFails(LogDetail.ALL,true)
            .queryParam(Controllers.OFFICE, SPK)
            .queryParam(Controllers.CLOB_ID, EXISTING_CLOB_ID)
        .when()
            .get("/clobs/ignored")
        .then()
            .log().ifValidationFails(LogDetail.ALL,true)
            .assertThat()
            .statusCode(is(HttpServletResponse.SC_OK))
            .body( is(EXISTING_CLOB_VALUE));
    }


    @ParameterizedTest
    @EnumSource(GetAllTest.class)
    void test_getAll_aliases(GetAllTest test)
    {
        given()
                .accept("text/plain")
                .log().ifValidationFails(LogDetail.ALL,true)
                .queryParam(Controllers.OFFICE, SPK)
                .accept(test._accept)
            .when()
                .get("/clobs/")
            .then()
                .log().ifValidationFails(LogDetail.ALL,true)
                .assertThat()
                .statusCode(is(HttpServletResponse.SC_OK))
                .contentType(is(test._expectedContentType));
    }

    enum GetAllTest
    {
        DEFAULT(Formats.DEFAULT, Formats.JSONV2),
        JSON(Formats.JSON, Formats.JSONV2),
        JSONV1(Formats.JSONV1, Formats.JSONV1),
        JSONV2(Formats.JSONV2, Formats.JSONV2),
        XML(Formats.XML, Formats.XMLV2),
        ;

        final String _accept;
        final String _expectedContentType;

        GetAllTest(String accept, String expectedContentType)
        {
            _accept = accept;
            _expectedContentType = expectedContentType;
        }
    }
}
