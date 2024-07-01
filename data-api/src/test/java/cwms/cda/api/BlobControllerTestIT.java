package cwms.cda.api;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import cwms.cda.data.dto.Blob;
import cwms.cda.formatters.Formats;
import cwms.cda.formatters.json.JsonV2;
import fixtures.TestAccounts;
import io.restassured.filter.log.LogDetail;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import javax.servlet.http.HttpServletResponse;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;

import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.is;

@Tag("integration")
public class BlobControllerTestIT extends DataApiTestIT {

    public static final String SPK = "SPK";
    private static final String EXISTING_BLOB_ID = "TEST_BLOBIT2";
    private static final String EXISTING_BLOB_VALUE = "test value";

    @BeforeAll
    static void createExistingBlob() throws Exception
    {
        String origDesc = "test description";
        byte[] origBytes = EXISTING_BLOB_VALUE.getBytes();

        String mediaType = "application/octet-stream";
        Blob blob = new Blob(SPK, EXISTING_BLOB_ID, origDesc, mediaType, origBytes);
        ObjectMapper om = JsonV2.buildObjectMapper();
        String serializedBlob = om.writeValueAsString(blob);
        TestAccounts.KeyUser user = TestAccounts.KeyUser.SPK_NORMAL;

        given()
                .log().ifValidationFails(LogDetail.ALL,true)
                .contentType(Formats.JSONV2)
                .body(serializedBlob)
                .header("Authorization",user.toHeaderValue())
                .queryParam("office",SPK)
                .queryParam("fail-if-exists",false)
            .when()
                .redirects().follow(true)
                .redirects().max(3)
                .post("/blobs/")
            .then()
                .log().ifValidationFails(LogDetail.ALL,true)
                .assertThat()
                .statusCode(is(HttpServletResponse.SC_CREATED));
    }

    @Test
    void test_getOne_not_found() throws UnsupportedEncodingException {
        String blobId = "TEST";
        String urlencoded = URLEncoder.encode(blobId, "UTF-8");

        given()
            .log().ifValidationFails(LogDetail.ALL,true)
            .accept(Formats.JSONV2)
            .queryParam(Controllers.OFFICE, SPK)
        .when()
            .get("/blobs/" + urlencoded)
        .then()
            .log().ifValidationFails(LogDetail.ALL,true)
            .assertThat()
            .statusCode(is(HttpServletResponse.SC_NOT_FOUND));
    }


    @Test
    void test_create_getOne() throws JsonProcessingException
    {
        /* There is an issue with how javalin handles / in the path that are actually part
        of the object name (NOTE: good candidate for actually having a GUID or other "code"
        as part of the path and the actual name as a query parameter.
        */

        given()
            .log().ifValidationFails(LogDetail.ALL, true)
            .accept(Formats.JSONV2)
            .queryParam(Controllers.OFFICE, SPK)
        .when()
            .get("/blobs/" + EXISTING_BLOB_ID)
        .then()
            .log().ifValidationFails(LogDetail.ALL, true)
            .assertThat()
            .statusCode(is(HttpServletResponse.SC_OK))
            .body(is(EXISTING_BLOB_VALUE));
    }

    @Test
    void test_blob_get_one_default()
    {
        given()
            .log()
            .ifValidationFails(LogDetail.ALL, true)
            .queryParam(Controllers.OFFICE, SPK)
        .when()
            .get("/blobs/" + EXISTING_BLOB_ID)
        .then()
            .log().ifValidationFails(LogDetail.ALL, true)
            .assertThat()
            .statusCode(is(HttpServletResponse.SC_OK))
            .body(is(EXISTING_BLOB_VALUE));
    }

    @Test
    void test_blob_range()
    {
        // We can now do Range requests!
        given()
            .log().ifValidationFails(LogDetail.ALL,true)
            .queryParam(Controllers.OFFICE, SPK)
            .header("Range"," bytes=3-")
        .when()
            .get("/blobs/" + EXISTING_BLOB_ID)
        .then()
            .log().ifValidationFails(LogDetail.ALL,true)
            .assertThat()
            .statusCode(is(HttpServletResponse.SC_PARTIAL_CONTENT))
            .body( is("t value"));
    }

    @ParameterizedTest
    @EnumSource(GetAllTest.class)
    void test_blob_get_all_default_alias(GetAllTest test)
    {
        given()
            .log().ifValidationFails(LogDetail.ALL,true)
            .accept(test._accept)
            .queryParam(Controllers.OFFICE, SPK)
        .when()
            .get("/blobs/")
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
        JSONV2(Formats.JSONV2, Formats.JSONV2),
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
