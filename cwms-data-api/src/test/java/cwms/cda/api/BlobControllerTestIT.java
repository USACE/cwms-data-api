package cwms.cda.api;

import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.is;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import cwms.cda.data.dto.Blob;
import cwms.cda.formatters.Formats;
import cwms.cda.formatters.json.JsonV2;
import fixtures.TestAccounts;
import io.restassured.filter.log.LogDetail;
import java.io.UnsupportedEncodingException;
import javax.servlet.http.HttpServletResponse;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag("integration")
public class BlobControllerTestIT extends DataApiTestIT {

    public static final String SPK = "SPK";

    @Test
    void test_getOne_not_found() throws UnsupportedEncodingException {
        String blobId = "TEST";
        String urlencoded = java.net.URLEncoder.encode(blobId, "UTF-8");

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
    void test_create_getOne() throws JsonProcessingException {
        String blobId = "TEST_BLOBIT2";

        String origDesc = "test description";
        String origValue = "test value";
        byte[] origBytes = origValue.getBytes();

        String mediaType = "application/octet-stream";
        Blob blob = new Blob(SPK, blobId, origDesc, mediaType, origBytes);
        ObjectMapper om = JsonV2.buildObjectMapper();
        String serializedBlob = om.writeValueAsString(blob);
        TestAccounts.KeyUser user = TestAccounts.KeyUser.SPK_NORMAL;

        given()
            .log().ifValidationFails(LogDetail.ALL,true)
            .accept(Formats.JSONV2)
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

//        /* There is an issue with how javalin handles / in the path that are actually part
//        of the object name (NOTE: good candidate for actually having a GUID or other "code"
//        as part of the path and the actual name as a query parameter.
//        */

        given()
            .accept(Formats.JSONV2)
            .log().ifValidationFails(LogDetail.ALL,true)
            .queryParam(Controllers.OFFICE, SPK)
        .when()
            .get("/blobs/" + blobId)
        .then()
            .log().ifValidationFails(LogDetail.ALL,true)
            .assertThat()
            .statusCode(is(HttpServletResponse.SC_OK))
            .body( is(origValue));


        given()
                .log().ifValidationFails(LogDetail.ALL,true)
                .queryParam(Controllers.OFFICE, SPK)
                .when()
                .get("/blobs/" + blobId)
                .then()
                .log().ifValidationFails(LogDetail.ALL,true)
                .assertThat()
                .statusCode(is(HttpServletResponse.SC_OK))
                .body( is(origValue))
                ;

        // We can now do Range requests!
        given()
                .log().ifValidationFails(LogDetail.ALL,true)
                .queryParam(Controllers.OFFICE, SPK)
                .header("Range"," bytes=3-")
                .when()
                .get("/blobs/" + blobId)
                .then()
                .log().ifValidationFails(LogDetail.ALL,true)
                .assertThat()
                .statusCode(is(HttpServletResponse.SC_PARTIAL_CONTENT))
                .body( is("t value"))
        ;

    }
}
