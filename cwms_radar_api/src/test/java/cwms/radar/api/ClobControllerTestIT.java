package cwms.radar.api;

import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.is;

import java.io.UnsupportedEncodingException;
import java.nio.charset.Charset;

import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletResponse;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import cwms.radar.data.dto.Clob;
import cwms.radar.formatters.Formats;
import cwms.radar.formatters.json.JsonV2;
import fixtures.RadarApiSetupCallback;
import fixtures.TestAccounts;
import io.restassured.response.Response;
import io.restassured.response.ValidatableResponse;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@Tag("integration")
@ExtendWith(RadarApiSetupCallback.class)
public class ClobControllerTestIT {


    public static final String SPK = "SPK";

    @Test
    void test_getOne_not_found() {
        String clobId = "TEST";
        String urlencoded = java.net.URLEncoder.encode(clobId);

        Response response = given()
                .log().everything(true)
                .accept(Formats.JSONV2)
                .queryParam(ClobController.OFFICE, SPK)
                .get("/clobs/" + urlencoded);

        response.then().log().everything(true).assertThat()
                .statusCode(is(404))
        ;

    }

    @Test
    void test_create_getOne() throws JsonProcessingException, UnsupportedEncodingException {
        String clobId = "TEST/TEST_CLOBIT2";

        String origDesc = "test description";
        String origValue = "test value";
        Clob clob = new Clob(SPK, clobId, origDesc, origValue);
        ObjectMapper om = JsonV2.buildObjectMapper();
        String serializedClob = om.writeValueAsString(clob);
        TestAccounts.KeyUser user = TestAccounts.KeyUser.SPK_NORMAL;

        given()
                .log().everything(true)
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
                .log().body().log().everything(true)
                .assertThat()
                .statusCode(is(HttpServletResponse.SC_CREATED));

        /*
        String urlencoded = java.net.URLEncoder.encode(clobId);
        given()
                .accept(Formats.JSONV2)
                .log().everything(true)
                .queryParam(ClobController.OFFICE, SPK)
                .when()
                .get("/clobs/"+urlencoded)//{clobId}", clobId)
                .then()
                .log().body().log().everything(true)
                .assertThat()
                .statusCode(is(200))
                .body("office", is(SPK))
                .body("id", is(clobId))
                .body("description", is(origDesc))
                .body("value", is(origValue))
                
        ;
        */

    }


}
