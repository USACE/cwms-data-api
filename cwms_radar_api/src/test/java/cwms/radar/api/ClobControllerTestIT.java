package cwms.radar.api;

import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.is;

import java.io.UnsupportedEncodingException;
import java.nio.charset.Charset;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import cwms.radar.data.dto.Clob;
import cwms.radar.formatters.Formats;
import cwms.radar.formatters.json.JsonV2;
import fixtures.RadarApiSetupCallback;
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
        String clobId = "TEST/TEST_CLOBIT";
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

        given()
                .log().everything(true)
                .accept(Formats.JSONV2)
                .body(serializedClob)
                .header("Authorization","apikey l2testkey")
                //.param("office",SPK)
                .when()
                .redirects().follow(true)
                .redirects().max(3)
                .post("/clobs/")
                .then()        
                .log().body().log().everything(true)
                .assertThat()
                .statusCode(is(200));

        String urlencoded = java.net.URLEncoder.encode(clobId,"UTF-8");
        given()
                .accept(Formats.JSONV2)
                .log().everything(true)
                .queryParam(ClobController.OFFICE, SPK)
                .when()
                .get("/clobs/" + urlencoded)
                .then()
                .log().body().log().everything(true)
                .assertThat()
                .statusCode(is(200))
                .body("office", is(SPK))
                .body("id", is(clobId))
                .body("description", is(origDesc))
                .body("value", is(origValue))
                
        ;

    }


}
