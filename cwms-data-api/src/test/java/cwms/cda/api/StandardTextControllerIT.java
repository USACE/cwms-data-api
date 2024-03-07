/*
 * MIT License
 *
 * Copyright (c) 2024 Hydrologic Engineering Center
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

package cwms.cda.api;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import cwms.cda.formatters.Formats;
import fixtures.TestAccounts.KeyUser;
import io.restassured.RestAssured;
import io.restassured.filter.log.LogDetail;
import io.restassured.path.json.config.JsonPathConfig;
import org.apache.commons.io.IOUtils;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import javax.servlet.http.HttpServletResponse;
import java.util.HashMap;
import java.util.Map;

import static io.restassured.RestAssured.given;
import static io.restassured.config.JsonConfig.jsonConfig;
import static org.hamcrest.Matchers.*;

@Tag("integration")
public class StandardTextControllerIT extends DataApiTestIT {

    public static final String controllerPath = "/standard-text-id/";

    @Test
    void test_standard_text_crud() throws Exception {
        ObjectMapper mapper = new ObjectMapper();

        String tsData = IOUtils.toString(getClass()
                .getResourceAsStream("/cwms/cda/api/standard-text.json"),"UTF-8"
            );

        JsonNode ts = mapper.readTree(tsData);
        String officeId = ts.get("id").get("office-id").asText();
        KeyUser user = KeyUser.SPK_NORMAL;
        // inserting the standard text
        given()
            .log().ifValidationFails(LogDetail.ALL,true)
            .accept(Formats.JSONV2)
            .contentType(Formats.JSONV2)
            .body(tsData)
            .header("Authorization",user.toHeaderValue())
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .post(controllerPath)
        .then()
            .log().ifValidationFails(LogDetail.ALL,true)
            .assertThat()
            .statusCode(is(HttpServletResponse.SC_CREATED));

        // get it back
        given()
            .config(RestAssured.config().jsonConfig(jsonConfig().numberReturnType(JsonPathConfig.NumberReturnType.DOUBLE)))
            .log().ifValidationFails(LogDetail.ALL,true)
            .accept(Formats.JSONV2)
            .queryParam("office",officeId)
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .get(controllerPath  + "HW")
        .then()
            .log().ifValidationFails(LogDetail.ALL,true)
            .assertThat()
            .statusCode(is(HttpServletResponse.SC_OK))
            .body("standard-text",equalTo("Hello, World"))
            .body("id.id",equalTo("HW"))
            .body("id.office-id",equalTo(officeId));

        // delete
        given()
            .config(RestAssured.config().jsonConfig(jsonConfig().numberReturnType(JsonPathConfig.NumberReturnType.DOUBLE)))
            .log().ifValidationFails(LogDetail.ALL,true)
            .accept(Formats.JSONV2)
            .header("Authorization",user.toHeaderValue())
            .queryParam("office",officeId)
            .queryParam("method","DELETE_ALL")
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .delete(controllerPath + "HW")
        .then()
            .log().ifValidationFails(LogDetail.ALL,true)
            .assertThat()
            .statusCode(is(HttpServletResponse.SC_NO_CONTENT));

        // should now be gone
        given()
            .config(RestAssured.config().jsonConfig(jsonConfig().numberReturnType(JsonPathConfig.NumberReturnType.DOUBLE)))
            .log().ifValidationFails(LogDetail.ALL,true)
            .accept(Formats.JSONV2)
            .queryParam("office",officeId)
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .get(controllerPath
                    + "HW")
        .then()
            .log().ifValidationFails(LogDetail.ALL,true)
            .assertThat()
            .statusCode(is(HttpServletResponse.SC_NOT_FOUND));
    }

    @Test
    void test_standard_text_catalog() {
        //Pulling out one CWMS owned standard text that should always exist
        Map<String, Object> expectedValue = new HashMap<>();
        expectedValue.put("standard-text", "NO RECORD");
        Map<String, String> idFields = new HashMap<>();
        idFields.put("office-id", "CWMS");
        idFields.put("id", "A");
        expectedValue.put("id", idFields);

        given()
            .config(RestAssured.config().jsonConfig(jsonConfig().numberReturnType(JsonPathConfig.NumberReturnType.DOUBLE)))
            .log().ifValidationFails(LogDetail.ALL,true)
            .accept(Formats.JSONV2)
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .get(controllerPath)
        .then()
            .log().ifValidationFails(LogDetail.ALL,true)
            .assertThat()
            .statusCode(is(HttpServletResponse.SC_OK))
                .body("values", hasItem(expectedValue));
    }
}
