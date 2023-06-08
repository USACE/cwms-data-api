/*
 * MIT License
 *
 * Copyright (c) 2023 Hydrologic Engineering Center
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

package cwms.radar.api;

import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import cwms.radar.data.dto.SpecifiedLevel;
import cwms.radar.formatters.Formats;
import cwms.radar.formatters.json.JsonV2;
import fixtures.CwmsDataApiSetupCallback;
import fixtures.TestAccounts;
import java.time.Instant;
import javax.servlet.http.HttpServletResponse;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@Tag("integration")
@ExtendWith(CwmsDataApiSetupCallback.class)
public class SpecifiedLevelIdIT extends DataApiTestIT {

    public static final String OFFICE = "SPK";

    @Test
    void test_create_read_delete() throws JsonProcessingException {
        ObjectMapper om = JsonV2.buildObjectMapper();
        TestAccounts.KeyUser user = TestAccounts.KeyUser.SPK_NORMAL;
        SpecifiedLevel specifiedLevel = new SpecifiedLevel("TestCRD" + Instant.now().getEpochSecond(), OFFICE, "CDA Integration Test");
        String serializedLevel = om.writeValueAsString(specifiedLevel);
        //Create
        given()
            .accept(Formats.JSONV2)
            .contentType(Formats.JSONV2)
            .body(serializedLevel)
            .header("Authorization", user.toHeaderValue())
            .queryParam(Controllers.OFFICE, OFFICE)
            .when()
            .redirects().follow(true)
            .redirects().max(3)
            .post("/specified-levels/")
            .then()
            .assertThat()
            .statusCode(is(HttpServletResponse.SC_CREATED));

        //Read
        given()
            .accept(Formats.JSONV2)
            .contentType(Formats.JSONV2)
            .header("Authorization", user.toHeaderValue())
            .queryParam("office", OFFICE)
            .queryParam(Controllers.TEMPLATE_ID_MASK, specifiedLevel.getId())
            .when()
            .redirects().follow(true)
            .redirects().max(3)
            .get("/specified-levels/")
            .then()
            .assertThat()
            .log().body().log().everything(true)
            .body("[0].id", equalTo(specifiedLevel.getId()))
            .body("[0].office-id", equalTo(specifiedLevel.getOfficeId()))
            .body("[0].description", equalTo(specifiedLevel.getDescription()))
            .statusCode(is(HttpServletResponse.SC_OK));
        //Delete
        given()
            .accept(Formats.JSONV2)
            .contentType(Formats.JSONV2)
            .header("Authorization", user.toHeaderValue())
            .queryParam("office", OFFICE)
            .queryParam(Controllers.TEMPLATE_ID_MASK, specifiedLevel.getId())
            .when()
            .redirects().follow(true)
            .redirects().max(3)
            .delete("/specified-levels/" + specifiedLevel.getId())
            .then()
            .assertThat()
            .log().body().log().everything(true)
            .statusCode(is(HttpServletResponse.SC_NO_CONTENT));

        //Read Empty
        given()
            .accept(Formats.JSONV2)
            .contentType(Formats.JSONV2)
            .header("Authorization", user.toHeaderValue())
            .queryParam("office", OFFICE)
            .queryParam(Controllers.TEMPLATE_ID_MASK, specifiedLevel.getId())
            .when()
            .redirects().follow(true)
            .redirects().max(3)
            .get("/specified-levels/")
            .then()
            .assertThat()
            .log().body().log().everything(true)
            .body("size()", is(0))
            .statusCode(is(HttpServletResponse.SC_OK));
    }


    @Test
    void test_update() throws JsonProcessingException {
        ObjectMapper om = JsonV2.buildObjectMapper();
        TestAccounts.KeyUser user = TestAccounts.KeyUser.SPK_NORMAL;
        long epochSeconds = Instant.now().getEpochSecond();
        SpecifiedLevel specifiedLevel = new SpecifiedLevel("TestUpdate" + epochSeconds, OFFICE, "CDA Integration Test");
        String serializedLevel = om.writeValueAsString(specifiedLevel);
        String newId = "Test" + (epochSeconds + 1);
        //Create
        given()
            .accept(Formats.JSONV2)
            .contentType(Formats.JSONV2)
            .body(serializedLevel)
            .header("Authorization", user.toHeaderValue())
            .queryParam(Controllers.OFFICE, OFFICE)
            .when()
            .redirects().follow(true)
            .redirects().max(3)
            .post("/specified-levels/")
            .then()
            .assertThat()
            .statusCode(is(HttpServletResponse.SC_CREATED));
        //Update
        given()
            .accept(Formats.JSONV2)
            .contentType(Formats.JSONV2)
            .header("Authorization", user.toHeaderValue())
            .queryParam(Controllers.OFFICE, OFFICE)
            .queryParam(Controllers.SPECIFIED_LEVEL_ID, newId)
            .when()
            .redirects().follow(true)
            .redirects().max(3)
            .patch("/specified-levels/" + specifiedLevel.getId())
            .then()
            .assertThat()
            .statusCode(is(HttpServletResponse.SC_NO_CONTENT));

        //Read
        given()
            .accept(Formats.JSONV2)
            .contentType(Formats.JSONV2)
            .header("Authorization", user.toHeaderValue())
            .queryParam(Controllers.OFFICE, OFFICE)
            .queryParam(Controllers.TEMPLATE_ID_MASK, newId)
            .when()
            .redirects().follow(true)
            .redirects().max(3)
            .get("/specified-levels/")
            .then()
            .assertThat()
            .log().body().log().everything(true)
            .body("[0].id", equalTo(newId))
            .statusCode(is(HttpServletResponse.SC_OK));
    }


    @Test
    void test_update_does_not_exist() throws JsonProcessingException {
        TestAccounts.KeyUser user = TestAccounts.KeyUser.SPK_NORMAL;
        long epochSeconds = Instant.now().getEpochSecond();
        SpecifiedLevel specifiedLevel = new SpecifiedLevel("BadUpdate" + epochSeconds, OFFICE, "CDA Integration Test");
        //Update
        given()
            .accept(Formats.JSONV2)
            .contentType(Formats.JSONV2)
            .header("Authorization", user.toHeaderValue())
            .queryParam(Controllers.OFFICE, OFFICE)
            .queryParam(Controllers.SPECIFIED_LEVEL_ID, specifiedLevel.getId())
            .when()
            .redirects().follow(true)
            .redirects().max(3)
            .patch("/specified-levels/" + specifiedLevel.getId())
            .then()
            .assertThat()
            .statusCode(is(HttpServletResponse.SC_BAD_REQUEST));
    }


    @Test
    void test_delete_does_not_exist() throws JsonProcessingException {
        TestAccounts.KeyUser user = TestAccounts.KeyUser.SPK_NORMAL;
        long epochSeconds = Instant.now().getEpochSecond();
        SpecifiedLevel specifiedLevel = new SpecifiedLevel("TestBadDelete" + epochSeconds, OFFICE, "CDA Integration Test");
        //Update
        given()
            .accept(Formats.JSONV2)
            .contentType(Formats.JSONV2)
            .header("Authorization", user.toHeaderValue())
            .queryParam(Controllers.OFFICE, OFFICE)
            .when()
            .redirects().follow(true)
            .redirects().max(3)
            .delete("/specified-levels/" + specifiedLevel.getId())
            .then()
            .assertThat()
            .statusCode(is(HttpServletResponse.SC_NOT_FOUND));
    }
}
