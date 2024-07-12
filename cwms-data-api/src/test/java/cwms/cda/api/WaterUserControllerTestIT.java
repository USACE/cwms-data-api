/*
 *
 * MIT License
 *
 * Copyright (c) 2024 Hydrologic Engineering Center
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 *  to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
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
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER
 * DEALINGS IN THE
 * SOFTWARE.
 */

package cwms.cda.api;

import cwms.cda.data.dao.JooqDao.DeleteMethod;
import cwms.cda.data.dto.watersupply.WaterUserContract;
import cwms.cda.formatters.ContentType;
import cwms.cda.formatters.Formats;
import cwms.cda.formatters.json.JsonV1;
import fixtures.TestAccounts;
import io.restassured.filter.log.LogDetail;
import org.apache.commons.io.IOUtils;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import javax.servlet.http.HttpServletResponse;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;

import static cwms.cda.api.Controllers.*;
import static cwms.cda.security.KeyAccessManager.AUTH_HEADER;
import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

@Tag("integration")
class WaterUserControllerTestIT extends DataApiTestIT {
    private static final String OFFICE_ID = "SPK";
    private static final WaterUserContract CONTRACT;
    static {
        try (InputStream userStream = WaterUserContract.class.getResourceAsStream("/cwms/cda/api/waterusercontract.json")) {
            assert userStream != null;
            String contractJson = IOUtils.toString(userStream, StandardCharsets.UTF_8);
            CONTRACT = Formats.parseContent(new ContentType(Formats.JSONV1), contractJson, WaterUserContract.class);
        } catch(Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    @Test
    void test_create_get_delete_WaterUser() throws Exception {
        // Test Structure
        // 1) Create a WaterUser
        // 2) Get the WaterUser, assert that it exists
        // 3) Delete the WaterUser
        // 4) Get the WaterUser, assert that it does not exist

        TestAccounts.KeyUser user = TestAccounts.KeyUser.SPK_NORMAL;
        String json = JsonV1.buildObjectMapper().writeValueAsString(CONTRACT.getWaterUser());

        // create WaterUser
        given()
            .log().ifValidationFails(LogDetail.ALL, true)
            .contentType(Formats.JSONV1)
            .accept(Formats.JSONV1)
            .body(json)
            .header(AUTH_HEADER, user.toHeaderValue())
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .post("/projects/" + OFFICE_ID + "/" + CONTRACT.getWaterUser().getParentLocationRef().getName() + "/water-user")
        .then()
            .log().ifValidationFails(LogDetail.ALL, true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_CREATED))
        ;

        // get WaterUser, assert that it is correct
        given()
            .log().ifValidationFails(LogDetail.ALL, true)
            .contentType(Formats.JSONV1)
            .accept(Formats.JSONV1)
            .header(AUTH_HEADER, user.toHeaderValue())
            .queryParam(LOCATION_ID, CONTRACT.getWaterUser().getParentLocationRef().getName())
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .get("/projects/" + OFFICE_ID + "/" + CONTRACT.getWaterUser().getParentLocationRef().getName()
                    + "/water-user/" + CONTRACT.getWaterUser().getEntityName())
        .then()
            .log().ifValidationFails(LogDetail.ALL, true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_OK))
            .body("entity-name", equalTo(CONTRACT.getWaterUser().getEntityName()))
            .body("parent-location-ref.name", equalTo(CONTRACT.getWaterUser().getParentLocationRef().getName()))
            .body("parent-location-ref.office-id", equalTo(CONTRACT.getWaterUser().getParentLocationRef().getOfficeId()))
            .body("water-right", equalTo(CONTRACT.getWaterUser().getWaterRight()))
        ;

        // delete WaterUser
        given()
            .log().ifValidationFails(LogDetail.ALL, true)
            .contentType(Formats.JSONV1)
            .accept(Formats.JSONV1)
            .header(AUTH_HEADER, user.toHeaderValue())
            .queryParam(LOCATION_ID, CONTRACT.getWaterUser().getParentLocationRef().getName())
            .queryParam(DELETE_MODE, DeleteMethod.DELETE_ALL.toString())
            .pathParam(WATER_USER, CONTRACT.getWaterUser().getEntityName())
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .delete("/projects/" + OFFICE_ID + CONTRACT.getWaterUser().getParentLocationRef().getName()
                    + "/water-user/" + CONTRACT.getWaterUser().getEntityName())
        .then()
            .log().ifValidationFails(LogDetail.ALL, true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_OK))
        ;

        // get water user and assert that it does not exist
        given()
            .log().ifValidationFails(LogDetail.ALL, true)
            .contentType(Formats.JSONV1)
            .accept(Formats.JSONV1)
            .header(AUTH_HEADER, user.toHeaderValue())
            .queryParam(LOCATION_ID, CONTRACT.getWaterUser().getParentLocationRef().getName())
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .get("/projects/" + OFFICE_ID + "/" + CONTRACT.getWaterUser().getParentLocationRef().getName()
                    + "/water-user/" + CONTRACT.getWaterUser().getEntityName())
        .then()
            .log().ifValidationFails(LogDetail.ALL, true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_NOT_FOUND))
        ;
    }

    @Test
    void test_rename_WaterUser() throws Exception {
        // Test Structure
        // 1) Create WaterUser
        // 2) Rename WaterUser
        // 3) Get WaterUser, assert name has changed

        TestAccounts.KeyUser user = TestAccounts.KeyUser.SPK_NORMAL;
        String json = JsonV1.buildObjectMapper().writeValueAsString(CONTRACT.getWaterUser());

        // Create WaterUser
        given()
            .log().ifValidationFails(LogDetail.ALL, true)
            .contentType(Formats.JSONV1)
            .body(json)
            .accept(Formats.JSONV1)
            .header(AUTH_HEADER, user.toHeaderValue())
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .post("/projects/" + OFFICE_ID + "/" + CONTRACT.getWaterUser().getParentLocationRef().getName() + "/water-user")
        .then()
            .log().ifValidationFails(LogDetail.ALL, true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_CREATED))
        ;

        // Rename WaterUser
        given()
            .log().ifValidationFails(LogDetail.ALL, true)
            .contentType(Formats.JSONV1)
            .body(json)
            .accept(Formats.JSONV1)
            .header(AUTH_HEADER, user.toHeaderValue())
            .pathParam(WATER_USER, CONTRACT.getWaterUser().getEntityName())
            .queryParam(LOCATION_ID, CONTRACT.getWaterUser().getParentLocationRef().getName())
            .queryParam(NAME, "NEW USER NAME")
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .patch("/projects/" + OFFICE_ID + "/" + CONTRACT.getWaterUser().getParentLocationRef().getName()
                    + "/water-user/" + CONTRACT.getWaterUser().getEntityName())
        .then()
            .log().ifValidationFails(LogDetail.ALL, true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_NO_CONTENT))
        ;

        // Get WaterUser, assert name has changed
        given()
            .log().ifValidationFails(LogDetail.ALL, true)
            .contentType(Formats.JSONV1)
            .accept(Formats.JSONV1)
            .header(AUTH_HEADER, user.toHeaderValue())
            .pathParam(WATER_USER, "NEW USER NAME")
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .post("/projects/" + OFFICE_ID + "/" + CONTRACT.getWaterUser().getParentLocationRef().getName()
                    + "/water-user/" + "NEW USER NAME")
        .then()
            .log().ifValidationFails(LogDetail.ALL, true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_OK))
            .body("entity-name", equalTo("NEW USER NAME"))
        ;

    }

    @Test
    void test_getAllWaterUsers() {
        given()
            .log().ifValidationFails(LogDetail.ALL, true)
            .accept(Formats.JSONV1)
            .header(AUTH_HEADER, TestAccounts.KeyUser.SPK_NORMAL.toHeaderValue())
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .get("/projects/" + OFFICE_ID + "/" + CONTRACT.getWaterUser().getParentLocationRef().getName() + "/water-user")
        .then()
            .log().ifValidationFails(LogDetail.ALL, true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_OK))

        ;
    }
}
