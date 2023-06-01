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

import cwms.radar.data.dto.Location;
import cwms.radar.formatters.Formats;
import cwms.radar.formatters.json.JsonV1;
import fixtures.TestAccounts.KeyUser;
import io.restassured.RestAssured;
import io.restassured.path.json.config.JsonPathConfig;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import javax.servlet.http.HttpServletResponse;

import static cwms.radar.api.Controllers.CASCADE_DELETE;
import static cwms.radar.api.Controllers.OFFICE;
import static cwms.radar.data.dao.JsonRatingUtilsTest.loadResourceAsString;
import static io.restassured.RestAssured.given;
import static io.restassured.config.JsonConfig.jsonConfig;
import static org.hamcrest.Matchers.is;

@Tag("integration")
public class LocationControllerTestIT extends DataApiTestIT {

    @Test
    public void test_location_create_get_delete() throws Exception {
        String officeId = "SPK";
        String json = loadResourceAsString("cwms/radar/api/location_create.json");
        Location location = new Location.Builder(LocationController.deserializeLocation(json, Formats.JSON))
                .withOfficeId(officeId)
                .withName(getClass().getSimpleName())
                .build();
        String serializedLocation = JsonV1.buildObjectMapper().writeValueAsString(location);

        KeyUser user = KeyUser.SPK_NORMAL;
        // create location
        given()
                .log().everything(true)
                .accept(Formats.JSON)
                .contentType(Formats.JSON)
                .body(serializedLocation)
                .header("Authorization", user.toHeaderValue())
                .when()
                .redirects().follow(true)
                .redirects().max(3)
                .post("/locations")
                .then()
                .log().body().log().everything(true)
                .assertThat()
                .statusCode(is(HttpServletResponse.SC_ACCEPTED));
        //Create associated time series so delete fails without cascade
        try {
            createTimeseries(officeId, location.getName() + ".Flow.Inst.~1Hour.0.radar-test");
        } catch (Exception ex) {

        }

        // get it back
        given()
                .config(RestAssured.config().jsonConfig(jsonConfig().numberReturnType(JsonPathConfig.NumberReturnType.DOUBLE)))
                .log().everything(true)
                .accept(Formats.JSON)
                .header("Authorization", user.toHeaderValue())
                .queryParam("office", officeId)
                .when()
                .redirects().follow(true)
                .redirects().max(3)
                .get("/locations/" + location.getName())
                .then()
                .log().body().log().everything(true)
                .assertThat()
                .statusCode(is(HttpServletResponse.SC_OK));

        // delete without cascade should fail
        given()
                .config(RestAssured.config().jsonConfig(jsonConfig().numberReturnType(JsonPathConfig.NumberReturnType.DOUBLE)))
                .log().everything(true)
                .accept(Formats.JSON)
                .header("Authorization", user.toHeaderValue())
                .queryParam(OFFICE, officeId)
                .queryParam(CASCADE_DELETE, false)
                .when()
                .redirects().follow(true)
                .redirects().max(3)
                .delete("/locations/" + location.getName())
                .then()
                .log().body().log().everything(true)
                .assertThat()
                .statusCode(is(HttpServletResponse.SC_METHOD_NOT_ALLOWED));

        // delete without cascade should fail
        given()
                .config(RestAssured.config().jsonConfig(jsonConfig().numberReturnType(JsonPathConfig.NumberReturnType.DOUBLE)))
                .log().everything(true)
                .accept(Formats.JSON)
                .header("Authorization", user.toHeaderValue())
                .queryParam(OFFICE, officeId)
                .queryParam(CASCADE_DELETE, true)
                .when()
                .redirects().follow(true)
                .redirects().max(3)
                .delete("/locations/" + location.getName())
                .then()
                .log().body().log().everything(true)
                .assertThat()
                .statusCode(is(HttpServletResponse.SC_ACCEPTED));

        // get it back
        given()
                .config(RestAssured.config().jsonConfig(jsonConfig().numberReturnType(JsonPathConfig.NumberReturnType.DOUBLE)))
                .log().everything(true)
                .accept(Formats.JSON)
                .header("Authorization", user.toHeaderValue())
                .queryParam("office", officeId)
                .when()
                .redirects().follow(true)
                .redirects().max(3)
                .get("/locations/" + location.getName())
                .then()
                .log().body().log().everything(true)
                .assertThat()
                .statusCode(is(HttpServletResponse.SC_NOT_FOUND));
    }
}
