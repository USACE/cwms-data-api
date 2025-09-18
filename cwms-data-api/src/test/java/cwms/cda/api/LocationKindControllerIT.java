/*
 *
 * MIT License
 *
 * Copyright (c) 2025 Hydrologic Engineering Center
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

import static cwms.cda.api.Controllers.LOCATION_KIND_LIKE;
import static cwms.cda.api.Controllers.NAMES;
import static cwms.cda.api.Controllers.OFFICE;
import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

import cwms.cda.data.dto.CwmsId;
import cwms.cda.data.dto.basin.Basin;
import cwms.cda.formatters.Formats;
import fixtures.TestAccounts;
import io.restassured.filter.log.LogDetail;
import javax.servlet.http.HttpServletResponse;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.testcontainers.shaded.org.apache.commons.lang3.RandomStringUtils;

@Tag("integration")
final class LocationKindControllerIT extends DataApiTestIT {
    @ParameterizedTest
    @ValueSource(strings = {Formats.JSONV2, Formats.DEFAULT})
    void test_get_location_kinds(String format) throws Exception {
        TestAccounts.KeyUser user = TestAccounts.KeyUser.SPK_NORMAL;

        String officeId = "SPK";
        String randomName = RandomStringUtils.randomAlphabetic(20);

        // create location
        createLocation(randomName, true, officeId, "SITE");

        // get all locations
        given()
            .log().ifValidationFails(LogDetail.ALL,true)
            .accept(format)
            .contentType(Formats.JSONV2)
            .header("Authorization", user.toHeaderValue())
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .get("/locations/with-kinds")
        .then()
            .log().ifValidationFails(LogDetail.ALL,true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_OK));

        // get specified location
        given()
            .log().ifValidationFails(LogDetail.ALL,true)
            .accept(format)
            .contentType(Formats.JSONV2)
            .header("Authorization", user.toHeaderValue())
            .queryParam(NAMES, randomName)
            .queryParam(OFFICE, officeId)
            .queryParam(LOCATION_KIND_LIKE, "SITE")
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .get("/locations/with-kinds")
        .then()
            .log().ifValidationFails(LogDetail.ALL,true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_OK))
            .body("size()", is(1))
            .body("[0].location-id.name", equalTo(randomName))
            .body("[0].location-id.office-id", equalTo(officeId))
            .body("[0].location-kind-id", equalTo("SITE"));
    }

    @ParameterizedTest
    @ValueSource(strings = {Formats.JSONV2, Formats.DEFAULT})
    void testLocationKindLike(String format) throws Exception {
        String locationName = RandomStringUtils.randomAlphabetic(20);
        String officeId = "SPK";
        createLocation(locationName, true, officeId);
        Basin basin = new Basin.Builder().withBasinId(CwmsId.buildCwmsId(officeId, locationName))
                                         .withAreaUnit("m2")
                                         .withContributingDrainageArea(2500.0)
                                         .withTotalDrainageArea(3500.0)
                                         .withSortOrder(1.0)
                                         .build();

        createBasin(basin);

        TestAccounts.KeyUser user = TestAccounts.KeyUser.SPK_NORMAL;

        given()
            .log().ifValidationFails(LogDetail.ALL,true)
            .accept(format)
            .contentType(Formats.JSONV2)
            .header("Authorization", user.toHeaderValue())
            .queryParam(LOCATION_KIND_LIKE, "BASIN")
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .get("/locations/with-kinds")
        .then()
            .log().ifValidationFails(LogDetail.ALL,true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_OK))
            .body("[0].location-kind-id", equalTo("BASIN"));
    }
}
