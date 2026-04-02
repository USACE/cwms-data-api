/*
 * MIT License
 *
 * Copyright (c) 2025 Hydrologic Engineering Center
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

package cwms.cda.api.rating;

import static cwms.cda.api.Controllers.OFFICE;
import static cwms.cda.api.Controllers.PAGE;
import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

import cwms.cda.api.DataApiTestIT;
import cwms.cda.formatters.Formats;
import fixtures.TestAccounts;
import hec.data.RatingException;
import hec.data.cwmsRating.RatingSet;
import io.restassured.filter.log.LogDetail;
import javax.servlet.http.HttpServletResponse;
import mil.army.usace.hec.cwms.rating.io.jdbc.RatingJdbcFactory;
import mil.army.usace.hec.cwms.rating.io.xml.RatingXmlFactory;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import usace.cwms.db.jooq.codegen.packages.CWMS_ENV_PACKAGE;

@Tag("integration")
class RatingSpecControllerPagingIT extends DataApiTestIT {

    @BeforeAll
    static void beforeAll() throws Exception {
        String locationId = "RatingSpecGetAllPaged";
        String ratingXml = readResourceFile("cwms/cda/api/RatingSpecGetAllPaged_Stage_Flow_COE_Production.xml");
        String officeId = "SPK";
        createLocation(locationId, true, officeId);
        connectionAsWebUser(c ->{
            CWMS_ENV_PACKAGE.call_SET_SESSION_OFFICE_ID(dslContext(c).configuration(), "SPK");
            for(int i = 0; i < RatingSpecController.DEFAULT_PAGE_SIZE * 2; i++) {
                try {
                    String xml = ratingXml.replace("{location-id}", locationId)
                        .replace("{version-template}", "Test" + i);
                    RatingSet ratingSet = RatingXmlFactory.ratingSet(xml);
                    RatingJdbcFactory.store(ratingSet, c, true, true);
                } catch (RatingException e) {
                    throw new RuntimeException(e);
                }
            }
        });
    }

    @Test
    void test_getAll_paged() {
        var response = given()
            .log().ifValidationFails(LogDetail.ALL, true)
            .accept(Formats.JSONV2)
            .queryParam(OFFICE, "SPK")
            .queryParam("rating-id-mask", ".*")
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .get("/ratings/spec")
        .then()
            .assertThat()
            .log().ifValidationFails(LogDetail.ALL, true)
            .statusCode(is(HttpServletResponse.SC_OK))
            .body("page-size", equalTo(RatingSpecController.DEFAULT_PAGE_SIZE))
            .body("total", greaterThan(RatingSpecController.DEFAULT_PAGE_SIZE))
            .body("next-page", notNullValue())
            .extract();
        String nextPage = response.path("next-page");
        given()
            .log().ifValidationFails(LogDetail.ALL, true)
            .accept(Formats.JSONV2)
            .queryParam(OFFICE, "SPK")
            .queryParam(PAGE, nextPage)
            .queryParam("rating-id-mask", ".*")
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .get("/ratings/spec")
        .then()
            .assertThat()
            .log().ifValidationFails(LogDetail.ALL, true)
            .statusCode(is(HttpServletResponse.SC_OK));
    }
}
