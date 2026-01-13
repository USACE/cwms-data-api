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

package cwms.cda.api.openapi.validation;

import static cwms.cda.api.Controllers.OFFICE;
import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.is;

import com.atlassian.oai.validator.restassured.OpenApiValidationFilter;
import cwms.cda.api.DataApiTestIT;
import cwms.cda.formatters.Formats;
import fixtures.TestAccounts;
import io.restassured.filter.log.LogDetail;
import javax.servlet.http.HttpServletResponse;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag("integration")
final class OfficeValidationIT extends DataApiTestIT {
    private static final OpenApiValidationFilter validationFilter = getOpenApiValidationFilter();
    TestAccounts.KeyUser user = TestAccounts.KeyUser.SPK_NORMAL;

    @Disabled("Disabled until OpenAPI spec is fixed for this endpoint. Accept type null is not supported by the validator")
    @Test
    void testGetOffice() {
        given()
            .log().ifValidationFails(LogDetail.ALL,true)
            .accept(Formats.JSON)
            .queryParam(OFFICE, user.getOperatingOffice())
            .filter(validationFilter)
            .contentType(Formats.JSON)
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .get("/offices/SPK")
        .then()
            .log().ifValidationFails(LogDetail.ALL,true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_OK));
    }

    @Disabled("Disabled until OpenAPI spec is fixed for this endpoint. Type value not in supported spec enum")
    @Test
    void testGetAllOffices() {
        given()
            .log().ifValidationFails(LogDetail.ALL,true)
            .accept(Formats.JSON)
            .filter(validationFilter)
            .contentType(Formats.JSON)
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .get("/offices/")
        .then()
            .log().ifValidationFails(LogDetail.ALL,true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_OK));
    }
}
