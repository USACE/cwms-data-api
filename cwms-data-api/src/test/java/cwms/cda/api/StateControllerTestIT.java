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

package cwms.cda.api;

import static io.restassured.RestAssured.given;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.isEmptyOrNullString;

import cwms.cda.formatters.Formats;
import io.javalin.core.util.Header;
import io.restassured.filter.log.LogDetail;
import javax.servlet.http.HttpServletResponse;

import io.restassured.response.ValidatableResponse;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag("integration")
public class StateControllerTestIT extends DataApiTestIT {

    @Test
    void test_state_catalog()  {
        catalogStates()
            .body("[0].name", equalTo("Unknown State or State N/A"))
            .body("[0].state-initial", equalTo("00"));
    }

    private ValidatableResponse catalogStates()
    {
        return catalogStatesWithContentType(Formats.JSONV2);
    }

    private ValidatableResponse catalogStatesWithContentType(String contentType)
    {
        return given()
                    .log().ifValidationFails(LogDetail.ALL, true)
                    .accept(Formats.JSONV2)
                .when()
                    .redirects().follow(true)
                    .redirects().max(3)
                    .get("/states/")
                .then()
                    .assertThat().log().ifValidationFails(LogDetail.ALL, true)
                    .statusCode(is(HttpServletResponse.SC_OK));
    }

    @Test
    void test_state_catalog_with_app_json()  {
        catalogStates()
            .body("[0].name", equalTo("Unknown State or State N/A"))
            .body("[0].state-initial", equalTo("00"));
    }

    @Test
    void test_state_has_ETag_and_Cache_Control()  {
        catalogStates()
            .header(Header.ETAG, not(isEmptyOrNullString()))
            .headers(Header.CACHE_CONTROL.toLowerCase(), containsString("max-age="));
    }

    @Test
    void test_state_catalog_default_content_type()
    {
        catalogStatesWithContentType(Formats.DEFAULT)
                .contentType(equalTo(Formats.JSONV2));
    }

    @Test
    void test_state_catalog_JSON_content_type()
    {
        catalogStatesWithContentType(Formats.JSON)
                .contentType(equalTo(Formats.JSONV2));
    }
}
