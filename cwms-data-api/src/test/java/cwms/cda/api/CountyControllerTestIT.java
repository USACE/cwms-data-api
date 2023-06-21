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

import cwms.cda.formatters.Formats;
import fixtures.CwmsDataApiSetupCallback;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import javax.servlet.http.HttpServletResponse;

import static io.restassured.RestAssured.given;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.Matchers.is;

@Tag("integration")
@ExtendWith(CwmsDataApiSetupCallback.class)
public class CountyControllerTestIT extends DataApiTestIT {


    @Test
    void test_county_catalog()  {
        given()
                .accept(Formats.JSONV2)
                .contentType(Formats.JSONV2)
                .when()
                .redirects().follow(true)
                .redirects().max(3)
                .get("/counties/")
                .then()
                .assertThat()
                .log().body()
                .log().ifError()
                .log().everything(true)
                .assertThat()
                .statusCode(is(HttpServletResponse.SC_OK))
                .body("[0].name", equalTo("Unknown County or County N/A"))
                .body("[0].county-id", equalTo("000"))
                .body("[0].state-initial", equalTo("00"));
    }
}
