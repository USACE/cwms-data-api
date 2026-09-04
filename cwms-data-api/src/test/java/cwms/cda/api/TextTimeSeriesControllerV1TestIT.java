/*
 * MIT License
 *
 * Copyright (c) 2026 Hydrologic Engineering Center
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
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import cwms.cda.api.texttimeseries.TextTimeSeriesController;
import cwms.cda.formatters.Formats;
import fixtures.TestAccounts;
import io.restassured.filter.log.LogDetail;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import javax.servlet.http.HttpServletResponse;
import org.apache.commons.io.IOUtils;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

@Tag("integration")
final class TextTimeSeriesControllerV1TestIT extends TextTimeSeriesControllerTestIT {

    @ParameterizedTest
    @ValueSource(strings = {Formats.JSONV2, Formats.DEFAULT})
    void test_update_regular(String format) throws Exception {
        // The basic structure of the test is to:
        // 1)retrieve and verify
        // 2)update
        // 3)retrieve and verify
        String startStr = "2005-01-01T03:00:00Z";
        String endStr = "2005-01-01T07:00:00Z";

        given()
            .log().ifValidationFails(LogDetail.ALL,true)
            .accept(format)
            .queryParam(Controllers.OFFICE, OFFICE)
            .queryParam(Controllers.NAME, tsId)
            .queryParam(Controllers.BEGIN,startStr)
            .queryParam(Controllers.END,endStr)
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .get("/timeseries/text")
        .then()
            .log().ifValidationFails(LogDetail.ALL,true)
        .assertThat()
            .body("standard-text-catalog", nullValue())
            .body("standard-text-values", nullValue())
            .body("regular-text-values", notNullValue())
            .body("regular-text-values.size()", equalTo(5))
            .statusCode(is(HttpServletResponse.SC_OK));


        //2) update
        InputStream resource = this.getClass().getResourceAsStream("/cwms/cda/api/spk/text_ts_update_reg.json");
        assertNotNull(resource);
        String tsData = IOUtils.toString(resource, StandardCharsets.UTF_8);
        assertNotNull(tsData);
        TestAccounts.KeyUser user = TestAccounts.KeyUser.SPK_NORMAL;
        given()
            .log().ifValidationFails(LogDetail.ALL,true)
            .accept(format)
            .queryParam(Controllers.OFFICE, OFFICE)
            .queryParam(TextTimeSeriesController.REPLACE_ALL, "true")
            .contentType(Formats.JSONV2)
            .body(tsData)
            .header(AUTHORIZATION, user.toHeaderValue())
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .patch("/timeseries/text/" + tsId)
        .then()
            .log().ifValidationFails(LogDetail.ALL,true)
            .assertThat()
            .statusCode(is(HttpServletResponse.SC_OK));

        //3)retrieve and verify
        given()
            .log().ifValidationFails(LogDetail.ALL,true)
            .accept(format)
            .queryParam(Controllers.OFFICE, OFFICE)
            .queryParam(Controllers.NAME, tsId)
            .queryParam(Controllers.BEGIN,startStr)
            .queryParam(Controllers.END,endStr)
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .get("/timeseries/text")
        .then()
            .log().ifValidationFails(LogDetail.ALL,true)
            .assertThat()
            .body("standard-text-catalog", nullValue())
            .body("standard-text-values", nullValue())
            .body("regular-text-values", notNullValue())
            .body("regular-text-values.size()", equalTo(5))
            .body("regular-text-values[0].text-value", equalTo("still great"))
            .statusCode(is(HttpServletResponse.SC_OK));

    }
}
