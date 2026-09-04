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
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.everyItem;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
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
import org.junit.jupiter.api.Test;

@Tag("integration")
final class TextTimeSeriesControllerV2TestIT extends TextTimeSeriesControllerTestIT {

    @Test
    void test_update_regular_partial_patch() throws Exception {
        // the request body names
        // only the essential identifying element of the row being changed (date-time) and the
        // field actually being changed (text-value) -- no office-id, no name, no other row
        // field -- and everything else already in the database (the other rows, and the other
        // fields of the timeseries) is left exactly as it was.
        //
        // Structure of the test is:
        // 1) retrieve and verify baseline state -- 5 rows, all sharing the same text-value
        //    (see store_reg_text_timeseries.sql)
        // 2) PATCH one row's text-value via the v2 endpoint with a minimal, partial body
        // 3) retrieve and verify only the targeted row changed; the other 4 keep the text-value
        //    they already had, and the row count is unaffected
        String startStr = "2005-01-01T03:00:00Z";
        String endStr = "2005-01-01T07:00:00Z";

        given()
            .log().ifValidationFails(LogDetail.ALL, true)
            .accept(Formats.JSONV2)
            .queryParam(Controllers.OFFICE, OFFICE)
            .queryParam(Controllers.NAME, tsId)
            .queryParam(Controllers.BEGIN, startStr)
            .queryParam(Controllers.END, endStr)
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .get("/timeseries/text")
        .then()
            .log().ifValidationFails(LogDetail.ALL, true)
        .assertThat()
            .body("regular-text-values", notNullValue())
            .body("regular-text-values.size()", equalTo(5))
            .body("regular-text-values.text-value", everyItem(equalTo(EXPECTED_TEXT_VALUE)))
            .statusCode(is(HttpServletResponse.SC_OK));

        // 2) partial PATCH on v2 -- body has only the row's identifier (date-time) and the new
        // text-value; no office-id/name, no other row field. office-id/name aren't needed here:
        // unlike the body, the resource itself is identified by the office/name path segments,
        // so nothing required is missing -- date-time is the only identifier Jackson actually
        // requires (see RegularTextTimeSeriesRow).
        InputStream resource = this.getClass()
                .getResourceAsStream("/cwms/cda/api/spk/text_ts_update_reg_partial.json");
        assertNotNull(resource);
        String partialBody = IOUtils.toString(resource, StandardCharsets.UTF_8);
        assertNotNull(partialBody);

        TestAccounts.KeyUser user = TestAccounts.KeyUser.SPK_NORMAL;
        given()
            .log().ifValidationFails(LogDetail.ALL, true)
            .accept(Formats.JSONV2)
            .queryParam(Controllers.BEGIN, startStr)
            .queryParam(Controllers.END, endStr)
            .queryParam(TextTimeSeriesController.REPLACE_ALL, "true")
            .contentType(Formats.JSONV2)
            .body(partialBody)
            .header(AUTHORIZATION, user.toHeaderValue())
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .patch("/v2/timeseries/text/" + OFFICE + "/" + tsId)
        .then()
            .log().ifValidationFails(LogDetail.ALL, true)
            .assertThat()
            .statusCode(is(HttpServletResponse.SC_OK));

        // 3) retrieve and verify: only the targeted row changed, the other 4 rows kept the
        // text-value they already had, and the row count is unaffected.
        given()
            .log().ifValidationFails(LogDetail.ALL, true)
            .accept(Formats.JSONV2)
            .queryParam(Controllers.OFFICE, OFFICE)
            .queryParam(Controllers.NAME, tsId)
            .queryParam(Controllers.BEGIN, startStr)
            .queryParam(Controllers.END, endStr)
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .get("/timeseries/text")
        .then()
            .log().ifValidationFails(LogDetail.ALL, true)
            .assertThat()
            .body("regular-text-values", notNullValue())
            .body("regular-text-values.size()", equalTo(5))
            .body("regular-text-values.text-value", containsInAnyOrder(
                    "partially patched", EXPECTED_TEXT_VALUE, EXPECTED_TEXT_VALUE,
                    EXPECTED_TEXT_VALUE, EXPECTED_TEXT_VALUE))
            .statusCode(is(HttpServletResponse.SC_OK));
    }
}
