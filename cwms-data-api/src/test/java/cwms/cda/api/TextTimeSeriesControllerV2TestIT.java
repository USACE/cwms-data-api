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

import cwms.cda.formatters.Formats;
import fixtures.TestAccounts;
import io.restassured.filter.log.LogDetail;
import io.restassured.response.Response;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.io.IOUtils;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag("integration")
final class TextTimeSeriesControllerV2TestIT extends TextTimeSeriesControllerTestIT {

    @Override
    protected @NotNull String getPath() {
        return "v2/timeseries/text/" + OFFICE;
    }

    @Test
    void test_update_regular_partial_patch_overwrite() throws Exception {
        // the request body names
        // only the essential identifying element of the row being changed (date-time) and the
        // field actually being changed (text-value) -- no office-id, no name, no other row
        // field.
        //
        // Structure of the test is:
        // 1) retrieve and verify baseline state -- 5 rows, all sharing the same text-value
        //    (see store_reg_text_timeseries.sql)
        // 2) PATCH one row's text-value via the v2 endpoint with a minimal, partial body and
        //    collection-merge-strategy=overwrite (the default; passed explicitly here for
        //    clarity)
        // 3) retrieve and verify: per ADR-0017, OVERWRITE means the collection
        //    becomes exactly what the body named, within the begin/end window -- so the 4 rows
        //    that were in the window but weren't named in the body are removed, leaving only the
        //    one row the body actually patched. (Contrast with
        //    test_update_regular_partial_patch_merge below, which patches the same row
        //    without disturbing the other 4.)
        String startStr = "2005-01-01T03:00:00Z";
        String endStr = "2005-01-01T07:00:00Z";

        given()
            .log().ifValidationFails(LogDetail.ALL, true)
            .accept(Formats.JSON)
            .queryParam(Controllers.OFFICE, OFFICE)
            .queryParam(Controllers.NAME, tsId)
            .queryParam(Controllers.BEGIN, startStr)
            .queryParam(Controllers.END, endStr)
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .get(getPath())
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
            .accept(Formats.JSON)
            .queryParam(Controllers.BEGIN, startStr)
            .queryParam(Controllers.END, endStr)
            .queryParam(Controllers.COLLECTION_MERGE_STRATEGY, "overwrite")
            .contentType(Formats.JSON)
            .body(partialBody)
            .header(AUTHORIZATION, user.toHeaderValue())
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .patch(getPath() + "/" + tsId)
        .then()
            .log().ifValidationFails(LogDetail.ALL, true)
            .assertThat()
            .statusCode(is(HttpServletResponse.SC_OK));

        // 3) retrieve and verify: only the one named row remains -- the other 4, though within
        // the begin/end window, were removed because OVERWRITE means the window's collection
        // now consists of exactly what the body named.
        given()
            .log().ifValidationFails(LogDetail.ALL, true)
            .accept(Formats.JSON)
            .queryParam(Controllers.OFFICE, OFFICE)
            .queryParam(Controllers.NAME, tsId)
            .queryParam(Controllers.BEGIN, startStr)
            .queryParam(Controllers.END, endStr)
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .get(getPath())
        .then()
            .log().ifValidationFails(LogDetail.ALL, true)
            .assertThat()
            .body("regular-text-values", notNullValue())
            .body("regular-text-values.size()", equalTo(1))
            .body("regular-text-values[0].text-value", equalTo("partially patched"))
            .statusCode(is(HttpServletResponse.SC_OK));
    }

    @Test
    void test_update_regular_partial_patch_merge() throws Exception {
        // Unlike the other two strategies' tests, this one can't reuse the static
        // text_ts_update_reg_partial.json fixture as-is: MERGE's identity for a text-timeseries
        // row is the pair (date-time, data-entry-date), not date-time alone (see Identifier
        // and ADR-0017's "MERGE's identity" row), and data-entry-date is assigned by the database
        // when the row is stored -- it can't be hard-coded into a fixture ahead of time. So this
        // test reads the target row's actual data-entry-date back from the initial GET and builds
        // the PATCH body around it, instead of loading the fixture file.
        //
        // collection-merge-strategy=merge instead of overwrite. Per ADR-0017, MERGE matches the
        // named row by that composite identity and updates just that row in place, leaving every
        // other existing row -- in or out of the window -- untouched. So unlike OVERWRITE, the
        // other 4 rows in the window survive.
        String startStr = "2005-01-01T03:00:00Z";
        String endStr = "2005-01-01T07:00:00Z";

        Response initial = given()
            .log().ifValidationFails(LogDetail.ALL, true)
            .accept(Formats.JSON)
            .queryParam(Controllers.OFFICE, OFFICE)
            .queryParam(Controllers.NAME, tsId)
            .queryParam(Controllers.BEGIN, startStr)
            .queryParam(Controllers.END, endStr)
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .get(getPath())
        .then()
            .log().ifValidationFails(LogDetail.ALL, true)
        .assertThat()
            .body("regular-text-values", notNullValue())
            .body("regular-text-values.size()", equalTo(5))
            .body("regular-text-values.text-value", everyItem(equalTo(EXPECTED_TEXT_VALUE)))
            .statusCode(is(HttpServletResponse.SC_OK))
            .extract().response();

        // Find the row at startStr and capture its server-assigned data-entry-date so the PATCH
        // body below can name this specific row's full identity, not just its date-time.
        long targetDateTimeMillis = Instant.parse(startStr).toEpochMilli();
        List<Map<String, Object>> initialRows = initial.jsonPath().getList("regular-text-values");
        Object targetDataEntryDate = initialRows.stream()
                .filter(row -> targetDateTimeMillis == ((Number) row.get("date-time")).longValue())
                .map(row -> row.get("data-entry-date"))
                .findFirst()
                .orElseThrow(() -> new AssertionError(
                        "Could not find a row at " + startStr + " in the initial GET response"));

        // 2) partial PATCH on v2 with collection-merge-strategy=merge. Names the target row's
        // full identity -- date-time and data-entry-date -- and its new text-value; no
        // office-id/name, no other row field.
        String partialBody = "{\"regular-text-values\":[{\"date-time\":\"" + targetDateTimeMillis
                + "\",\"data-entry-date\":\"" + targetDataEntryDate
                + "\",\"text-value\":\"partially patched\"}]}";

        TestAccounts.KeyUser user = TestAccounts.KeyUser.SPK_NORMAL;
        given()
            .log().ifValidationFails(LogDetail.ALL, true)
            .accept(Formats.JSON)
            .queryParam(Controllers.BEGIN, startStr)
            .queryParam(Controllers.END, endStr)
            .queryParam(Controllers.COLLECTION_MERGE_STRATEGY, "merge")
            .contentType(Formats.JSON)
            .body(partialBody)
            .header(AUTHORIZATION, user.toHeaderValue())
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .patch(getPath() + "/" + tsId)
        .then()
            .log().ifValidationFails(LogDetail.ALL, true)
            .assertThat()
            .statusCode(is(HttpServletResponse.SC_OK));

        // 3) retrieve and verify: only the targeted row changed, the other 4 rows kept the
        // text-value they already had, and the row count is unaffected.
        given()
            .log().ifValidationFails(LogDetail.ALL, true)
            .accept(Formats.JSON)
            .queryParam(Controllers.OFFICE, OFFICE)
            .queryParam(Controllers.NAME, tsId)
            .queryParam(Controllers.BEGIN, startStr)
            .queryParam(Controllers.END, endStr)
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .get(getPath())
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
