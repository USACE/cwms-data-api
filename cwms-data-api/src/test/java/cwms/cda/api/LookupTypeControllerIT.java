/*
 * MIT License
 *
 * Copyright (c) 2024 Hydrologic Engineering Center
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

import cwms.cda.data.dto.LookupType;
import cwms.cda.formatters.ContentType;
import cwms.cda.formatters.Formats;
import fixtures.TestAccounts;
import static io.restassured.RestAssured.withArgs;
import io.restassured.filter.log.LogDetail;
import org.apache.commons.io.IOUtils;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;

import static cwms.cda.security.KeyAccessManager.AUTH_HEADER;
import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.assertNotNull;

@Tag("integration")
final class LookupTypeControllerIT extends DataApiTestIT {

    private static final String CATEGORY_IT = "AT_EMBANK_STRUCTURE_TYPE";
    private static final String PREFIX_IT = "STRUCTURE_TYPE";
    @Test
    void test_get_create_delete() throws IOException {
        InputStream resource = this.getClass().getResourceAsStream("/cwms/cda/api/lookup_type.json");
        assertNotNull(resource);
        String json = IOUtils.toString(resource, StandardCharsets.UTF_8);
        assertNotNull(json);
        LookupType lookupType = Formats.parseContent(new ContentType(Formats.JSON), json, LookupType.class);

        // Structure of test:
        // 1)Create the LookupType
        // 2)Retrieve the LookupType and assert that it exists
        // 3)Delete the LookupType
        // 4)Retrieve the LookupType and assert that it does not exist
        TestAccounts.KeyUser user = TestAccounts.KeyUser.SPK_NORMAL;

        //Create the lookup type
        given()
                .log().ifValidationFails(LogDetail.ALL, true)
                .accept(Formats.JSON)
                .queryParam(Controllers.CATEGORY, CATEGORY_IT)
                .queryParam(Controllers.PREFIX, PREFIX_IT)
                .contentType(Formats.JSON)
                .body(json)
                .header(AUTH_HEADER, user.toHeaderValue())
                .when()
                .redirects().follow(true)
                .redirects().max(3)
                .post("/lookup-types/")
                .then()
                .log().ifValidationFails(LogDetail.ALL, true)
                .assertThat()
                .statusCode(is(HttpServletResponse.SC_CREATED))
        ;
        String office = user.getOperatingOffice();
        // Retrieve the lookup type and assert that it exists
        given()
                .log().ifValidationFails(LogDetail.ALL,true)
                .accept(Formats.JSON)
                .queryParam(Controllers.OFFICE, office)
                .queryParam(Controllers.CATEGORY, CATEGORY_IT)
                .queryParam(Controllers.PREFIX, PREFIX_IT)
                .when()
                .redirects().follow(true)
                .redirects().max(3)
                .get("lookup-types/")
                .then()
                .log().ifValidationFails(LogDetail.ALL,true)
                .assertThat()
                .statusCode(is(HttpServletResponse.SC_OK))
                .body("find { it.'display-value' == '%s' }.office-id", withArgs(lookupType.getDisplayValue()), equalTo(office))
                .body("find { it.'display-value' == '%s' }.tooltip", withArgs(lookupType.getDisplayValue()), equalTo(lookupType.getTooltip()))
                .body("find { it.'display-value' == '%s' }.active", withArgs(lookupType.getDisplayValue()), equalTo(lookupType.getActive()))
                .body("find { it.'display-value' == '%s' }.display-value", withArgs(lookupType.getDisplayValue()), equalTo(lookupType.getDisplayValue()))
        ;

        // Delete the lookup type
        given()
                .log().ifValidationFails(LogDetail.ALL,true)
                .accept(Formats.JSON)
                .queryParam(Controllers.OFFICE, office)
                .queryParam(Controllers.CATEGORY, CATEGORY_IT)
                .queryParam(Controllers.PREFIX, PREFIX_IT)
                .header(AUTH_HEADER, user.toHeaderValue())
                .when()
                .redirects().follow(true)
                .redirects().max(3)
                .delete("lookup-types/" + lookupType.getDisplayValue())
                .then()
                .log().ifValidationFails(LogDetail.ALL,true)
                .assertThat()
                .statusCode(is(HttpServletResponse.SC_NO_CONTENT))
        ;

        // Retrieve the lookup type and assert that it does not exist
        given()
                .log().ifValidationFails(LogDetail.ALL,true)
                .accept(Formats.JSON)
                .queryParam(Controllers.OFFICE, office)
                .queryParam(Controllers.CATEGORY, CATEGORY_IT)
                .queryParam(Controllers.PREFIX, PREFIX_IT)
                .when()
                .redirects().follow(true)
                .redirects().max(3)
                .get("lookup-types/")
                .then()
                .log().ifValidationFails(LogDetail.ALL,true)
                .assertThat()
                .statusCode(is(HttpServletResponse.SC_OK))
                .body("find { it.'display-value' == '%s' }", withArgs(lookupType.getDisplayValue()), nullValue())
        ;
    }

    @Test
    void test_update_does_not_exist() throws IOException {
        InputStream resource = this.getClass().getResourceAsStream("/cwms/cda/api/lookup_type_does_not_exist.json");
        assertNotNull(resource);
        String json = IOUtils.toString(resource, StandardCharsets.UTF_8);
        assertNotNull(json);
        LookupType lookupType = Formats.parseContent(new ContentType(Formats.JSON), json, LookupType.class);
        TestAccounts.KeyUser user = TestAccounts.KeyUser.SPK_NORMAL;

        // Try to update a lookup type that does not exist
        given()
                .log().ifValidationFails(LogDetail.ALL, true)
                .accept(Formats.JSON)
                .queryParam(Controllers.CATEGORY, CATEGORY_IT)
                .queryParam(Controllers.PREFIX, PREFIX_IT)
                .contentType(Formats.JSON)
                .body(json)
                .header(AUTH_HEADER, user.toHeaderValue())
                .when()
                .redirects().follow(true)
                .redirects().max(3)
                .patch("/lookup-types/" + lookupType.getDisplayValue())
                .then()
                .log().ifValidationFails(LogDetail.ALL, true)
                .assertThat()
                .statusCode(is(HttpServletResponse.SC_NOT_FOUND))
        ;
    }

    @Test
    void test_delete_does_not_exist() throws IOException {
        TestAccounts.KeyUser user = TestAccounts.KeyUser.SPK_NORMAL;
        // Try to delete a lookup type that does not exist
        given()
                .log().ifValidationFails(LogDetail.ALL,true)
                .accept(Formats.JSON)
                .queryParam(Controllers.OFFICE, user.getOperatingOffice())
                .queryParam(Controllers.CATEGORY, CATEGORY_IT)
                .queryParam(Controllers.PREFIX, PREFIX_IT)
                .header(AUTH_HEADER, user.toHeaderValue())
                .when()
                .redirects().follow(true)
                .redirects().max(3)
                .delete("/lookup-types/" + "Non-ExistentLookupType3891029381fhsd")
                .then()
                .log().ifValidationFails(LogDetail.ALL,true)
                .assertThat()
                .statusCode(is(HttpServletResponse.SC_NOT_FOUND))
        ;
    }
}