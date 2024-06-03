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

import cwms.cda.data.dto.Property;
import cwms.cda.formatters.ContentType;
import cwms.cda.formatters.Formats;
import fixtures.TestAccounts;
import io.restassured.filter.log.LogDetail;
import org.apache.commons.io.IOUtils;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.time.Instant;

import static cwms.cda.security.KeyAccessManager.AUTH_HEADER;
import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.assertNotNull;

@Tag("integration")
final class PropertyControllerIT extends DataApiTestIT {



    @Test
    void test_get_create_delete() throws IOException {
        InputStream resource = this.getClass().getResourceAsStream("/cwms/cda/api/property.json");
        assertNotNull(resource);
        String json = IOUtils.toString(resource, StandardCharsets.UTF_8);
        assertNotNull(json);
        Property property = Formats.parseContent(new ContentType(Formats.JSON), json, Property.class);

        // Structure of test:
        // 1)Create the Property
        // 2)Retrieve the Property and assert that it exists
        // 3)Delete the Property
        // 4)Retrieve the Property and assert that it does not exist
        TestAccounts.KeyUser user = TestAccounts.KeyUser.SPK_NORMAL;

        //Create the property
        given()
            .log().ifValidationFails(LogDetail.ALL, true)
            .accept(Formats.JSON)
            .contentType(Formats.JSON)
            .body(json)
            .header(AUTH_HEADER, user.toHeaderValue())
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .post("/properties/")
        .then()
            .log().ifValidationFails(LogDetail.ALL, true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_CREATED))
        ;
        String office = property.getOfficeId();
        // Retrieve the property and assert that it exists
        given()
            .log().ifValidationFails(LogDetail.ALL,true)
            .accept(Formats.JSON)
            .queryParam(Controllers.OFFICE, office)
            .queryParam(Controllers.CATEGORY_ID, property.getCategory())
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .get("properties/" + property.getName())
        .then()
            .log().ifValidationFails(LogDetail.ALL,true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_OK))
            .body("category", equalTo(property.getCategory()))
            .body("office-id", equalTo(office))
            .body("comment", equalTo(property.getComment()))
            .body("value", equalTo(property.getValue()))
            .body("name", equalTo(property.getName()))
        ;

        // Delete a Property
        given()
            .log().ifValidationFails(LogDetail.ALL,true)
            .accept(Formats.JSON)
            .queryParam(Controllers.OFFICE, office)
            .queryParam(Controllers.CATEGORY_ID, property.getCategory())
            .header(AUTH_HEADER, user.toHeaderValue())
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .delete("properties/" + property.getName())
        .then()
            .log().ifValidationFails(LogDetail.ALL,true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_NO_CONTENT))
        ;

        // Retrieve a Property and assert that it does not exist
        given()
            .log().ifValidationFails(LogDetail.ALL,true)
            .accept(Formats.JSON)
            .queryParam(Controllers.OFFICE, office)
            .queryParam(Controllers.CATEGORY_ID, property.getCategory())
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .get("properties/" + property.getName())
        .then()
            .log().ifValidationFails(LogDetail.ALL,true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_OK))
            .body("value", nullValue())
        ;
    }

    @Test
    void test_update_does_not_exist() throws IOException {
        InputStream resource = this.getClass().getResourceAsStream("/cwms/cda/api/property_bogus.json");
        assertNotNull(resource);
        String json = IOUtils.toString(resource, StandardCharsets.UTF_8);
        assertNotNull(json);
        Property property = Formats.parseContent(new ContentType(Formats.JSON), json, Property.class);
        TestAccounts.KeyUser user = TestAccounts.KeyUser.SPK_NORMAL;

        //Create the property
        given()
            .log().ifValidationFails(LogDetail.ALL, true)
            .accept(Formats.JSON)
            .contentType(Formats.JSON)
            .body(json)
            .header(AUTH_HEADER, user.toHeaderValue())
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .patch("/properties/" + property.getName())
        .then()
            .log().ifValidationFails(LogDetail.ALL, true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_NOT_FOUND))
        ;
    }

    @Test
    void test_delete_does_not_exist() throws IOException {
        TestAccounts.KeyUser user = TestAccounts.KeyUser.SPK_NORMAL;
        // Delete a Property
        given()
            .log().ifValidationFails(LogDetail.ALL,true)
            .accept(Formats.JSON)
            .queryParam(Controllers.OFFICE, user.getOperatingOffice())
            .queryParam(Controllers.CATEGORY_ID, Instant.now().toEpochMilli())
            .header(AUTH_HEADER, user.toHeaderValue())
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .delete("properties/" + Instant.now().toEpochMilli())
        .then()
            .log().ifValidationFails(LogDetail.ALL,true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_NOT_FOUND))
        ;
    }

    @Test
    void test_get_all() throws IOException {
        InputStream resource = this.getClass().getResourceAsStream("/cwms/cda/api/property.json");
        assertNotNull(resource);
        String json = IOUtils.toString(resource, StandardCharsets.UTF_8);
        assertNotNull(json);
        Property property = Formats.parseContent(new ContentType(Formats.JSON), json, Property.class);

        // Structure of test:
        // 1)Create the Property
        // 2)Retrieve the Property with getAll and assert that it exists
        // 3)Delete the Property
        TestAccounts.KeyUser user = TestAccounts.KeyUser.SPK_NORMAL;

        //Create the property
        given()
            .log().ifValidationFails(LogDetail.ALL, true)
            .accept(Formats.JSON)
            .contentType(Formats.JSON)
            .body(json)
            .header(AUTH_HEADER, user.toHeaderValue())
        .when()
            .redirects().follow(true)
            .redirects().max(3)
        .post("/properties/")
            .then()
            .log().ifValidationFails(LogDetail.ALL, true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_CREATED))
        ;
        String office = property.getOfficeId();
        // Retrieve the property and assert that it exists
        given()
            .log().ifValidationFails(LogDetail.ALL,true)
            .accept(Formats.JSON)
            .queryParam(Controllers.OFFICE_MASK, office)
            .queryParam(Controllers.CATEGORY_ID_MASK, property.getCategory())
            .queryParam(Controllers.NAME_MASK, property.getName())
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .get("properties/")
        .then()
            .log().ifValidationFails(LogDetail.ALL,true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_OK))
            .body("[0].category", equalTo(property.getCategory()))
            .body("[0].office-id", equalTo(office))
            .body("[0].comment", equalTo(property.getComment()))
            .body("[0].value", equalTo(property.getValue()))
            .body("[0].name", equalTo(property.getName()))
        ;

        // Delete a Property
        given()
            .log().ifValidationFails(LogDetail.ALL,true)
            .accept(Formats.JSON)
            .queryParam(Controllers.OFFICE, office)
            .queryParam(Controllers.CATEGORY_ID, property.getCategory())
            .header(AUTH_HEADER, user.toHeaderValue())
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .delete("properties/" + property.getName())
        .then()
            .log().ifValidationFails(LogDetail.ALL,true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_NO_CONTENT))
        ;
    }
}
