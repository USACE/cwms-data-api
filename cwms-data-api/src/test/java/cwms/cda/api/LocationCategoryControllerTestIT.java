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

import static cwms.cda.api.Controllers.CASCADE_DELETE;
import static cwms.cda.api.Controllers.OFFICE;
import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

import fixtures.TestAccounts;
import io.restassured.filter.log.LogDetail;

import javax.servlet.http.HttpServletResponse;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import cwms.cda.data.dto.LocationCategory;
import cwms.cda.formatters.ContentType;
import cwms.cda.formatters.Formats;

@Tag("integration")
class LocationCategoryControllerTestIT extends DataApiTestIT {
	TestAccounts.KeyUser user = TestAccounts.KeyUser.SPK_NORMAL;
	TestAccounts.KeyUser user2 = TestAccounts.KeyUser.SWT_NORMAL;

    @Test
    void test_create_read_delete() {
        String officeId = user.getOperatingOffice();
        LocationCategory cat = new LocationCategory(officeId, LocationCategoryControllerTestIT.class.getSimpleName(),
            "IntegrationTesting");
        ContentType contentType = Formats.parseHeader(Formats.JSON, LocationCategory.class);
        String xml = Formats.format(contentType, cat);
        registerCategory(cat);
        //Create Category
        given()
			.log().ifValidationFails(LogDetail.ALL,true)
            .accept(Formats.JSON)
            .contentType(Formats.JSON)
            .body(xml)
            .header("Authorization", user.toHeaderValue())
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .post("/location/category")
        .then()
            .assertThat()
			.log().ifValidationFails(LogDetail.ALL,true)
            .statusCode(is(HttpServletResponse.SC_CREATED));
        //Read
        given()
			.log().ifValidationFails(LogDetail.ALL,true)
            .accept(Formats.JSON)
            .contentType(Formats.JSON)
            .queryParam(OFFICE, officeId)
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .get("/location/category/" + cat.getId())
        .then()
            .assertThat()
            .log().ifValidationFails(LogDetail.ALL,true)
            .statusCode(is(HttpServletResponse.SC_OK))
            .body("office-id", equalTo(cat.getOfficeId()))
            .body("id", equalTo(cat.getId()))
            .body("description", equalTo(cat.getDescription()));
        //Delete
        given()
			.log().ifValidationFails(LogDetail.ALL,true)
            .accept(Formats.JSON)
            .contentType(Formats.JSON)
            .header("Authorization", user.toHeaderValue())
            .queryParam(OFFICE, officeId)
            .queryParam(CASCADE_DELETE, "true")
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .delete("/location/category/" + cat.getId())
        .then()
            .assertThat()
            .log().ifValidationFails(LogDetail.ALL,true)
            .statusCode(is(HttpServletResponse.SC_NO_CONTENT));

        //Read Empty
        given()
			.log().ifValidationFails(LogDetail.ALL,true)
            .accept(Formats.JSON)
            .contentType(Formats.JSON)
            .queryParam("office", officeId)
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .get("/location/category/" + cat.getId())
        .then()
            .assertThat()
            .log().ifValidationFails(LogDetail.ALL,true)
            .statusCode(is(HttpServletResponse.SC_NOT_FOUND));
    }

	@Test
	void test_create_already_existing_CWMS_category() {
		String officeId = user.getOperatingOffice();
		LocationCategory cat = new LocationCategory(officeId, "Default", "Default");
		ContentType contentType = Formats.parseHeader(Formats.JSON, LocationCategory.class);
		String xml = Formats.format(contentType, cat);
		//Attempt to Create Category, should fail
		given()
			.log().ifValidationFails(LogDetail.ALL,true)
			.accept(Formats.JSON)
			.contentType(Formats.JSON)
			.body(xml)
			.header("Authorization", user.toHeaderValue())
		.when()
			.redirects().follow(true)
			.redirects().max(3)
			.post("/location/category")
		.then()
			.log().ifValidationFails(LogDetail.ALL,true)
		.assertThat()
			.statusCode(is(HttpServletResponse.SC_CONFLICT));
		//Read Empty
		given()
			.log().ifValidationFails(LogDetail.ALL,true)
			.accept(Formats.JSON)
			.contentType(Formats.JSON)
			.queryParam("office", officeId)
		.when()
			.redirects().follow(true)
			.redirects().max(3)
			.get("/location/category/" + cat.getId())
		.then()
			.log().ifValidationFails(LogDetail.ALL,true)
		.assertThat()
			.statusCode(is(HttpServletResponse.SC_NOT_FOUND));
	}

	@Test
	void test_create_read_delete_same_category_different_office() {
		String officeId = user.getOperatingOffice();
		String officeId2 = user2.getOperatingOffice();
		LocationCategory cat = new LocationCategory(officeId, "test_create_read_delete", "IntegrationTesting");
		LocationCategory cat2 = new LocationCategory(officeId2, "test_create_read_delete", "IntegrationTesting");
		ContentType contentType = Formats.parseHeader(Formats.JSON, LocationCategory.class);
		registerCategory(cat);
		registerCategory(cat2);
		//Create Category
		String xml = Formats.format(contentType, cat);
		given()
			.log().ifValidationFails(LogDetail.ALL,true)
			.accept(Formats.JSON)
			.contentType(Formats.JSON)
			.body(xml)
			.header("Authorization", user.toHeaderValue())
		.when()
			.redirects().follow(true)
			.redirects().max(3)
			.post("/location/category")
		.then()
			.log().ifValidationFails(LogDetail.ALL,true)
		.assertThat()
			.statusCode(is(HttpServletResponse.SC_CREATED));
		//Create second Category
		xml = Formats.format(contentType, cat2);
		given()
			.log().ifValidationFails(LogDetail.ALL,true)
			.accept(Formats.JSON)
			.contentType(Formats.JSON)
			.body(xml)
			.header("Authorization", user.toHeaderValue())
		.when()
			.redirects().follow(true)
			.redirects().max(3)
			.post("/location/category")
		.then()
			.log().ifValidationFails(LogDetail.ALL,true)
		.assertThat()
			.statusCode(is(HttpServletResponse.SC_CREATED));
		//Read
		given()
			.log().ifValidationFails(LogDetail.ALL,true)
			.accept(Formats.JSON)
			.contentType(Formats.JSON)
			.queryParam(OFFICE, officeId)
		.when()
			.redirects().follow(true)
			.redirects().max(3)
			.get("/location/category/" + cat.getId())
		.then()
			.log().ifValidationFails(LogDetail.ALL,true)
		.assertThat()
			.statusCode(is(HttpServletResponse.SC_OK))
			.body("office-id", equalTo(cat.getOfficeId()))
			.body("id", equalTo(cat.getId()))
			.body("description", equalTo(cat.getDescription()));
		// Read second category
		given()
			.log().ifValidationFails(LogDetail.ALL,true)
			.accept(Formats.JSON)
			.contentType(Formats.JSON)
			.queryParam(OFFICE, officeId2)
		.when()
			.redirects().follow(true)
			.redirects().max(3)
			.get("/location/category/" + cat2.getId())
		.then()
			.log().ifValidationFails(LogDetail.ALL,true)
		.assertThat()
			.statusCode(is(HttpServletResponse.SC_OK))
			.body("office-id", equalTo(cat2.getOfficeId()))
			.body("id", equalTo(cat2.getId()))
			.body("description", equalTo(cat2.getDescription()));
		//Delete
		given()
			.log().ifValidationFails(LogDetail.ALL,true)
			.accept(Formats.JSON)
			.contentType(Formats.JSON)
			.header("Authorization", user.toHeaderValue())
			.queryParam(OFFICE, officeId)
			.queryParam(CASCADE_DELETE, "true")
		.when()
			.redirects().follow(true)
			.redirects().max(3)
			.delete("/location/category/" + cat.getId())
		.then()
			.log().ifValidationFails(LogDetail.ALL,true)
		.assertThat()
			.statusCode(is(HttpServletResponse.SC_NO_CONTENT));
		//Delete seocond category
		given()
			.log().ifValidationFails(LogDetail.ALL,true)
			.accept(Formats.JSON)
			.contentType(Formats.JSON)
			.header("Authorization", user.toHeaderValue())
			.queryParam(OFFICE, officeId2)
			.queryParam(CASCADE_DELETE, "true")
		.when()
			.redirects().follow(true)
			.redirects().max(3)
			.delete("/location/category/" + cat2.getId())
		.then()
			.log().ifValidationFails(LogDetail.ALL,true)
		.assertThat()
			.statusCode(is(HttpServletResponse.SC_NO_CONTENT));
		//Read Empty
		given()
			.log().ifValidationFails(LogDetail.ALL,true)
			.accept(Formats.JSON)
			.contentType(Formats.JSON)
			.queryParam("office", officeId)
		.when()
			.redirects().follow(true)
			.redirects().max(3)
			.get("/location/category/" + cat.getId())
		.then()
			.log().ifValidationFails(LogDetail.ALL,true)
		.assertThat()
			.statusCode(is(HttpServletResponse.SC_NOT_FOUND));
		//Read second Empty
		given()
			.log().ifValidationFails(LogDetail.ALL,true)
			.accept(Formats.JSON)
			.contentType(Formats.JSON)
			.queryParam("office", officeId2)
		.when()
			.redirects().follow(true)
			.redirects().max(3)
			.get("/location/category/" + cat2.getId())
		.then()
			.log().ifValidationFails(LogDetail.ALL,true)
		.assertThat()
			.statusCode(is(HttpServletResponse.SC_NOT_FOUND));
	}
}
