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

package cwms.radar.api;

import static cwms.radar.api.Controllers.CASCADE_DELETE;
import static cwms.radar.api.Controllers.OFFICE;
import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;

import cwms.radar.data.dto.LocationCategory;
import cwms.radar.data.dto.LocationGroup;
import cwms.radar.formatters.ContentType;
import cwms.radar.formatters.Formats;
import fixtures.CwmsDataApiSetupCallback;
import fixtures.TestAccounts;
import mil.army.usace.hec.test.database.CwmsDatabaseContainer;

import javax.servlet.http.HttpServletResponse;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@Tag("integration")
@Disabled // tests failing for reasons other than office id switcharound.
class LocationCategoryControllerTestIT extends DataApiTestIT {
	@Test
	void test_create_read_delete() throws Exception {
		String officeId = "SPK";
		TestAccounts.KeyUser user = TestAccounts.KeyUser.SPK_NORMAL;
		LocationCategory cat = new LocationCategory(officeId, LocationCategoryControllerTestIT.class.getSimpleName(), "IntegrationTesting");
		ContentType contentType = Formats.parseHeaderAndQueryParm(Formats.JSON, null);
		String xml = Formats.format(contentType, cat);
		registerCategory(cat);;
		//Create Category
		given()
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
			.statusCode(is(HttpServletResponse.SC_CREATED));
		//Read
		given()
			.accept(Formats.JSON)
			.contentType(Formats.JSON)
			.queryParam(OFFICE, officeId)
			.when()
			.redirects().follow(true)
			.redirects().max(3)
			.get("/location/category/" + cat.getId())
			.then()
			.assertThat()
			.log().body().log().everything(true)
			.statusCode(is(HttpServletResponse.SC_OK))
			.body("office-id", equalTo(cat.getOfficeId()))
			.body("id", equalTo(cat.getId()))
			.body("description", equalTo(cat.getDescription()));
		//Delete
		given()
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
			.log().body().log().everything(true)
			.statusCode(is(HttpServletResponse.SC_NO_CONTENT));

		//Read Empty
		given()
			.accept(Formats.JSON)
			.contentType(Formats.JSON)
			.queryParam("office", officeId)
			.when()
			.redirects().follow(true)
			.redirects().max(3)
			.get("/location/category/" + cat.getId())
			.then()
			.assertThat()
			.log().body().log().everything(true)
			.statusCode(is(HttpServletResponse.SC_NOT_FOUND));
	}


}