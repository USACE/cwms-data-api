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

import static cwms.radar.api.Controllers.METHOD;
import static cwms.radar.api.Controllers.OFFICE;
import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

import cwms.radar.data.dao.JooqDao;
import cwms.radar.formatters.Formats;
import fixtures.RadarApiSetupCallback;
import fixtures.TestAccounts;
import hec.data.cwmsRating.io.RatingTemplateContainer;
import javax.servlet.http.HttpServletResponse;
import mil.army.usace.hec.cwms.rating.io.xml.RatingSpecXmlFactory;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@Tag("integration")
@ExtendWith(RadarApiSetupCallback.class)
class RatingTemplateControllerTestIT extends DataApiTestIT
{

	@Test
	void test_create_read_delete() throws Exception {
		String locationId = "RatingSpecTest";
		String officeId = RadarApiSetupCallback.getDatabaseLink().getOfficeId();
		createLocation(locationId, true, officeId);
		String ratingXml = readResourceFile("cwms/radar/api/Zanesville_Stage_Flow_COE_Production.xml");
		RatingTemplateContainer ratingTemplateContainer = RatingSpecXmlFactory.ratingTemplateContainer(ratingXml);
		ratingTemplateContainer.officeId = officeId;
		String templateXml = RatingSpecXmlFactory.toXml(ratingTemplateContainer, "", 0);
		TestAccounts.KeyUser user = TestAccounts.KeyUser.SPK_NORMAL;
		//Create Template
		given()
			.accept(Formats.JSONV2)
			.contentType(Formats.XMLV2)
			.body(templateXml)
			.header("Authorization", user.toHeaderValue())
			.queryParam(OFFICE, officeId)
			.when()
			.redirects().follow(true)
			.redirects().max(3)
			.post("/ratings/template")
			.then()
			.assertThat()
			.statusCode(is(HttpServletResponse.SC_CREATED));
		//Read
		given()
			.accept(Formats.JSONV2)
			.contentType(Formats.JSONV2)
			.header("Authorization", user.toHeaderValue())
			.queryParam("office", officeId)
			.when()
			.redirects().follow(true)
			.redirects().max(3)
			.get("/ratings/template/" + ratingTemplateContainer.templateId)
			.then()
			.assertThat()
			.log().body().log().everything(true)
			.statusCode(is(HttpServletResponse.SC_OK))
			.body("office-id", equalTo(ratingTemplateContainer.officeId))
			.body("id", equalTo(ratingTemplateContainer.templateId))
			.body("independent-parameter-specs[0].in-range-method", equalTo(ratingTemplateContainer.inRangeMethods[0]))
			.body("independent-parameter-specs[0].out-range-low-method", equalTo(ratingTemplateContainer.outRangeLowMethods[0]))
			.body("independent-parameter-specs[0].out-range-high-method", equalTo(ratingTemplateContainer.outRangeHighMethods[0]));
		//Delete
		given()
			.accept(Formats.JSONV2)
			.contentType(Formats.JSONV2)
			.header("Authorization", user.toHeaderValue())
			.queryParam(OFFICE, officeId)
			.queryParam(METHOD, JooqDao.DeleteMethod.DELETE_ALL)
			.when()
			.redirects().follow(true)
			.redirects().max(3)
			.delete("/ratings/template/" + ratingTemplateContainer.templateId)
			.then()
			.assertThat()
			.log().body().log().everything(true)
			.statusCode(is(HttpServletResponse.SC_NO_CONTENT));

		//Read Empty
		given()
			.accept(Formats.JSONV2)
			.contentType(Formats.JSONV2)
			.header("Authorization", user.toHeaderValue())
			.queryParam("office", officeId)
			.when()
			.redirects().follow(true)
			.redirects().max(3)
			.get("/ratings/template/" + ratingTemplateContainer.templateId)
			.then()
			.assertThat()
			.log().body().log().everything(true)
			.statusCode(is(HttpServletResponse.SC_NOT_FOUND));
	}


}