/*
 * MIT License
 *
 * Copyright (c) 2025 Hydrologic Engineering Center
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

package cwms.cda.api.rating;

import cwms.cda.api.DataApiTestIT;
import cwms.cda.data.dao.JooqDao;
import cwms.cda.data.dao.VerticalDatum;
import cwms.cda.formatters.Formats;
import fixtures.TestAccounts;
import hec.data.cwmsRating.io.RatingSetContainer;
import hec.data.cwmsRating.io.RatingSpecContainer;
import io.restassured.filter.log.LogDetail;
import io.restassured.response.ExtractableResponse;
import io.restassured.response.Response;
import mil.army.usace.hec.cwms.rating.io.xml.RatingContainerXmlFactory;
import mil.army.usace.hec.cwms.rating.io.xml.RatingSetContainerXmlFactory;
import mil.army.usace.hec.cwms.rating.io.xml.RatingSpecXmlFactory;
import org.junit.jupiter.api.*;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import javax.servlet.http.HttpServletResponse;

import java.io.IOException;
import java.util.Arrays;

import static cwms.cda.api.Controllers.*;
import static io.restassured.RestAssured.given;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

@Tag("integration")
class RatingsControllerTestIT extends DataApiTestIT
{
	static final String EXISTING_LOC = "RatingsControllerTestIT";
    static final String EXISTING_LOC_LONG = "RatingsControlTestLong";
	private static final String EXISTING_SPEC = EXISTING_LOC + ".Stage;Flow.COE.Production";
    private static final String EXISTING_SPEC_LONG = EXISTING_LOC_LONG + ".Stage;Flow.COE.Production";
	private static final String TEMPLATE = "Stage;Flow.COE";
	static final String SPK = "SPK";

	@BeforeAll
	static void beforeAll() throws Exception {
		store(false);
        storeLong();
	}

	@AfterAll
	static void cleanUp()
	{
		TestAccounts.KeyUser user = TestAccounts.KeyUser.SPK_NORMAL;

		// Delete Template
		given()
				.log().ifValidationFails(LogDetail.ALL,true)
				.contentType(Formats.XMLV2)
				.header("Authorization", user.toHeaderValue())
				.queryParam(OFFICE, SPK)
				.queryParam(METHOD, JooqDao.DeleteMethod.DELETE_ALL)
		.when()
				.redirects().follow(true)
				.redirects().max(3)
				.delete("/ratings/template/" + TEMPLATE)
		.then()
				.log().ifValidationFails(LogDetail.ALL,true)
		.assertThat()
				.statusCode(is(HttpServletResponse.SC_NO_CONTENT));
	}

	static void store(boolean storeTemplate) throws Exception
	{
		//Make sure we always have something.
		createLocationWithVerticalDatum(EXISTING_LOC, true, SPK, VerticalDatum.NAVD88);

		String ratingXml = readResourceFile("cwms/cda/api/Zanesville_Stage_Flow_COE_Production.xml");
		ratingXml = ratingXml.replaceAll("Zanesville", EXISTING_LOC);
		String ratingXml2 = ratingXml.replaceAll("2002-04-09T13:53:01Z", "2016-06-06T00:00:00Z");
		String ratingXml3 = ratingXml.replaceAll("2002-04-09T13:53:01Z", "2085-06-06T00:00:00Z");
		RatingSetContainer container = RatingSetContainerXmlFactory.ratingSetContainerFromXml(ratingXml);
		RatingSetContainer container2 = RatingSetContainerXmlFactory.ratingSetContainerFromXml(ratingXml2);
		RatingSetContainer container3 = RatingSetContainerXmlFactory.ratingSetContainerFromXml(ratingXml3);
		RatingSpecContainer specContainer = container.ratingSpecContainer;
		specContainer.officeId = SPK;
		specContainer.specOfficeId = SPK;
		specContainer.locationId = EXISTING_LOC;
		String specXml = RatingSpecXmlFactory.toXml(specContainer, "", 0, true);
		String templateXml = RatingSpecXmlFactory.toXml(specContainer, "", 0);
		String setXml = RatingContainerXmlFactory.toXml(container, "", 0, true, false);
		String setXml2 = RatingContainerXmlFactory.toXml(container2, "", 0, true, false);
		String setXml3 = RatingContainerXmlFactory.toXml(container3, "", 0, true, false);
		TestAccounts.KeyUser user = TestAccounts.KeyUser.SPK_NORMAL;

		//Create Template
		given()
			.log().ifValidationFails(LogDetail.ALL,true)
			.contentType(Formats.XMLV2)
			.body(templateXml)
			.header("Authorization", user.toHeaderValue())
			.queryParam(OFFICE, SPK)
		.when()
			.redirects().follow(true)
			.redirects().max(3)
			.post("/ratings/template")
		.then()
			.assertThat()
			.log().ifValidationFails(LogDetail.ALL,true)
			.statusCode(is(HttpServletResponse.SC_CREATED));

		//Create Spec
		given()
				.log().ifValidationFails(LogDetail.ALL,true)
				.contentType(Formats.XMLV2)
				.body(specXml)
				.header("Authorization", user.toHeaderValue())
				.queryParam(OFFICE, SPK)
			.when()
				.redirects().follow(true)
				.redirects().max(3)
				.post("/ratings/spec")
			.then()
				.assertThat()
				.log().ifValidationFails(LogDetail.ALL,true)
				.statusCode(is(HttpServletResponse.SC_CREATED));

		//Create the set
		given()
			.log().ifValidationFails(LogDetail.ALL,true)
			.contentType(Formats.XMLV2)
			.body(setXml)
			.header("Authorization", user.toHeaderValue())
			.queryParam(OFFICE, SPK)
			.queryParam(STORE_TEMPLATE, storeTemplate)
		.when()
			.redirects().follow(true)
			.redirects().max(3)
			.post("/ratings")
		.then()
			.assertThat()
			.log().ifValidationFails(LogDetail.ALL,true)
			.statusCode(is(HttpServletResponse.SC_CREATED));

		//Create the second set
		given()
			.log().ifValidationFails(LogDetail.ALL,true)
			.contentType(Formats.XMLV2)
			.body(setXml2)
			.header("Authorization", user.toHeaderValue())
			.queryParam(OFFICE, SPK)
			.queryParam(STORE_TEMPLATE, storeTemplate)
		.when()
			.redirects().follow(true)
			.redirects().max(3)
			.post("/ratings")
		.then()
			.log().ifValidationFails(LogDetail.ALL,true)
		.assertThat()
			.statusCode(is(HttpServletResponse.SC_CREATED));

		// Create the third set
		given()
			.log().ifValidationFails(LogDetail.ALL,true)
			.contentType(Formats.XMLV2)
			.body(setXml3)
			.header("Authorization", user.toHeaderValue())
			.queryParam(OFFICE, SPK)
			.queryParam(STORE_TEMPLATE, storeTemplate)
		.when()
			.redirects().follow(true)
			.redirects().max(3)
			.post("/ratings")
		.then()
			.log().ifValidationFails(LogDetail.ALL,true)
		.assertThat()
			.statusCode(is(HttpServletResponse.SC_CREATED));
	}

    static void storeLong() throws Exception
    {
        //Make sure we always have something.
        createLocationWithVerticalDatum(EXISTING_LOC_LONG, true, SPK, VerticalDatum.NAVD88);

        String ratingXml = readResourceFile("cwms/cda/api/long-rating.xml");
        ratingXml = ratingXml.replaceAll("Zanesville", EXISTING_LOC_LONG);
        String ratingXml2 = ratingXml.replaceAll("2002-04-09T13:53:01Z", "2016-06-06T00:00:00Z");
        String ratingXml3 = ratingXml.replaceAll("2002-04-09T13:53:01Z", "2085-06-06T00:00:00Z");
        RatingSetContainer container = RatingSetContainerXmlFactory.ratingSetContainerFromXml(ratingXml);
        RatingSetContainer container2 = RatingSetContainerXmlFactory.ratingSetContainerFromXml(ratingXml2);
        RatingSetContainer container3 = RatingSetContainerXmlFactory.ratingSetContainerFromXml(ratingXml3);
        RatingSpecContainer specContainer = container.ratingSpecContainer;
        specContainer.officeId = SPK;
        specContainer.specOfficeId = SPK;
        specContainer.locationId = EXISTING_LOC_LONG;
        String specXml = RatingSpecXmlFactory.toXml(specContainer, "", 0, true);
        String templateXml = RatingSpecXmlFactory.toXml(specContainer, "", 0);
        String setXml = RatingContainerXmlFactory.toXml(container, "", 0, true, false);
        String setXml2 = RatingContainerXmlFactory.toXml(container2, "", 0, true, false);
        String setXml3 = RatingContainerXmlFactory.toXml(container3, "", 0, true, false);
        TestAccounts.KeyUser user = TestAccounts.KeyUser.SPK_NORMAL;

        //Create Template
        given()
            .log().ifValidationFails(LogDetail.ALL,true)
            .contentType(Formats.XMLV2)
            .body(templateXml)
            .header("Authorization", user.toHeaderValue())
            .queryParam(OFFICE, SPK)
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .post("/ratings/template")
        .then()
            .log().ifValidationFails(LogDetail.ALL,true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_CREATED));

        //Create Spec
        given()
            .log().ifValidationFails(LogDetail.ALL,true)
            .contentType(Formats.XMLV2)
            .body(specXml)
            .header("Authorization", user.toHeaderValue())
            .queryParam(OFFICE, SPK)
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .post("/ratings/spec")
        .then()
            .log().ifValidationFails(LogDetail.ALL,true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_CREATED));

        //Create the set
        given()
            .log().ifValidationFails(LogDetail.ALL,true)
            .contentType(Formats.XMLV2)
            .body(setXml)
            .header("Authorization", user.toHeaderValue())
            .queryParam(OFFICE, SPK)
            .queryParam(STORE_TEMPLATE, false)
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .post("/ratings")
        .then()
            .log().ifValidationFails(LogDetail.ALL,true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_CREATED));

        //Create the second set
        given()
            .log().ifValidationFails(LogDetail.ALL,true)
            .contentType(Formats.XMLV2)
            .body(setXml2)
            .header("Authorization", user.toHeaderValue())
            .queryParam(OFFICE, SPK)
            .queryParam(STORE_TEMPLATE, false)
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .post("/ratings")
        .then()
            .log().ifValidationFails(LogDetail.ALL,true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_CREATED));

        // Create the third set
        given()
            .log().ifValidationFails(LogDetail.ALL,true)
            .contentType(Formats.XMLV2)
            .body(setXml3)
            .header("Authorization", user.toHeaderValue())
            .queryParam(OFFICE, SPK)
            .queryParam(STORE_TEMPLATE, false)
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .post("/ratings")
        .then()
            .log().ifValidationFails(LogDetail.ALL,true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_CREATED));
    }

	@ParameterizedTest
	@EnumSource(GetAllLegacyTest.class)
	void test_getAll_legacy(GetAllLegacyTest test) {
		given()
            .log().ifValidationFails(LogDetail.ALL,true)
            .queryParam(FORMAT, test.queryParam)
		.when()
            .redirects().follow(true)
            .redirects().max(3)
            .get("/ratings")
		.then()
		.assertThat()
            .log().ifValidationFails(LogDetail.ALL,true)
            .statusCode(is(HttpServletResponse.SC_BAD_REQUEST))
            .body("message", equalTo("Bad Request"))
            .body("source", equalTo("User Input"))
            .body("details.message", equalTo("Invalid format. V1 formats no longer supported: " + test.queryParam));
	}

	@ParameterizedTest
	@EnumSource(GetAllTest.class)
	void test_getAll(GetAllTest test) {
		given()
            .log().ifValidationFails(LogDetail.ALL,true)
            .accept(test.accept)
		.when()
            .redirects().follow(true)
            .redirects().max(3)
            .get("/ratings")
		.then()
		.assertThat()
            .log().ifValidationFails(LogDetail.ALL,true)
            .statusCode(is(HttpServletResponse.SC_OK))
            .contentType(is(test.expectedContentType));
	}

	@ParameterizedTest
	@EnumSource(GetOneTest.class)
	void test_getOne(GetOneTest test) {
		given()
				.log().ifValidationFails(LogDetail.ALL,true)
				.accept(test.accept)
				.queryParam(OFFICE, SPK)
		.when()
				.redirects().follow(true)
				.redirects().max(3)
				.get("/ratings/" + EXISTING_SPEC)
		.then()
		.assertThat()
				.log().ifValidationFails(LogDetail.ALL,true)
				.statusCode(is(HttpServletResponse.SC_OK))
				.contentType(is(test.expectedContentType));
	}

	@ParameterizedTest
	@EnumSource(GetAllTest.class)
	void test_getAll_NATIVE_units(GetAllTest test)
	{
		given()
			.log().ifValidationFails(LogDetail.ALL,true)
			.accept(test.accept)
			.queryParam(UNIT, "NATIVE")
		.when()
			.redirects().follow(true)
			.redirects().max(3)
			.get("/ratings")
		.then()
			.log().ifValidationFails(LogDetail.ALL,true)
		.assertThat()
			.statusCode(is(HttpServletResponse.SC_OK))
			.contentType(is(test.expectedContentType));
	}

	@Test
	void test_get_one_latest() {
		// get latest json
		ExtractableResponse<Response> response = given()
				.log().ifValidationFails(LogDetail.ALL,true)
				.contentType(Formats.JSONV2)
				.queryParam(OFFICE, SPK)
		.when()
				.redirects().follow(true)
				.redirects().max(3)
				.get("/ratings/" + EXISTING_SPEC + "/latest")
		.then()
		.assertThat()
				.log().ifValidationFails(LogDetail.ALL,true)
				.statusCode(is(HttpServletResponse.SC_OK))
				.contentType(is(Formats.JSONV2))
				.extract();

		String effectiveDate = response.path("ratings.simple-rating[0].effective-date");
		if (effectiveDate == null) {
			effectiveDate = response.path("simple-rating.effective-date");
		}
		assertNotNull(effectiveDate);
		assertEquals("2016-06-06T00:00:00Z", effectiveDate);

		// get latest xml
		response = given()
				.log().ifValidationFails(LogDetail.ALL,true)
				.contentType(Formats.XMLV2)
				.queryParam(OFFICE, SPK)
		.when()
				.redirects().follow(true)
				.redirects().max(3)
				.get("/ratings/" + EXISTING_SPEC + "/latest")
		.then()
				.log().ifValidationFails(LogDetail.ALL,true)
		.assertThat()
				.statusCode(is(HttpServletResponse.SC_OK))
				.contentType(is(Formats.XMLV2))
				.extract();

		effectiveDate = response.path("ratings.simple-rating[0].effective-date");
		if (effectiveDate == null) {
			effectiveDate = response.path("simple-rating.effective-date");
		}
		assertNotNull(effectiveDate);
		assertEquals("2016-06-06T00:00:00Z", effectiveDate);
	}

    @Test
    void get_one_with_content_length() {
        given()
            .log().ifValidationFails(LogDetail.ALL,true)
            .accept(Formats.JSON)
            .queryParam(OFFICE, SPK)
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .get("/ratings/" + EXISTING_SPEC_LONG)
        .then()
            .log().ifValidationFails(LogDetail.ALL,true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_OK))
            .header("Content-Length", is("19831"))
            .contentType(is(Formats.JSONV2));
    }

	enum GetOneTest
	{
		DEFAULT(Formats.DEFAULT, Formats.XMLV2),
		XMLV2(Formats.XMLV2, Formats.XMLV2),
		JSONV2(Formats.JSONV2, Formats.JSONV2),
		;

		final String accept;
		final String expectedContentType;

		GetOneTest(String accept, String expectedContentType)
		{
			this.accept = accept;
			this.expectedContentType = expectedContentType;
		}
	}

	enum GetAllLegacyTest
	{
		JSON(Formats.JSON_LEGACY, Formats.JSON),
		XML(Formats.XML_LEGACY, Formats.XML),
		;

		final String queryParam;
		final String expectedContentType;

		GetAllLegacyTest(String queryParam, String expectedContentType)
		{
			this.queryParam = queryParam;
			this.expectedContentType = expectedContentType;
		}
	}

	enum GetAllTest
	{
		DEFAULT(Formats.DEFAULT, Formats.XMLV2),
		XMLV2(Formats.XMLV2, Formats.XMLV2),
		JSONV2(Formats.JSONV2, Formats.JSONV2),
		;

		final String accept;
		final String expectedContentType;

		GetAllTest(String accept, String expectedContentType)
		{
			this.accept = accept;
			this.expectedContentType = expectedContentType;
		}
	}

    @Test
    void test_1206_rating_create_json() throws IOException {
        // example from 1206 but office changed to SPK
        String body = readResourceFile("cwms/cda/api/spk/ratings_ind.json");
        body = body.replaceAll("Barren", EXISTING_LOC);
        TestAccounts.KeyUser user = TestAccounts.KeyUser.SPK_NORMAL;

        // Create the set
        given()
                .log().ifValidationFails(LogDetail.ALL,true)
                .contentType(Formats.JSONV2)
                .body(body)
                .header("Authorization", user.toHeaderValue())
                .queryParam(OFFICE, SPK)
            .when()
                .redirects().follow(true)
                .redirects().max(3)
                .post("/ratings")
            .then()
            .assertThat()
                .log().ifValidationFails(LogDetail.ALL,true)
                .statusCode(is(HttpServletResponse.SC_CREATED));
    }

    @Test
    void test_1206_rating_create_xml() throws IOException {
        // example from 1206 but office changed to SPK and converted to xml
        String body = readResourceFile("cwms/cda/api/spk/ratings_ind.xml");
        body = body.replaceAll("Barren", EXISTING_LOC);
        TestAccounts.KeyUser user = TestAccounts.KeyUser.SPK_NORMAL;

        // Create the set
        given()
                .log().ifValidationFails(LogDetail.ALL,true)
                .contentType(Formats.XMLV2)
                .body(body)
                .header("Authorization", user.toHeaderValue())
                .queryParam(OFFICE, SPK)
            .when()
                .redirects().follow(true)
                .redirects().max(3)
                .post("/ratings")
            .then()
            .assertThat()
                .log().ifValidationFails(LogDetail.ALL,true)
                .statusCode(is(HttpServletResponse.SC_CREATED));
    }

    @Test
    void test_1758_rating_post() throws IOException {
        TestAccounts.KeyUser user = TestAccounts.KeyUser.SPK_NORMAL;
        String ratingId = "FSMI.Stage;Flow.EXSA.PRODUCTION";
        ratingId = ratingId.replaceAll("FSMI", EXISTING_LOC);
        String body = readResourceFile("cwms/cda/data/dto/rating/rating_post.json");
        body = body.replaceAll("FSMI", EXISTING_LOC);
        String template = readResourceFile("cwms/cda/api/spk/rating_template.json");

        //Create Template
        given()
                .log().ifValidationFails(LogDetail.ALL,true)
                .contentType(Formats.JSONV2)
                .body(template)
                .header("Authorization", user.toHeaderValue())
                .queryParam(OFFICE, SPK)
        .when()
                .redirects().follow(true)
                .redirects().max(3)
                .post("/ratings/template")
        .then()
                .log().ifValidationFails(LogDetail.ALL,true)
        .assertThat()
                .statusCode(is(HttpServletResponse.SC_CREATED));

        // Create the set
        given()
                .log().ifValidationFails(LogDetail.ALL,true)
                .contentType(Formats.JSONV2)
                .accept(Formats.JSONV2)
                .body(body)
                .header("Authorization", user.toHeaderValue())
                .queryParam(OFFICE, SPK)
        .when()
                .redirects().follow(true)
                .redirects().max(3)
                .post("/ratings")
        .then()
                .log().ifValidationFails(LogDetail.ALL,true)
        .assertThat()
                .statusCode(is(HttpServletResponse.SC_CREATED));

        given()
                .log().ifValidationFails(LogDetail.ALL,true)
                .accept(Formats.JSONV2)
                .queryParam(OFFICE, SPK)
        .when()
                .redirects().follow(true)
                .redirects().max(3)
                .get("/ratings/" + ratingId)
        .then()
                .log().ifValidationFails(LogDetail.ALL,true)
        .assertThat()
                .statusCode(is(HttpServletResponse.SC_OK))
                .contentType(is(Formats.JSONV2))
                .body("rating-spec.office-id", is(SPK))
                .body("simple-rating.office-id", is(SPK));
    }

    @Test
    void test_1758_rating_post_usgs_stream_rating() throws IOException {
        TestAccounts.KeyUser user = TestAccounts.KeyUser.SPK_NORMAL;
        String ratingId = "USGSTEST.Stage;Flow.USGS-EXSA.USGS-NWIS".replace("USGSTEST", EXISTING_LOC);
        // json as returned by GET /ratings
        String body = readResourceFile("cwms/cda/data/dto/rating/rating_post_usgs.json")
                .replace("USGSTEST", EXISTING_LOC);

        // Create the set
        given()
                .log().ifValidationFails(LogDetail.ALL,true)
                .contentType(Formats.JSONV2)
                .body(body)
                .header("Authorization", user.toHeaderValue())
                .queryParam(OFFICE, SPK)
        .when()
                .redirects().follow(true)
                .redirects().max(3)
                .post("/ratings")
        .then()
                .log().ifValidationFails(LogDetail.ALL,true)
        .assertThat()
                .statusCode(is(HttpServletResponse.SC_CREATED));

        given()
                .log().ifValidationFails(LogDetail.ALL,true)
                .accept(Formats.JSONV2)
                .queryParam(OFFICE, SPK)
        .when()
                .redirects().follow(true)
                .redirects().max(3)
                .get("/ratings/" + ratingId)
        .then()
                .log().ifValidationFails(LogDetail.ALL,true)
        .assertThat()
                .statusCode(is(HttpServletResponse.SC_OK))
                .contentType(is(Formats.JSONV2))
                .body("rating-spec.office-id", is(SPK))
                .body("rating-spec.source-agency", is("USGS"))
                .body("usgs-stream-rating.office-id", is(SPK))
                .body("usgs-stream-rating.rating-spec-id", is(ratingId))
                .body("usgs-stream-rating.units-id", is("ft;cfs"))
                .body("usgs-stream-rating.height-shifts.point.size()", is(2))
                .body("usgs-stream-rating.rating-points.point.size()", is(4));
    }

    @Test
    void test_1758_rating_post_virtual_rating() throws IOException {
        TestAccounts.KeyUser user = TestAccounts.KeyUser.SPK_NORMAL;
        String ratingId = "VIRTTEST.Stage;Flow.Virtual.Production".replace("VIRTTEST", EXISTING_LOC);
        // json as returned by GET /ratings, including the source ratings the virtual rating references
        String body = readResourceFile("cwms/cda/data/dto/rating/rating_post_virtual.json")
                .replace("VIRTTEST", EXISTING_LOC);

        // Create the set
        given()
                .log().ifValidationFails(LogDetail.ALL,true)
                .contentType(Formats.JSONV2)
                .body(body)
                .header("Authorization", user.toHeaderValue())
                .queryParam(OFFICE, SPK)
        .when()
                .redirects().follow(true)
                .redirects().max(3)
                .post("/ratings")
        .then()
                .log().ifValidationFails(LogDetail.ALL,true)
        .assertThat()
                .statusCode(is(HttpServletResponse.SC_CREATED));

        given()
                .log().ifValidationFails(LogDetail.ALL,true)
                .accept(Formats.JSONV2)
                .queryParam(OFFICE, SPK)
        .when()
                .redirects().follow(true)
                .redirects().max(3)
                .get("/ratings/" + ratingId)
        .then()
                .log().ifValidationFails(LogDetail.ALL,true)
        .assertThat()
                .statusCode(is(HttpServletResponse.SC_OK))
                .contentType(is(Formats.JSONV2))
                .body("virtual-rating.office-id", is(SPK))
                .body("virtual-rating.rating-spec-id", is(ratingId))
                .body("virtual-rating.connections", is("R2I1=R1D"))
                .body("virtual-rating.source-ratings.source-rating.size()", is(2));
    }

    @Test
    void test_1758_rating_post_transitional_rating() throws IOException {
        TestAccounts.KeyUser user = TestAccounts.KeyUser.SPK_NORMAL;
        String ratingId = "TRANSTEST.Stage;Flow.Transitional.Production".replace("TRANSTEST", EXISTING_LOC);
        // json as returned by GET /ratings, including the source ratings the transitional rating references
        String body = readResourceFile("cwms/cda/data/dto/rating/rating_post_transitional.json")
                .replace("TRANSTEST", EXISTING_LOC);

        // Create the set
        given()
                .log().ifValidationFails(LogDetail.ALL,true)
                .contentType(Formats.JSONV2)
                .body(body)
                .header("Authorization", user.toHeaderValue())
                .queryParam(OFFICE, SPK)
        .when()
                .redirects().follow(true)
                .redirects().max(3)
                .post("/ratings")
        .then()
                .log().ifValidationFails(LogDetail.ALL,true)
        .assertThat()
                .statusCode(is(HttpServletResponse.SC_CREATED));

        String retrieved = given()
                .log().ifValidationFails(LogDetail.ALL,true)
                .accept(Formats.JSONV2)
                .queryParam(OFFICE, SPK)
        .when()
                .redirects().follow(true)
                .redirects().max(3)
                .get("/ratings/" + ratingId)
        .then()
                .log().ifValidationFails(LogDetail.ALL,true)
        .assertThat()
                .statusCode(is(HttpServletResponse.SC_OK))
                .contentType(is(Formats.JSONV2))
                .body("transitional-rating.office-id", is(SPK))
                .body("transitional-rating.rating-spec-id", is(ratingId))
                .body("transitional-rating.units-id", is("ft;cfs"))
                // the database normalizes the condition from "I1 GT 20"
                .body("transitional-rating.select.case.when", is("I1 gt 20.0"))
                .body("transitional-rating.select.default", is("R1"))
                .body("transitional-rating.source-ratings.rating-spec-id.size()", is(2))
                .extract()
                .asString();

        // The source rating spec ids in that json keep the whitespace from the rating xml
        // (e.g. "\n    LOC.Stage;Flow.TransSrc.Low\n   "), make sure it can still be stored as is.
        given()
                .log().ifValidationFails(LogDetail.ALL,true)
                .contentType(Formats.JSONV2)
                .body(retrieved)
                .header("Authorization", user.toHeaderValue())
                .queryParam(OFFICE, SPK)
        .when()
                .redirects().follow(true)
                .redirects().max(3)
                .patch("/ratings/" + ratingId)
        .then()
                .log().ifValidationFails(LogDetail.ALL,true)
        .assertThat()
                .statusCode(is(HttpServletResponse.SC_OK));

        // The sources still resolve, so the whitespace wasn't stored with the ids
        given()
                .log().ifValidationFails(LogDetail.ALL,true)
                .accept(Formats.JSONV2)
                .queryParam(OFFICE, SPK)
        .when()
                .redirects().follow(true)
                .redirects().max(3)
                .get("/ratings/" + ratingId)
        .then()
                .log().ifValidationFails(LogDetail.ALL,true)
        .assertThat()
                .statusCode(is(HttpServletResponse.SC_OK))
                .body("transitional-rating.source-ratings.rating-spec-id.element-value.collect { it.trim() }",
                        is(Arrays.asList(EXISTING_LOC + ".Stage;Flow.TransSrc.Low",
                                EXISTING_LOC + ".Stage;Flow.TransSrc.High")));
    }

}

