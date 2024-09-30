/*
 * Copyright (c) 2024. Hydrologic Engineering Center (HEC).
 * United States Army Corps of Engineers
 * All Rights Reserved. HEC PROPRIETARY/CONFIDENTIAL.
 * Source may not be released without written approval from HEC
 */

package cwms.cda.api;

import cwms.cda.formatters.Formats;
import fixtures.TestAccounts;
import hec.data.cwmsRating.io.RatingSetContainer;
import hec.data.cwmsRating.io.RatingSpecContainer;
import io.restassured.filter.log.LogDetail;
import mil.army.usace.hec.cwms.rating.io.xml.RatingContainerXmlFactory;
import mil.army.usace.hec.cwms.rating.io.xml.RatingSetContainerXmlFactory;
import mil.army.usace.hec.cwms.rating.io.xml.RatingSpecXmlFactory;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import javax.servlet.http.HttpServletResponse;

import static cwms.cda.api.Controllers.*;
import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.is;

@Tag("integration")
class RatingsControllerTestIT extends DataApiTestIT
{
	private static final String EXISTING_LOC = "RatingsControllerTestIT";
	private static final String EXISTING_SPEC = EXISTING_LOC + ".Stage;Flow.COE.Production";
	private static final String SPK = "SPK";

	@BeforeAll
	static void beforeAll() throws Exception
	{
		//Make sure we always have something.
		createLocation(EXISTING_LOC, true, SPK);

		String ratingXml = readResourceFile("cwms/cda/api/Zanesville_Stage_Flow_COE_Production.xml");
		ratingXml = ratingXml.replaceAll("Zanesville", EXISTING_LOC);
		RatingSetContainer container = RatingSetContainerXmlFactory.ratingSetContainerFromXml(ratingXml);
		RatingSpecContainer specContainer = container.ratingSpecContainer;
		specContainer.officeId = SPK;
		specContainer.specOfficeId = SPK;
		specContainer.locationId = EXISTING_LOC;
		String specXml = RatingSpecXmlFactory.toXml(specContainer, "", 0, true);
		String templateXml = RatingSpecXmlFactory.toXml(specContainer, "", 0);
		String setXml = RatingContainerXmlFactory.toXml(container, "", 0, true, false);
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
			.queryParam(STORE_TEMPLATE, false)
		.when()
			.redirects().follow(true)
			.redirects().max(3)
			.post("/ratings")
		.then()
			.assertThat()
			.log().ifValidationFails(LogDetail.ALL,true)
			.statusCode(is(HttpServletResponse.SC_OK));
	}

	@ParameterizedTest
	@EnumSource(GetAllLegacyTest.class)
	void test_getAll_legacy(GetAllLegacyTest test)
	{
		given()
			.log().ifValidationFails(LogDetail.ALL,true)
			.queryParam(FORMAT, test._queryParam)
		.when()
			.redirects().follow(true)
			.redirects().max(3)
			.get("/ratings")
		.then()
			.assertThat()
			.log().ifValidationFails(LogDetail.ALL,true)
			.statusCode(is(HttpServletResponse.SC_OK))
			.contentType(is(test._expectedContentType));
	}

	@ParameterizedTest
	@EnumSource(GetAllTest.class)
	void test_getAll(GetAllTest test)
	{
		given()
			.log().ifValidationFails(LogDetail.ALL,true)
			.accept(test._accept)
		.when()
			.redirects().follow(true)
			.redirects().max(3)
			.get("/ratings")
		.then()
			.assertThat()
			.log().ifValidationFails(LogDetail.ALL,true)
			.statusCode(is(HttpServletResponse.SC_OK))
			.contentType(is(test._expectedContentType));
	}

	@ParameterizedTest
	@EnumSource(GetOneTest.class)
	void test_getOne(GetOneTest test)
	{
		given()
			.log().ifValidationFails(LogDetail.ALL,true)
			.accept(test._accept)
			.queryParam(OFFICE, SPK)
		.when()
			.redirects().follow(true)
			.redirects().max(3)
			.get("/ratings/" + EXISTING_SPEC)
		.then()
			.assertThat()
			.log().ifValidationFails(LogDetail.ALL,true)
			.statusCode(is(HttpServletResponse.SC_OK))
			.contentType(is(test._expectedContentType));
	}

	@ParameterizedTest
	@EnumSource(GetOneTest.class)
	void test_get_one_match_date(GetOneTest test) {
		given()
			.log().ifValidationFails(LogDetail.ALL,true)
			.accept(test._accept)
			.queryParam(OFFICE, SPK)
			.queryParam(DATE, "2021-01-01T00:00:00Z")
			.queryParam("enable", true)
		.when()
			.redirects().follow(true)
			.redirects().max(3)
			.get("/ratings/" + EXISTING_SPEC)
		.then()
		.assertThat()
			.log().ifValidationFails(LogDetail.ALL,true)
			.statusCode(is(HttpServletResponse.SC_OK))
			.contentType(is(test._expectedContentType));

		given()
			.log().ifValidationFails(LogDetail.ALL,true)
			.accept(test._accept)
			.queryParam(OFFICE, SPK)
			.queryParam("enable", true)
		.when()
			.redirects().follow(true)
			.redirects().max(3)
			.get("/ratings/" + EXISTING_SPEC)
		.then()
		.assertThat()
			.log().ifValidationFails(LogDetail.ALL,true)
			.statusCode(is(HttpServletResponse.SC_OK))
			.contentType(is(test._expectedContentType));
	}

	enum GetOneTest
	{
		DEFAULT(Formats.DEFAULT, Formats.XMLV2),
		XML(Formats.XML, Formats.XMLV2),
		XMLV2(Formats.XMLV2, Formats.XMLV2),
		JSON(Formats.JSON, Formats.JSONV2),
		JSONV2(Formats.JSONV2, Formats.JSONV2),
		;

		final String _accept;
		final String _expectedContentType;

		GetOneTest(String accept, String expectedContentType)
		{
			_accept = accept;
			_expectedContentType = expectedContentType;
		}
	}

	enum GetAllLegacyTest
	{
		JSON(Formats.JSON_LEGACY, Formats.JSON),
		XML(Formats.XML_LEGACY, Formats.XML),
		;

		final String _queryParam;
		final String _expectedContentType;

		GetAllLegacyTest(String queryParam, String expectedContentType)
		{
			_queryParam = queryParam;
			_expectedContentType = expectedContentType;
		}
	}

	enum GetAllTest
	{
		DEFAULT(Formats.DEFAULT, Formats.XMLV2),
		XML(Formats.XML, Formats.XMLV2),
		XMLV1(Formats.XMLV1, Formats.XMLV1),
		XMLV2(Formats.XMLV2, Formats.XMLV2),
		JSON(Formats.JSON, Formats.JSONV2),
		JSONV1(Formats.JSONV1, Formats.JSONV1),
		JSONV2(Formats.JSONV2, Formats.JSONV2),
		;

		final String _accept;
		final String _expectedContentType;

		GetAllTest(String accept, String expectedContentType)
		{
			_accept = accept;
			_expectedContentType = expectedContentType;
		}
	}
}
