package cwms.cda.api;

import cwms.cda.formatters.Formats;
import io.restassured.filter.log.LogDetail;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import javax.servlet.http.HttpServletResponse;

import static cwms.cda.api.Controllers.FORMAT;
import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.*;

@Tag("integration")
class UnitsControllerTestIT extends DataApiTestIT
{

	@ParameterizedTest
	@EnumSource(TimeZoneControllerTestIT.GetAllTest.class)
	void test_getAll(TimeZoneControllerTestIT.GetAllTest test)
	{
		given()
			.log().ifValidationFails(LogDetail.ALL,true)
			.accept(test._accept)
		.when()
			.redirects().follow(true)
			.redirects().max(3)
			.get("/units")
		.then()
			.assertThat()
			.log().ifValidationFails(LogDetail.ALL,true)
			.statusCode(is(HttpServletResponse.SC_OK))
			.contentType(is(test._expectedContentType));
	}

	@ParameterizedTest
	@EnumSource(TimeZoneControllerTestIT.GetAllLegacyTest.class)
	void test_getAll(TimeZoneControllerTestIT.GetAllLegacyTest test)
	{
		given()
			.log().ifValidationFails(LogDetail.ALL,true)
			.queryParam(FORMAT, test._format)
		.when()
			.redirects().follow(true)
			.redirects().max(3)
			.get("/units")
		.then()
			.assertThat()
			.log().ifValidationFails(LogDetail.ALL,true)
			.statusCode(is(HttpServletResponse.SC_OK))
			.contentType(is(test._expectedContentType));
	}

	enum GetAllLegacyTest
	{
		JSON(Formats.JSON_LEGACY, Formats.JSON),
		XML(Formats.XML_LEGACY, Formats.XML),
		TAB(Formats.TAB_LEGACY, Formats.TAB),
		CSV(Formats.CSV_LEGACY, Formats.CSV),
		;

		final String _format;
		final String _expectedContentType;

		GetAllLegacyTest(String format, String expectedContentType)
		{
			_format = format;
			_expectedContentType = expectedContentType;
		}
	}

	enum GetAllTest
	{
		DEFAULT(Formats.DEFAULT, Formats.JSONV2),
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