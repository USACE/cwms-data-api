package cwms.cda.api;

import cwms.cda.formatters.Formats;
import io.restassured.filter.log.LogDetail;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import javax.servlet.http.HttpServletResponse;

import java.net.URLEncoder;

import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.is;

@Tag("integration")
@Disabled("No data available for test or the test isn't currently loading any data.")
class BasinControllerTestIT extends DataApiTestIT
{
	private static final String SPK = "SPK";

	@EnumSource(AliasTest.class)
	@ParameterizedTest
	void test_getOne_aliases(AliasTest test) throws Exception
	{
		String requestId = "TEST";
		given()
			.log().ifValidationFails(LogDetail.ALL,true)
			.accept(test._accept)
			.queryParam(Controllers.OFFICE, SPK)
		.when()
			.redirects().follow(true)
			.redirects().max(3)
			.get("/basins/" + requestId)
		.then()
			.log().ifValidationFails(LogDetail.ALL,true)
			.assertThat()
			.contentType(is(test._expectedContentType));
	}

	@EnumSource(AliasTest.class)
	@ParameterizedTest
	void test_getAll_aliases(AliasTest test)
	{
		given()
			.log().ifValidationFails(LogDetail.ALL,true)
			.accept(test._accept)
			.queryParam(Controllers.OFFICE, SPK)
		.when()
			.redirects().follow(true)
			.redirects().max(3)
			.get("/basins/")
		.then()
			.log().ifValidationFails(LogDetail.ALL,true)
			.assertThat()
			.contentType(is(test._expectedContentType));
	}

	enum AliasTest
	{
		DEFAULT(Formats.DEFAULT, Formats.NAMED_PGJSON),
		JSON(Formats.JSON, Formats.NAMED_PGJSON),
		PGJSON(Formats.PGJSON, Formats.PGJSON),
		NAMED_PGJSON(Formats.NAMED_PGJSON, Formats.NAMED_PGJSON),
		;
		final String _accept;
		final String _expectedContentType;

		AliasTest(String accept, String expectedContentType)
		{
			_accept = accept;
			_expectedContentType = expectedContentType;
		}
	}
}