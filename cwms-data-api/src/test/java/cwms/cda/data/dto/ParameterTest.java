package cwms.cda.data.dto;

import cwms.cda.formatters.ContentType;
import cwms.cda.formatters.Formats;
import org.apache.commons.io.IOUtils;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

class ParameterTest
{
	private static final String SPK = "SPK";

	static Parameter newTestParameter()
	{
		String param = "%-ofArea-Snow";
		String baseParam = "%";
		String subParam = "ofArea-Snow";
		String subParamDesc = "Percent of Area Covered by Snow";
		String officeId = "CWMS";
		String unitId = "%";
		String unitLongName = "Percent";
		String unitDesc = "Ratio of 1E-02";

		return new Parameter(param, baseParam, subParam, subParamDesc, officeId, unitId, unitLongName, unitDesc);
	}

	@ParameterizedTest
	@EnumSource(ParametersTest.SerializationType.class)
	void test_serialization(ParametersTest.SerializationType test)
	{
		Parameter param = ParameterTest.newTestParameter();
		String json = Formats.format(test._contentType, param);
		Parameter parsedParam = Formats.parseContent(test._contentType, json, Parameter.class);
		assertEquals(param, parsedParam);
	}

	@Test
	void test_from_resource_file() throws Exception
	{
		InputStream resource = getClass().getClassLoader().getResourceAsStream("cwms/cda/data/dto/Parameter.json");
		assertNotNull(resource);
		String json = IOUtils.toString(resource, StandardCharsets.UTF_8);
		ContentType contentType = new ContentType(Formats.JSONV2);
		Parameter receivedTzs = Formats.parseContent(contentType, json, Parameter.class);
		assertNotNull(receivedTzs);
	}

	enum SerializationType
	{
		JSONV2(Formats.JSONV2),
		XMLV2(Formats.XMLV2),
		;

		final ContentType _contentType;

		SerializationType(String contentType)
		{
			_contentType = new ContentType(contentType);
		}
	}
}