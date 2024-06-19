package cwms.cda.data.dto;

import cwms.cda.formatters.ContentType;
import cwms.cda.formatters.Formats;
import org.apache.commons.io.IOUtils;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.*;

class ParametersTest
{
	private static final String SPK = "SPK";

	@ParameterizedTest
	@EnumSource(SerializationType.class)
	void test_serialization(SerializationType test)
	{
		Parameter param = ParameterTest.newTestParameter();
		Parameters parameters = new Parameters(SPK, Collections.singletonList(param));
		String json = Formats.format(test._contentType, parameters);
		Parameters parsedParam = Formats.parseContent(test._contentType, json, Parameters.class);
		assertEquals(parameters.getParameters(), parsedParam.getParameters());
		assertEquals(parameters.getOfficeId(), parsedParam.getOfficeId());
	}

	@Test
	void test_from_resource_file() throws Exception
	{
		InputStream resource = getClass().getClassLoader().getResourceAsStream("cwms/cda/data/dto/Parameters.json");
		assertNotNull(resource);
		String json = IOUtils.toString(resource, StandardCharsets.UTF_8);
		ContentType contentType = new ContentType(Formats.JSONV2);
		Parameters receivedTzs = Formats.parseContent(contentType, json, Parameters.class);
		assertFalse(receivedTzs.getParameters().isEmpty());
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