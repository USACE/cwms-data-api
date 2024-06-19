package cwms.cda.data.dto;

import cwms.cda.formatters.ContentType;
import cwms.cda.formatters.Formats;
import org.apache.commons.io.IOUtils;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class UnitTest
{

	@ParameterizedTest
	@EnumSource(SerializationType.class)
	void test_serialization(SerializationType test)
	{

	}

	@Test
	void test_from_resource_file() throws Exception
	{
		InputStream resource = getClass().getClassLoader().getResourceAsStream("cwms/cda/data/dto/TimeZones.json");
		assertNotNull(resource);
		String json = IOUtils.toString(resource, StandardCharsets.UTF_8);
		ContentType contentType = new ContentType(Formats.JSONV2);
		TimeZones receivedTzs = Formats.parseContent(contentType, json, TimeZones.class);
		assertTrue(!receivedTzs.getTimeZones().isEmpty());
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