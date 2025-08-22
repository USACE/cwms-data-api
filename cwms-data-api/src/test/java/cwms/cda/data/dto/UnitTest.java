package cwms.cda.data.dto;

import cwms.cda.formatters.ContentType;
import cwms.cda.formatters.Formats;
import org.apache.commons.io.IOUtils;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

class UnitTest
{

	@ParameterizedTest
	@EnumSource(SerializationType.class)
	void test_serialization(SerializationType test)
	{
		String unitId = "m3/sec";
		String unitSystem = "SI";
		String longName = "Cubic meters per second";
		String description = "Volume rate of 1 cubic meter per second";
		String abstractParameter = "Volume Rate";
		List<String> aliases = new ArrayList<>();
		Unit originalUnit = new Unit(unitId, longName, abstractParameter, description, unitSystem, aliases);
		String text = Formats.format(test._contentType, originalUnit);
		Unit unit = Formats.parseContent(test._contentType, text, Unit.class);
		assertEquals(originalUnit, unit);
	}

	@Test
	void test_from_resource_file() throws Exception
	{
		InputStream resource = getClass().getClassLoader().getResourceAsStream("cwms/cda/data/dto/Unit.json");
		assertNotNull(resource);
		String json = IOUtils.toString(resource, StandardCharsets.UTF_8);
		ContentType contentType = new ContentType(Formats.JSONV2);
		Unit parsedUnit = Formats.parseContent(contentType, json, Unit.class);
		assertNotNull(parsedUnit);
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