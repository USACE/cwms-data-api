package cwms.cda.data.dto;

import cwms.cda.api.errors.FieldException;
import cwms.cda.formatters.ContentType;
import cwms.cda.formatters.Formats;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import static org.junit.jupiter.api.Assertions.*;

class TimeZoneTest
{

	@ParameterizedTest
	@EnumSource(SerializationType.class)
	void test_serialization(SerializationType test)
	{
		String tz = "UTC";
		TimeZone expectedTz = new TimeZone(tz);
		String json = Formats.format(test._contentType, expectedTz);
		TimeZone receivedTz = Formats.parseContent(test._contentType, json, TimeZone.class);
		assertEquals(expectedTz.getTimeZone(), receivedTz.getTimeZone());
	}

	@Test
	void test_getTimeZone()
	{
		String expectedTz = "UTC";
		TimeZone zone = new TimeZone(expectedTz);
		String receivedZone = zone.getTimeZone();
		assertEquals(expectedTz, receivedZone);
	}

	@Test
	void test_getTimeZone_null()
	{
		TimeZone zone = new TimeZone();
		String receivedZone = zone.getTimeZone();
		assertNull(receivedZone);
	}

	@Test
	void test_validate()
	{
		String expectedTz = "UTC";
		TimeZone zone = new TimeZone(expectedTz);
		assertDoesNotThrow(zone::validate);
	}

	@Test
	void test_validate_failure()
	{
		String expectedTz = "Not a Time Zone";
		TimeZone zone = new TimeZone(expectedTz);
		assertThrows(FieldException.class, zone::validate);
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