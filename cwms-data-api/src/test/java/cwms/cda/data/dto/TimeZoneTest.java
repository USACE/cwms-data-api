package cwms.cda.data.dto;

import cwms.cda.api.errors.FieldException;
import cwms.cda.formatters.ContentType;
import cwms.cda.formatters.Formats;
import org.apache.commons.io.IOUtils;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.time.ZoneId;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

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

	@Test
	void test_serialize_list()
	{
		ContentType contentType = new ContentType(Formats.JSONV2);
		TimeZones tzs = new TimeZones(ZoneId.getAvailableZoneIds()
									   .stream()
									   .map(TimeZone::new)
									   .collect(Collectors.toList()));
		String json = Formats.format(contentType, tzs);
		TimeZones receivedTzs = Formats.parseContent(contentType, json, TimeZones.class);

		List<TimeZone> expectedZones = tzs.getTimeZones();
		List<TimeZone> receivedZones = receivedTzs.getTimeZones();

		assertEquals(expectedZones.size(), receivedZones.size());

		assertAll(IntStream.range(0, expectedZones.size())
						   .mapToObj(i -> testZone(expectedZones.get(i), receivedZones.get(i)))
						   .collect(Collectors.toList()));
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

	private Executable testZone(TimeZone expected, TimeZone received)
	{
		return () -> assertEquals(expected.getTimeZone(), received.getTimeZone());
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