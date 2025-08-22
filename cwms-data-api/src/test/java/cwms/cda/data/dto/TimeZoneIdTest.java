package cwms.cda.data.dto;

import cwms.cda.api.errors.FieldException;
import cwms.cda.formatters.ContentType;
import cwms.cda.formatters.Formats;
import org.apache.commons.io.IOUtils;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.time.ZoneId;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.*;

class TimeZoneIdTest
{

	@ParameterizedTest
	@EnumSource(SerializationType.class)
	void test_serialization(SerializationType test)
	{
		String tz = "UTC";
		TimeZoneId expectedTz = new TimeZoneId(tz);
		String json = Formats.format(test._contentType, expectedTz);
		TimeZoneId receivedTz = Formats.parseContent(test._contentType, json, TimeZoneId.class);
		assertEquals(expectedTz.getTimeZone(), receivedTz.getTimeZone());
	}

	@Test
	void test_getTimeZone()
	{
		String expectedTz = "UTC";
		TimeZoneId zone = new TimeZoneId(expectedTz);
		String receivedZone = zone.getTimeZone();
		assertEquals(expectedTz, receivedZone);
	}

	@Test
	void test_getTimeZone_null()
	{
		TimeZoneId zone = new TimeZoneId();
		String receivedZone = zone.getTimeZone();
		assertNull(receivedZone);
	}

	@Test
	void test_validate()
	{
		String expectedTz = "UTC";
		TimeZoneId zone = new TimeZoneId(expectedTz);
		assertDoesNotThrow(zone::validate);
	}

	@Test
	void test_validate_failure()
	{
		String expectedTz = "Not a Time Zone";
		TimeZoneId zone = new TimeZoneId(expectedTz);
		assertThrows(FieldException.class, zone::validate);
	}

	@Test
	void test_serialize_list()
	{
		ContentType contentType = new ContentType(Formats.JSONV2);
		TimeZoneIds tzs = new TimeZoneIds(ZoneId.getAvailableZoneIds()
									   .stream()
									   .map(TimeZoneId::new)
									   .collect(Collectors.toList()));
		String json = Formats.format(contentType, tzs);
		TimeZoneIds receivedTzs = Formats.parseContent(contentType, json, TimeZoneIds.class);

		List<TimeZoneId> expectedZones = tzs.getTimeZones();
		List<TimeZoneId> receivedZones = receivedTzs.getTimeZones();

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
		TimeZoneIds receivedTzs = Formats.parseContent(contentType, json, TimeZoneIds.class);
		assertTrue(!receivedTzs.getTimeZones().isEmpty());
	}

	private Executable testZone(TimeZoneId expected, TimeZoneId received)
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