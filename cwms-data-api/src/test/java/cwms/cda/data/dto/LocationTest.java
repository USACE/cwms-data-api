package cwms.cda.data.dto;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import cwms.cda.api.enums.Nation;
import cwms.cda.api.errors.FieldException;
import cwms.cda.api.errors.RequiredFieldException;
import cwms.cda.formatters.Formats;
import cwms.cda.formatters.json.JsonV1;

import java.time.ZoneId;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.junit.jupiter.api.Assertions.*;

class LocationTest
{

	@Test
	void serializedOutputContainsActive() throws JsonProcessingException
	{
		Location location = buildTestLocation();
		assertNotNull(location);

		ObjectMapper om = new ObjectMapper();
		String serializedLocation = om.writeValueAsString(location);
		assertNotNull(serializedLocation);

		assertTrue(serializedLocation.contains("\"active\":true"));

	}

	@Test
	void serializedOutputNewline() throws JsonProcessingException
	{
		Location location = buildTestLocationNewLine();
		assertNotNull(location);

		ObjectMapper om = JsonV1.buildObjectMapper();
		String serializedLocation = om.writeValueAsString(location);
		assertNotNull(serializedLocation);

		Location location2 = om.readValue(serializedLocation, Location.class);
		assertNotNull(location2);

	}

	@ParameterizedTest
	@ValueSource(strings = { Formats.JSONV2, Formats.XMLV2 })
	void testSerializationRoundTrip(String format) throws JsonProcessingException
	{
		Location location = buildTestLocation();
		assertNotNull(location);

		String serialized = Formats.format(Formats.parseHeader(format, Location.class), location);
		assertNotNull(serialized);

		Location deserialized = Formats.parseContent(Formats.parseHeader(format, Location.class),
			serialized, Location.class);
		assertEquals(location, deserialized);
	}

	@Test
	void canBuildNullLatLon(){
		Location location = new Location.Builder("TEST_LOCATION2", "SITE", ZoneId.of("UTC"),
				null, null,  // lat/lon are null in this test
				"NVGD29", "LRL")
				.withElevation(10.0)
				.withCountyName("Sacramento")
				.withNation(Nation.US)
				.withActive(true)
				.withStateInitial("CA")
				.withBoundingOfficeId("LRL")
				.withLongName("TEST_LOCATION")
				.withPublishedLatitude(50.0)
				.withPublishedLongitude(50.0)
				.withDescription("for testing")
				.build();

		assertNotNull(location);
		assertNull(location.getLatitude());
		assertNull(location.getLongitude());


		try {
			location.validate();
			fail();
		} catch (FieldException e) {
			Map<String, ? extends List<String>> details = e.getDetails();
			assertNotNull(details);
			assertTrue(details.containsKey(RequiredFieldException.MISSING_FIELDS));
			List<String> missingFields = details.get(RequiredFieldException.MISSING_FIELDS);
			assertTrue(missingFields.contains("latitude"));
			assertTrue(missingFields.contains("longitude"));
		}
	}

	private Location buildTestLocation() {
		return new Location.Builder("TEST_LOCATION2", "SITE", ZoneId.of("UTC"),
				50.0, 50.0, "NVGD29", "LRL")
				.withElevation(10.0)
				.withCountyName("Sacramento")
				.withNation(Nation.US)
				.withActive(true)
				.withStateInitial("CA")
				.withBoundingOfficeId("LRL")
				.withLongName("TEST_LOCATION")
				.withPublishedLatitude(50.0)
				.withPublishedLongitude(50.0)
				.withDescription("for testing")
				.withElevationUnits("m")
				.build();
	}

	private Location buildTestLocationNewLine() {
		return new Location.Builder("TEST_LOCATION2", "SITE", ZoneId.of("UTC"),
				50.0, 50.0, "NVGD29", "LRL")
				.withElevation(10.0)
				.withCountyName("Sacramento")
				.withNation(Nation.US)
				.withActive(true)
				.withStateInitial("CA")
				.withBoundingOfficeId("LRL")
				.withLongName("TEST_LOCATION")
				.withPublishedLatitude(50.0)
				.withPublishedLongitude(50.0)
				.withDescription("for testing\r\n  next line\nhas a double quote \"\r\n this line has a single quote '\r")
				.build();
	}


}
