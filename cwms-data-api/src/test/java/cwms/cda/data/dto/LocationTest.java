package cwms.cda.data.dto;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import cwms.cda.api.enums.Nation;
import cwms.cda.api.errors.FieldException;
import cwms.cda.api.errors.RequiredFieldException;
import cwms.cda.data.dto.catalog.LocationAlias;
import cwms.cda.formatters.Formats;
import cwms.cda.formatters.json.JsonV1;

import cwms.cda.helpers.DTOMatch;
import java.io.Serializable;
import java.time.ZoneId;
import java.util.ArrayList;
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
		om.registerModule(new Jdk8Module());	// Must be registered to handle Optionals correctly
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
		om.registerModule(new Jdk8Module());
		String serializedLocation = om.writeValueAsString(location);
		assertNotNull(serializedLocation);

		Location location2 = om.readValue(serializedLocation, Location.class);
		assertNotNull(location2);

	}

	@Test
	void deserializeWithAliasedTimezoneName() throws JsonProcessingException {
		String input = "{\n" +
				"  \"office-id\" : \"LRL\",\n" +
				"  \"name\" : \"TEST_LOCATION2\",\n" +
				"  \"latitude\" : 50.0,\n" +
				"  \"longitude\" : 50.0,\n" +
				"  \"active\" : true,\n" +
				"  \"public-name\" : \"TEST_LOCATION2\",\n" +
				"  \"long-name\" : \"TEST_LOCATION\",\n" +
				"  \"description\" : \"for testing\",\n" +
				"  \"timezone-name\" : \"Unknown or Not Applicable\",\n" +   // This is the key line for the test
				"  \"location-kind\" : \"SITE\"\n" +
				"}";

		ObjectMapper om = JsonV1.buildObjectMapper();
		Location location2 = om.readValue(input, Location.class);
		assertNotNull(location2);

		assertEquals("UTC", location2.getTimezoneName());
	}


	@ParameterizedTest
	@ValueSource(strings = { Formats.JSONV2, Formats.XMLV2 })
	void testSerializationRoundTrip(String format)
	{
		Location location = buildTestLocation();
		assertNotNull(location);

		String serialized = Formats.format(Formats.parseHeader(format, Location.class), location);
		assertNotNull(serialized);

		Location deserialized = Formats.parseContent(Formats.parseHeader(format, Location.class),
			serialized, Location.class);
		DTOMatch.assertMatch(location, deserialized);
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
			Map<String, Serializable> details = e.getDetails();
			assertNotNull(details);
			assertTrue(details.containsKey(RequiredFieldException.MISSING_FIELDS));
			String missingFields = String.valueOf(details.get(RequiredFieldException.MISSING_FIELDS));
			assertTrue(missingFields.contains("latitude"));
			assertTrue(missingFields.contains("longitude"));
		}
	}

	@Test
	void test_alias_roundtrip() {
		List<LocationAlias> aliases = new ArrayList<>();

		aliases.add(new LocationAlias("alias1", "type1"));
		aliases.add(new LocationAlias("alias2", "type2"));

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
			.withLatitude(50.0)
			.withLongitude(50.0)
			.withAliases(aliases)
			.build();

		assertNotNull(location);

		String serialized = Formats.format(Formats.parseHeader(Formats.JSONV2, Location.class), location);
		assertNotNull(serialized);

		Location deserialized = Formats.parseContent(Formats.parseHeader(Formats.JSONV2, Location.class),
			serialized, Location.class);
		assertEquals(location, deserialized);
	}

	@Test
	void test_serialization_no_alias()
	{
		Location location = buildTestLocation();
		assertNotNull(location);
		assertFalse(location.getAliases().isPresent());

		String serialized = Formats.format(Formats.parseHeader(Formats.JSONV2, Location.class), location);
		assertNotNull(serialized);

		assertFalse(serialized.contains("aliases"));
	}

	@Test
	void test_serialization_empty_alias()
	{
		Location location = new Location.Builder("TEST_LOCATION2", "SITE", ZoneId.of("UTC"),
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
			.withAliases(new ArrayList<>())
			.build();
		assertNotNull(location);
		assertTrue(location.getAliases().isPresent());

		String serialized = Formats.format(Formats.parseHeader(Formats.JSONV2, Location.class), location);
		assertNotNull(serialized);

		assertTrue(serialized.contains("aliases"));
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
