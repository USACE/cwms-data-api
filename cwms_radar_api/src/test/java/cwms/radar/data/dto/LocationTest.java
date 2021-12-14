package cwms.radar.data.dto;

import java.time.ZoneId;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import cwms.radar.api.enums.Nation;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

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
				.build();
	}
}
