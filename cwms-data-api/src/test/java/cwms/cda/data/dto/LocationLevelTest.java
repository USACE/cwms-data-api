package cwms.cda.data.dto;

import java.time.ZonedDateTime;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import cwms.cda.data.dto.locationlevel.LocationLevel;
import cwms.cda.formatters.ContentType;
import cwms.cda.formatters.Formats;
import cwms.cda.formatters.json.JsonV2;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertTrue;

class LocationLevelTest
{
	@Test
	void test_serialization_formats()
	{
		ZonedDateTime zdt = ZonedDateTime.parse("2021-06-21T08:00:00-07:00[PST8PDT]");
		final LocationLevel level = new LocationLevel.Builder("Test", zdt).build();

		ContentType contentType = Formats.parseHeader(Formats.JSONV2, LocationLevel.class);
		String jsonStr = Formats.format(contentType, level);

		// If JSONv2 isn't setup correctly it will serialize the level like:
//		{"location-level-id":"Test","level-date":1624287600.000000000}

		assertTrue(jsonStr.contains("2021"));
	}

	@Test
	void test_serialization_om() throws JsonProcessingException {
		ZonedDateTime zdt = ZonedDateTime.parse("2021-06-21T08:00:00-07:00[PST8PDT]");
		final LocationLevel level = new LocationLevel.Builder("Test", zdt).build();

		ObjectMapper om = JsonV2.buildObjectMapper();
		String jsonStr = om.writeValueAsString(level);

		// If JSONv2 isn't annotated correctly it will serialize the level like:
//		{"location-level-id":"Test","level-date":1624287600.000000000}

		assertTrue(jsonStr.contains("2021"));
	}



}
