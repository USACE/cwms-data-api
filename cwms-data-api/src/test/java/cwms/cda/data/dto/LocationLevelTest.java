package cwms.cda.data.dto;

import java.time.ZonedDateTime;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import cwms.cda.api.errors.RequiredFieldException;
import cwms.cda.data.dto.locationlevel.ConstantLocationLevel;
import cwms.cda.data.dto.locationlevel.LocationLevel;
import cwms.cda.data.dto.locationlevel.SeasonalLocationLevel;
import cwms.cda.data.dto.locationlevel.TimeSeriesLocationLevel;
import cwms.cda.data.dto.locationlevel.VirtualLocationLevel;
import cwms.cda.formatters.ContentType;
import cwms.cda.formatters.Formats;
import cwms.cda.formatters.json.JsonV2;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class LocationLevelTest {
	@Test
	void test_serialization_formats_TimeSeries() {
		ZonedDateTime zdt = ZonedDateTime.parse("2021-06-21T08:00:00-07:00[PST8PDT]");
		String tsId = "Test.Elev.Ave.1Day.Regulating";
		final TimeSeriesLocationLevel level = new TimeSeriesLocationLevel.Builder("Test", zdt, tsId).build();

		ContentType contentType = Formats.parseHeader(Formats.JSONV2, LocationLevel.class);
		String jsonStr = Formats.format(contentType, level);

		// If JSONv2 isn't setup correctly it will serialize the level like:
		// {"location-level-id":"Test","level-date":1624287600.000000000}

		assertTrue(jsonStr.contains("2021"));
	}

	@Test
	void test_serialization_formats_Constant() {
		ZonedDateTime zdt = ZonedDateTime.parse("2021-06-21T08:00:00-07:00[PST8PDT]");
		final ConstantLocationLevel level = new ConstantLocationLevel.Builder("Test", zdt).build();

		ContentType contentType = Formats.parseHeader(Formats.JSONV2, LocationLevel.class);
		String jsonStr = Formats.format(contentType, level);

		// If JSONv2 isn't annotated correctly it will serialize the level like:
		// {"location-level-id":"Test","level-date":1624287600.000000000}

		assertTrue(jsonStr.contains("2021"));
	}

	@Test
	void test_serialization_formats_Seasonal() {
		ZonedDateTime zdt = ZonedDateTime.parse("2021-06-21T08:00:00-07:00[PST8PDT]");
		final SeasonalLocationLevel level = new SeasonalLocationLevel.Builder("Test", zdt).build();

		ContentType contentType = Formats.parseHeader(Formats.JSONV2, LocationLevel.class);
		String jsonStr = Formats.format(contentType, level);

		// If JSONv2 isn't annotated correctly it will serialize the level like:
		// {"location-level-id":"Test","level-date":1624287600.000000000}

		assertTrue(jsonStr.contains("2021"));
	}

	@Test
	void test_serialization_formats_Virtual() {
		ZonedDateTime zdt = ZonedDateTime.parse("2021-06-21T08:00:00-07:00[PST8PDT]");
		final VirtualLocationLevel level = new VirtualLocationLevel.Builder("Test", zdt).build();

		ContentType contentType = Formats.parseHeader(Formats.JSONV2, LocationLevel.class);
		String jsonStr = Formats.format(contentType, level);

		// If JSONv2 isn't annotated correctly it will serialize the level like:
		// {"location-level-id":"Test","level-date":1624287600.000000000}

		assertTrue(jsonStr.contains("2021"));
	}

	@Test
	void test_serialization_om_TimeSeries() throws JsonProcessingException {
		ZonedDateTime zdt = ZonedDateTime.parse("2021-06-21T08:00:00-07:00[PST8PDT]");
		String tsId = "Test.Elev.Ave.1Day.Regulating";
		final TimeSeriesLocationLevel level = new TimeSeriesLocationLevel.Builder("Test", zdt, tsId).build();

		ObjectMapper om = JsonV2.buildObjectMapper();
		String jsonStr = om.writeValueAsString(level);

		// If JSONv2 isn't annotated correctly it will serialize the level like:
		// {"location-level-id":"Test","level-date":1624287600.000000000}

		assertTrue(jsonStr.contains("2021"));
	}

	@Test
	void test_serialization_om_Seasonal() throws JsonProcessingException {
		ZonedDateTime zdt = ZonedDateTime.parse("2021-06-21T08:00:00-07:00[PST8PDT]");
		final SeasonalLocationLevel level = new SeasonalLocationLevel.Builder("Test", zdt).build();

		ObjectMapper om = JsonV2.buildObjectMapper();
		String jsonStr = om.writeValueAsString(level);

		// If JSONv2 isn't annotated correctly it will serialize the level like:
		// {"location-level-id":"Test","level-date":1624287600.000000000}

		assertTrue(jsonStr.contains("2021"));
	}

	@Test
	void test_serialization_om_Constant() throws JsonProcessingException {
		ZonedDateTime zdt = ZonedDateTime.parse("2021-06-21T08:00:00-07:00[PST8PDT]");
		final ConstantLocationLevel level = new ConstantLocationLevel.Builder("Test", zdt).build();

		ObjectMapper om = JsonV2.buildObjectMapper();
		String jsonStr = om.writeValueAsString(level);

		// If JSONv2 isn't annotated correctly it will serialize the level like:
		// {"location-level-id":"Test","level-date":1624287600.000000000}

		assertTrue(jsonStr.contains("2021"));
	}

	@Test
	void test_serialization_om_Virtual() throws JsonProcessingException {
		ZonedDateTime zdt = ZonedDateTime.parse("2021-06-21T08:00:00-07:00[PST8PDT]");
		final VirtualLocationLevel level = new VirtualLocationLevel.Builder("Test", zdt).build();

		ObjectMapper om = JsonV2.buildObjectMapper();
		String jsonStr = om.writeValueAsString(level);

		// If JSONv2 isn't annotated correctly it will serialize the level like:
		// {"location-level-id":"Test","level-date":1624287600.000000000}

		assertTrue(jsonStr.contains("2021"));
	}

	@Test
	void test_mutual_exclusivity_seasonal() {
		assertThrows(RequiredFieldException.class, () -> new SeasonalLocationLevel.Builder("Test", ZonedDateTime.now()).build());
		assertThrows(RequiredFieldException.class, () -> new SeasonalLocationLevel.Builder("Test", ZonedDateTime.now())
						.withIntervalMinutes(25).withIntervalMonths(12).build());
	}

}
