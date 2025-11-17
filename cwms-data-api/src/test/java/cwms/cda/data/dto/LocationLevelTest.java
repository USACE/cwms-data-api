package cwms.cda.data.dto;

import cwms.cda.api.errors.ExclusiveFieldsException;
import cwms.cda.data.dto.catalog.LocationAlias;
import java.time.ZonedDateTime;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import cwms.cda.api.errors.RequiredFieldException;
import cwms.cda.data.dto.locationlevel.ConstantLocationLevel;
import cwms.cda.data.dto.locationlevel.LocationLevel;
import cwms.cda.data.dto.locationlevel.SeasonalLocationLevel;
import cwms.cda.data.dto.locationlevel.SeasonalValueBean;
import cwms.cda.data.dto.locationlevel.TimeSeriesLocationLevel;
import cwms.cda.data.dto.locationlevel.VirtualLocationLevel;
import cwms.cda.formatters.ContentType;
import cwms.cda.formatters.Formats;
import cwms.cda.formatters.json.JsonV2;

import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
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
		final SeasonalLocationLevel level = (new SeasonalLocationLevel.Builder("Test", zdt)
				.withSeasonalValue(new SeasonalValueBean.Builder().withValue(34.9).build())
				.withIntervalMinutes(23)
				.withOfficeId("SPK"))
				.build();

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
		final SeasonalLocationLevel level = (new SeasonalLocationLevel.Builder("Test", zdt)
				.withSeasonalValue(new SeasonalValueBean.Builder().withValue(21.0).build())
				.withIntervalMonths(12)
				.withOfficeId("SPK"))
				.build();

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

	@Test
	void test_update_level() {
		ConstantLocationLevel existingLevel = new ConstantLocationLevel.Builder("Test", ZonedDateTime.now()).withConstantValue(12345.65).build();

		ZonedDateTime zdt = ZonedDateTime.parse("2021-06-21T08:00:00-07:00[PST8PDT]");

		ConstantLocationLevel updatedLevel = new ConstantLocationLevel.Builder("Test", ZonedDateTime.now()).withConstantValue(1899.45).build();

		LocationLevel updated = LocationLevel.getUpdatedLocationLevel(existingLevel, updatedLevel, zdt);

		assertNotNull(updated);
		assertInstanceOf(ConstantLocationLevel.class, updated);
		ConstantLocationLevel constantLevel = (ConstantLocationLevel) updated;
		assertEquals(1899.45, constantLevel.getConstantValue());
		assertEquals(zdt, constantLevel.getLevelDate());
	}

	@Test
	void test_update_level_virtual() {
		VirtualLocationLevel existingLevel = new VirtualLocationLevel.Builder("Test", ZonedDateTime.now()).withConstituentConnections("L1=L2").build();

		ZonedDateTime zdt = ZonedDateTime.parse("2021-06-21T08:00:00-07:00[PST8PDT]");

		VirtualLocationLevel updatedLevel = new VirtualLocationLevel.Builder("Test", ZonedDateTime.now()).withConstituentConnections("L2=L3").build();

		LocationLevel updated = LocationLevel.getUpdatedLocationLevel(existingLevel, updatedLevel, zdt);

		assertNotNull(updated);
		assertInstanceOf(VirtualLocationLevel.class, updated);
		VirtualLocationLevel virtualLevel = (VirtualLocationLevel) updated;
		assertEquals("L2=L3", virtualLevel.getConstituentConnections());
		assertEquals(zdt, virtualLevel.getLevelDate());
	}

	@Test
	void test_alias_serialization_roundtrip() {
		ZonedDateTime zdt = ZonedDateTime.parse("2021-06-21T08:00:00-07:00[PST8PDT]");
		List<LocationAlias> aliases = new ArrayList<>();
		LocationAlias alias1 = new LocationAlias("AL1", "Office1");
		LocationAlias alias2 = new LocationAlias("AL2", "Office2");
		aliases.add(alias1);
		aliases.add(alias2);

		final ConstantLocationLevel level = new ConstantLocationLevel.Builder("Test", zdt)
																	 .withConstantValue(25.0)
																	 .withOfficeId("SPK")
																	 .withAliases(aliases)
																	 .build();

		ContentType contentType = Formats.parseHeader(Formats.JSONV2, LocationLevel.class);
		String jsonStr = Formats.format(contentType, level);

		// If JSONv2 isn't annotated correctly it will serialize the level like:
		// {"location-level-id":"Test","level-date":1624287600.000000000}

		assertTrue(jsonStr.contains("2021"));
		assertTrue(jsonStr.contains("Office1"));
		assertTrue(jsonStr.contains("Office2"));
		assertTrue(jsonStr.contains("AL1"));
		assertTrue(jsonStr.contains("AL2"));

		ConstantLocationLevel levelFromJson = Formats.parseContent(new ContentType(Formats.JSONV2), jsonStr, ConstantLocationLevel.class);
		assertNotNull(levelFromJson);
		assertEquals(2, levelFromJson.getAliases().size());
		LocationAlias aliasFromJson1 = levelFromJson.getAliases().get(0);
		LocationAlias aliasFromJson2 = levelFromJson.getAliases().get(1);
		assertEquals("AL1", aliasFromJson1.getName());
		assertEquals("Office1", aliasFromJson1.getValue());
		assertEquals("AL2", aliasFromJson2.getName());
		assertEquals("Office2", aliasFromJson2.getValue());
	}

	@Test
	void test_no_alias_serialization_roundtrip() {
		ZonedDateTime zdt = ZonedDateTime.parse("2021-06-21T08:00:00-07:00[PST8PDT]");

		final ConstantLocationLevel level = new ConstantLocationLevel.Builder("Test", zdt)
																	 .withOfficeId("SPK")
																	 .withConstantValue(25.0)
																	 .build();

		ContentType contentType = Formats.parseHeader(Formats.JSONV2, LocationLevel.class);
		String jsonStr = Formats.format(contentType, level);

		// If JSONv2 isn't annotated correctly it will serialize the level like:
		// {"location-level-id":"Test","level-date":1624287600.000000000}

		assertTrue(jsonStr.contains("2021"));
		assertFalse(jsonStr.contains("Office1"));
		assertFalse(jsonStr.contains("Office2"));
		assertFalse(jsonStr.contains("AL1"));
		assertFalse(jsonStr.contains("AL2"));

		ConstantLocationLevel levelFromJson = Formats.parseContent(new ContentType(Formats.JSONV2), jsonStr, ConstantLocationLevel.class);
		assertNotNull(levelFromJson);
		assertTrue(levelFromJson.getAliases().isEmpty());
	}

	@Test
	void testMutuallyExclusiveSeasonalLevel() {
		SeasonalLocationLevel.Builder sb = new SeasonalLocationLevel
			.Builder("LocationLevelId", ZonedDateTime.now())
			.withIntervalMinutes(120)
			.withIntervalMonths(2)
			.withOfficeId("LRL")
			.withIntervalOrigin(ZonedDateTime.now())
			.withSeasonalValue(new SeasonalValueBean.Builder(12.0).withOffsetMonths(2).build());

		assertThrows(ExclusiveFieldsException.class, sb::build);

		try {
			sb.build();
		} catch (ExclusiveFieldsException e) {
			assertEquals("Parser", e.getSource());
			assertEquals("Mutually exclusive fields were provided in the request.", e.getCdaErrorMessage());
			assertEquals("Only one of the following can be defined at "
				+ "once for a seasonal location level: interval-minutes, interval-months",
                e.getDetails().get("Use only one of"));
			assertEquals("Mutually exclusive fields were provided in the request.", e.getMessage());
			assertEquals(400, e.getCdaHttpErrorCode());
		}
	}
}
