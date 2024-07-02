package cwms.cda.data.dto;

import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.ArrayList;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import cwms.cda.api.enums.Nation;
import cwms.cda.api.errors.ExclusiveFieldsException;
import cwms.cda.api.errors.RequiredFieldException;
import cwms.cda.formatters.ContentType;
import cwms.cda.formatters.Formats;
import cwms.cda.formatters.json.JsonV2;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class LocationLevelTest
{



	@Test
	void test_exlusived_fields_monitored()
	{
		final LocationLevel noParameter = new LocationLevel.Builder("Test",ZonedDateTime.now())
				.withOfficeId("SPK")
				.build();

		assertThrows(RequiredFieldException.class, () -> {
			noParameter.validate();
		});


		final LocationLevel constAndSeasonalId = new LocationLevel.Builder("Test",ZonedDateTime.now())
													.withOfficeId("SPK")
													.withConstantValue(5.0)
													.withSeasonalTimeSeriesId("The test timeseries")
													.build();

		assertThrows(ExclusiveFieldsException.class, () -> {
			constAndSeasonalId.validate();
		});


		// we don't need actual values for this test, just the system to think there might be
		final LocationLevel constAndValBean = new LocationLevel.Builder("Test",ZonedDateTime.now())
													.withOfficeId("SPK")
													.withConstantValue(5.0)
													.withSeasonalValues(new ArrayList<>())
													.build();
		assertThrows(ExclusiveFieldsException.class, () -> {
			constAndSeasonalId.validate();
		});


	}

	@Test
	void test_serialization_formats()
	{
		ZonedDateTime zdt = ZonedDateTime.parse("2021-06-21T08:00:00-07:00[PST8PDT]");
		final LocationLevel level = new LocationLevel.Builder("Test", zdt).build();

		ContentType contentType = Formats.parseHeader(Formats.JSONV2);
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
