package cwms.radar.data.dto;

import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.ArrayList;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import cwms.radar.api.enums.Nation;
import cwms.radar.api.errors.ExclusiveFieldsException;
import cwms.radar.api.errors.RequiredFieldException;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertThrows;

class LocationLevelTest
{



	@Test
	void test_exlusived_fields_monitored()
	{
		final LocationLevel noParameter = new LocationLevel.Builder("Test",ZonedDateTime.now()).build();

		assertThrows(RequiredFieldException.class, () -> {
			noParameter.validate();
		});


		final LocationLevel constAndSeasonalId = new LocationLevel.Builder("Test",ZonedDateTime.now())
													.withConstantValue(5.0)
													.withSeasonalTimeSeriesId("The test timeseries")
													.build();

		assertThrows(ExclusiveFieldsException.class, () -> {
			constAndSeasonalId.validate();
		});


		// we don't need actual values for this test, just the system to think there might be
		final LocationLevel constAndValBean = new LocationLevel.Builder("Test",ZonedDateTime.now())
													.withConstantValue(5.0)
													.withSeasonalValues(new ArrayList<>())
													.build();
		assertThrows(ExclusiveFieldsException.class, () -> {
			constAndSeasonalId.validate();
		});


	}

}
