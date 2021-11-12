package cwms.radar.data.dto;

import java.time.Duration;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import cwms.radar.formatters.xml.XMLv1;
import cwms.radar.formatters.xml.XMLv2;
import io.javalin.plugin.json.JavalinJackson;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TimeSeriesTest
{

	@Test
	void testRoundtripJson() throws JsonProcessingException
	{
		TimeSeries ts = buildTimeSeries();

		ObjectMapper om = buildObjectMapper();

		String tsBody = om.writeValueAsString(ts);
		assertNotNull(tsBody);

		TimeSeries ts2 = om.readValue(tsBody, TimeSeries.class);
		assertNotNull(ts2);

		assertEquals(ts.getName(), ts2.getName());
		assertEquals(ts.getOfficeId(), ts2.getOfficeId());
		assertTrue(ts.getBegin().isEqual(ts2.getBegin()));
		assertTrue(ts.getEnd().isEqual(ts2.getEnd()));
		assertEquals(ts.getUnits(), ts2.getUnits());
		assertEquals(ts.getValues(), ts2.getValues());
	}

	@NotNull
	private TimeSeries buildTimeSeries()
	{
		String tsId = "RYAN3.Stage.Inst.5Minutes.0.ZSTORE_TS_TEST";

		ZonedDateTime start = ZonedDateTime.parse("2021-06-21T14:00:00-07:00[PST8PDT]");
		ZonedDateTime end = ZonedDateTime.parse("2021-06-22T14:00:00-07:00[PST8PDT]");
		TimeSeries ts = new TimeSeries(null, -1, 0, tsId, "LRL", start, end, null, Duration.ZERO);
		return ts;
	}


	@Test
	void testFormatter()
	{
		ZonedDateTime start = ZonedDateTime.parse("2021-06-21T14:00:00-07:00[PST8PDT]");
		DateTimeFormatter formatter1 = DateTimeFormatter.ofPattern(TimeSeries.ZONED_DATE_TIME_FORMAT);

		String formatted1 = start.format(formatter1);
		ZonedDateTime rt1 = ZonedDateTime.parse(formatted1, formatter1);
		assertTrue(start.isEqual(rt1));
	}



	@NotNull
	public static ObjectMapper buildObjectMapper()
	{
		return buildObjectMapper(new ObjectMapper());
	}

	@NotNull
	public static ObjectMapper buildObjectMapper(ObjectMapper om)
	{
		ObjectMapper retval = om.copy();

		retval.setPropertyNamingStrategy(PropertyNamingStrategies.KEBAB_CASE);
		retval.setSerializationInclusion(JsonInclude.Include.NON_NULL);
		retval.registerModule(new JavaTimeModule());

		return retval;
	}


    @Test
    void test_xml_value_columns()
    {
	    TimeSeries ts = buildTimeSeries();

	    XMLv2 xmlV2 = new XMLv2();
	    String xmlStr = xmlV2.format(ts);
	    assertNotNull(xmlStr);

	    assertFalse(xmlStr.contains("valueColumns"));
	    assertTrue(xmlStr.contains("value-columns"));

	    assertFalse(xmlStr.contains("officeId"));
	    assertTrue(xmlStr.contains("office-id"));
    }
}


