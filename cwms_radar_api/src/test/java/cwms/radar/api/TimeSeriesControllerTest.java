package cwms.radar.api;

import java.io.IOException;
import java.io.InputStream;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Scanner;
import java.util.TimeZone;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.codahale.metrics.MetricRegistry;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import cwms.radar.data.dao.TimeSeriesDao;
import cwms.radar.data.dto.TimeSeries;
import cwms.radar.formatters.Formats;
import cwms.radar.formatters.json.JsonV2;
import cwms.radar.formatters.xml.XMLv2;
import io.javalin.core.util.Header;
import io.javalin.http.Context;
import org.jetbrains.annotations.NotNull;
import org.jooq.DSLContext;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TimeSeriesControllerTest
{



	@Test
	public void testDaoMock() throws JsonProcessingException
	{
		String officeId = "LRL";
		String tsId = "RYAN3.Stage.Inst.5Minutes.0.ZSTORE_TS_TEST";
		TimeSeries expected = buildTimeSeries(officeId, tsId);

		// build a mock dao that returns a pre-built ts when called a certain way
		TimeSeriesDao dao = mock(TimeSeriesDao.class);

		when(
				dao.getTimeseries(eq(""), eq(500), eq(tsId), eq(officeId), eq("EN"),
						isNull(),
						isNull(), isNull(), isNull())).thenReturn(expected);


		// build mock request and response
		final HttpServletRequest request= mock(HttpServletRequest.class);
		final HttpServletResponse response = mock(HttpServletResponse.class);
		final Map<String, ?> map = new LinkedHashMap<>();

		when(request.getAttribute("office-id")).thenReturn(officeId);
		when(request.getAttribute("database")).thenReturn(null);

		when(request.getHeader(Header.ACCEPT)).thenReturn(Formats.JSONV2);

		Map<String, String> urlParams = new LinkedHashMap<>();
		urlParams.put("office", officeId);
		urlParams.put("name", tsId);

		String paramStr = buildParamStr(urlParams);

		when(request.getQueryString()).thenReturn(paramStr);
		when(request.getRequestURL()).thenReturn(new StringBuffer( "http://127.0.0.1:7001/timeseries"));

		// build real context that uses the mock request/response
		Context ctx = new Context(request, response, map);

		// Build a controller that doesn't actually talk to database
		TimeSeriesController controller = new TimeSeriesController(new MetricRegistry()){
			@Override
			protected DSLContext getDslContext(Context ctx)
			{
				return null;
			}

			@NotNull
			@Override
			protected TimeSeriesDao getTimeSeriesDao(DSLContext dsl)
			{
				return dao;
			}
		};
		// make controller use our mock dao

		// Do a controller getAll with our context
		controller.getAll(ctx);

		// Check that the controller accessed our mock dao in the expected way
		verify(dao, times(1)).
				getTimeseries(eq(""), eq(500), eq(tsId), eq(officeId), eq("EN"),
						isNull(), isNull(), isNull(), isNull());

		// Make sure controller thought it was happy
		verify(response).setStatus(200);
		// And make sure controller returned json
		verify(response).setContentType(Formats.JSONV2);

		String result = ctx.resultString();
		assertNotNull(result);  // MAke sure we got some sort of response

		// Turn json response back into a TimeSeries object
		ObjectMapper om = JsonV2.buildObjectMapper();
		TimeSeries actual = om.readValue(result, TimeSeries.class);

		assertSimilar(expected, actual);
	}

	private void assertSimilar(TimeSeries expected, TimeSeries actual)
	{
		// Make sure ts we got back resembles the fakeTS our mock dao was supposed to return.
		assertEquals(expected.getOfficeId(), actual.getOfficeId(), "offices did not match");
		assertEquals(expected.getName(), actual.getName(), "names did not match");
		assertEquals(expected.getValues(), actual.getValues(), "values did not match");
		assertTrue(expected.getBegin().isEqual(actual.getBegin()), "begin dates not equal");
		assertTrue(expected.getEnd().isEqual(actual.getEnd()), "end dates not equal");
	}

	public String loadResourceAsString(String fileName)
	{
		ClassLoader classLoader = getClass().getClassLoader();
		InputStream stream = classLoader.getResourceAsStream(fileName);
		assertNotNull(stream, "Could not load the resource as stream:" + fileName);
		Scanner scanner = new Scanner(stream);
		String contents = scanner.useDelimiter("\\A").next();
		scanner.close();
		return contents;
	}

	@Test
	public void testDeserializeTimeSeriesJaxb() throws IOException
	{
		String officeId = "LRL";
		String tsId = "RYAN3.Stage.Inst.5Minutes.0.ZSTORE_TS_TEST";
		TimeSeries fakeTs = buildTimeSeries(officeId, tsId);

		XMLv2 out = new XMLv2();
		String str = out.format(fakeTs);

		TimeSeries ts2 = TimeSeriesController.deserializeJaxb(str);
		assertNotNull(ts2);

		assertSimilar(fakeTs, ts2);
	}

	@Test
	public void testDeserializeTimeSeriesXmlUTC() throws IOException
	{
		TimeZone aDefault = TimeZone.getDefault();
		try
		{
			TimeZone.setDefault(TimeZone.getTimeZone("UTC"));

			String xml = loadResourceAsString("cwms/radar/api/timeseries_create.xml");
			assertNotNull(xml);
			TimeSeries ts = TimeSeriesController.deserializeTimeSeries(xml, Formats.XMLV2);  // Should this be XMLv2?

			assertNotNull(ts);

			TimeSeries fakeTs = buildTimeSeries("LRL", "RYAN3.Stage.Inst.5Minutes.0.ZSTORE_TS_TEST");
			assertSimilar(fakeTs, ts);
		} finally
		{
			TimeZone.setDefault(aDefault);
		}
	}

	@Test
	public void testDeserializeTimeSeriesXml() throws IOException
	{
			String xml = loadResourceAsString("cwms/radar/api/timeseries_create.xml");
			assertNotNull(xml);
			TimeSeries ts = TimeSeriesController.deserializeTimeSeries(xml, Formats.XMLV2);  // Should this be XMLv2?

			assertNotNull(ts);

			TimeSeries fakeTs = buildTimeSeries("LRL", "RYAN3.Stage.Inst.5Minutes.0.ZSTORE_TS_TEST");
			assertSimilar(fakeTs, ts);
	}

	@Test
	public void testDeserializeTimeSeriesJSON() throws IOException
	{
		String jsonV2 = loadResourceAsString("cwms/radar/api/timeseries_create.json");
		assertNotNull(jsonV2);
		TimeSeries ts = TimeSeriesController.deserializeTimeSeries(jsonV2, Formats.JSONV2);

		assertNotNull(ts);

		TimeSeries fakeTs = buildTimeSeries("LRL", "RYAN3.Stage.Inst.5Minutes.0.ZSTORE_TS_TEST");
		assertSimilar(fakeTs, ts);
	}


	@NotNull
	private TimeSeries buildTimeSeries(String officeId, String tsId)
	{
		ZonedDateTime start = ZonedDateTime.parse("2021-06-21T08:00:00-07:00[PST8PDT]");
		ZonedDateTime end = ZonedDateTime.parse("2021-06-21T09:00:00-07:00[PST8PDT]");

		long diff = end.toEpochSecond() - start.toEpochSecond();
		assertEquals(3600, diff); // just to make sure I've got the date parsing thing right.

		int minutes = 15;
		int count = 60/15 ; // do I need a +1?  ie should this be 12 or 13?
		// Also, should end be the last point or the next interval?

		TimeSeries ts = new TimeSeries(null, -1, 0, tsId, officeId, start, end, "m", Duration.ofMinutes(minutes));

		ZonedDateTime next = start;
		for(int i = 0; i < count; i++)
		{
			Timestamp dateTime = Timestamp.from(next.toInstant());
			ts.addValue(dateTime, (double) i, 0);
			next = next.plus(minutes, ChronoUnit.MINUTES);
		}
		return ts;
	}

	@NotNull
	private String buildParamStr(Map<String, String> urlParams)
	{
		StringBuilder sb = new StringBuilder();
		urlParams.entrySet().forEach(e->sb.append(e.getKey()).append("=").append(e.getValue()).append("&"));

		if(sb.length() > 0){
			sb.setLength(sb.length()-1);
		}

		return sb.toString();
	}

}