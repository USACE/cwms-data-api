package cwms.radar.api;

import java.sql.Timestamp;
import java.time.Duration;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.util.LinkedHashMap;
import java.util.Map;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.codahale.metrics.MetricRegistry;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import cwms.radar.data.dao.TimeSeriesDao;
import cwms.radar.data.dto.TimeSeries;
import cwms.radar.formatters.Formats;
import cwms.radar.formatters.json.JsonV2;
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
		TimeSeries fakeTs = buildTimeSeries(officeId, tsId);

		// build a mock dao that returns a pre-built ts when called a certain way
		TimeSeriesDao dao = mock(TimeSeriesDao.class);

		when(
				dao.getTimeseries(isNull(), eq(500), eq(tsId), eq(officeId), eq("EN"),
						isNull(),
						isNull(), isNull(), isNull())).thenReturn(fakeTs);


		// build mock request and response
		final HttpServletRequest request= mock(HttpServletRequest.class);
		final HttpServletResponse response = mock(HttpServletResponse.class);
		final Map<Class<?>, ?> map = new LinkedHashMap<>();

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
				getTimeseries(isNull(), eq(500), eq(tsId), eq(officeId), eq("EN"),
						isNull(),
						isNull(), isNull(), isNull());

		// Make sure controller thought it was happy
		verify(response).setStatus(200);
		// And make sure controller returned json
		verify(response).setContentType(Formats.JSONV2);

		String result = ctx.resultString();
		assertNotNull(result);  // MAke sure we got some sort of response

		// Turn json response back into a TimeSeries object
		ObjectMapper om = JsonV2.buildObjectMapper();
		TimeSeries ts = om.readValue(result, TimeSeries.class);

		// Make sure ts we got back resembles the fakeTS our mock dao was supposed to return.
		assertEquals(ts.getOfficeId(), fakeTs.getOfficeId());
		assertEquals(ts.getName(), fakeTs.getName());
		assertEquals(ts.getValues(), fakeTs.getValues());
		assertTrue(ts.getBegin().isEqual(fakeTs.getBegin()));
		assertTrue(ts.getEnd().isEqual(fakeTs.getEnd()));
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
			Timestamp dateTime = Timestamp.valueOf(next.toLocalDateTime());
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