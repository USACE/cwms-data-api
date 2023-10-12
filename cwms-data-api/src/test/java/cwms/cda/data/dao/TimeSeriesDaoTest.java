package cwms.cda.data.dao;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.jooq.DSLContext;
import org.jooq.Record1;
import org.jooq.impl.DSL;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import cwms.cda.data.dto.TimeSeries;
import usace.cwms.db.dao.util.CwmsDatabaseVersionInfo;
import usace.cwms.db.dao.util.TimeValueQuality;
import usace.cwms.db.jooq.JooqCwmsDatabaseVersionInfoFactory;
import usace.cwms.db.jooq.codegen.tables.AV_CWMS_TS_ID2;
import usace.cwms.db.jooq.codegen.tables.AV_LOC;
import usace.cwms.db.jooq.dao.CwmsDbLocJooq;
import usace.cwms.db.jooq.dao.CwmsDbTsJooq;

import static cwms.cda.data.dao.DaoTest.getConnection;
import static cwms.cda.data.dao.DaoTest.getDslContext;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;

@Disabled
public class TimeSeriesDaoTest
{
	private static final Logger LOGGER = Logger.getLogger(TimeSeriesDaoTest.class.getName());

	public static final String LOC_ID = "RYAN3";
	public static final String TIME_SERIES_ID = LOC_ID + ".Stage.Inst.5Minutes.0.ZSTORE_TS_TEST" + Calendar.getInstance().get(
			Calendar.MILLISECOND);

	public static final String UNITS = "m";
	public static final Timestamp VERSION_DATE = null;

	public static final Timestamp START_TIME;

	static
	{
		Calendar startCal = Calendar.getInstance();
		startCal.set(2010, 1, 1, 0, 0, 0);
		startCal.set(Calendar.MILLISECOND, 0);
		START_TIME = new Timestamp(startCal.getTimeInMillis());
	}

	public static final Timestamp END_TIME;

	static
	{
		Calendar endCal = Calendar.getInstance();
		endCal.set(2010, 2, 1, 0, 0, 0);
		endCal.set(Calendar.MILLISECOND, 0);
		END_TIME = new Timestamp(endCal.getTimeInMillis());
	}

	public static final Number INCLUSIVE = 1;
	public static final boolean TRIM = true;
	public static final Timestamp[] TRANSACTION_TIME = new Timestamp[1];
	public static final String STORE_RULE = "DELETE INSERT";
	public static final boolean OVERRIDE_PROTECTION = true;
	public static final int COUNT = 500;
	public static final long[] TIME_ARRAY;

	static
	{
		TIME_ARRAY = new long[COUNT];
		Calendar cal = Calendar.getInstance();
		cal.set(2010, 1, 2, 0, 0, 0);
		cal.set(Calendar.MILLISECOND, 0);
		for(int i = 0; i < COUNT; i++)
		{
			TIME_ARRAY[i] = cal.getTimeInMillis();
			cal.add(Calendar.MINUTE, 5);
		}
	}

	public static final double[] VALUE_ARRAY;

	static
	{
		VALUE_ARRAY = new double[COUNT];
		for(int i = 0; i < COUNT; i++)
		{
			VALUE_ARRAY[i] = COUNT + i * 1.2;
		}
	}

	public static final int[] QUALITY_ARRAY;

	static
	{
		QUALITY_ARRAY = new int[COUNT];
		Calendar cal = Calendar.getInstance();
		cal.set(2010, 1, 2, 0, 0, 0);
		for(int i = 0; i < COUNT; i++)
		{
			QUALITY_ARRAY[i] = 3;
		}
	}


	@Test
	public void testCreateEmpty() throws Exception
	{

		String officeId = "LRL";
		try(Connection connection = getConnection(); DSLContext lrl = getDslContext(connection, officeId))
		{
			TimeSeriesDao dao = new TimeSeriesDaoImpl(lrl);

			//			String tsId858 = "RYAN3.Stage.Inst.5Minutes.0.ZSTORE_TS_TEST858";
			//			BigDecimal tsCode = retrieveTsCode(connection, tsId858);

			String tsId = "RYAN3.Stage.Inst.5Minutes.0.ZSTORE_TS_TEST" + Calendar.getInstance().get(
					Calendar.MILLISECOND);
			// Do I need to somehow check whether the location exists?  Its not going to exist if I add the millis to it...
			if(!locationExists(connection, "RYAN3"))
			{
				storeLocation(connection, officeId, "RYAN3");
			}

			ZonedDateTime start = ZonedDateTime.parse("2021-06-21T08:00:00-07:00[PST8PDT]");
			ZonedDateTime end = ZonedDateTime.parse("2021-06-22T08:00:00-07:00[PST8PDT]");
			TimeSeries ts = new TimeSeries(null, -1, 0, tsId, officeId, start, end, null, Duration.ZERO);
			dao.create(ts);
		}


	}

	@Test
	public void testCreateWithData() throws Exception
	{

		String officeId = "LRL";
		try(Connection connection = getConnection(); DSLContext lrl = getDslContext(connection, officeId))
		{
			TimeSeriesDao dao = new TimeSeriesDaoImpl(lrl);

			Calendar instance = Calendar.getInstance();
			String tsId = TIME_SERIES_ID;
			// Do I need to somehow check whether the location exists?  Its not going to exist if I add the millis to it...
			if(!locationExists(connection, "RYAN3"))
			{
				storeLocation(connection, officeId, "RYAN3");
			}

			ZonedDateTime start = ZonedDateTime.parse("2021-06-21T08:00:00-07:00[PST8PDT]");
			ZonedDateTime end = ZonedDateTime.parse("2021-06-21T09:00:00-07:00[PST8PDT]");

			long diff = end.toEpochSecond() - start.toEpochSecond();
			assertEquals(3600, diff); // just to make sure I've got the date parsing thing right.

			int minutes = 15;
			int count = 60 / 15; // do I need a +1?  ie should this be 12 or 13?
			// Also, should end be the last point or the next interval?

			TimeSeries ts = new TimeSeries(null, -1, 0, tsId, officeId, start, end, "m", Duration.ofMinutes(minutes));

			ZonedDateTime next = start;
			for(int i = 0; i < count; i++)
			{
				Timestamp dateTime = Timestamp.valueOf(next.toLocalDateTime());
				ts.addValue(dateTime, (double) i, 0);
				next = next.plus(minutes, ChronoUnit.MINUTES);
			}

			dao.create(ts);
		}


	}


	private boolean locationExists(Connection connection, String locId)
	{
		Integer count = 0;

		Record1<Integer> record = DSL.using(connection).selectCount().from(AV_LOC.AV_LOC).where(
				AV_LOC.AV_LOC.LOCATION_ID.eq(locId)).fetchOptional().orElse(null);
		if(record != null)
		{
			count = record.value1();
		}
		return count > 0;
	}

	private void storeLocation(Connection connection, String officeId, String locationId) throws SQLException
	{
		CwmsDbLocJooq locJooq = new CwmsDbLocJooq();

		locJooq.store(connection, officeId, locationId, null, null, "PST", null, null, null, null, null, null, null,
				locationId, null, null, true, true);
	}

	private BigDecimal retrieveTsCode(Connection connection, String tsId) throws Exception
	{
		BigDecimal bigD = DSL.using(connection).select(AV_CWMS_TS_ID2.AV_CWMS_TS_ID2.TS_CODE).from(
				AV_CWMS_TS_ID2.AV_CWMS_TS_ID2).where(AV_CWMS_TS_ID2.AV_CWMS_TS_ID2.CWMS_TS_ID.eq(tsId)).fetchOptional(
				AV_CWMS_TS_ID2.AV_CWMS_TS_ID2.TS_CODE).orElse(null);

		return bigD;
	}

	@Test
	public void testTimeSeriesStoreRetrieve() throws Exception
	{
		Connection connection = getConnection();

		CwmsDbTsJooq cwmsTsJdbc = new CwmsDbTsJooq();
		createTs(cwmsTsJdbc, connection);
		String officeId = "LRL";
		String timeSeriesDesc = TIME_SERIES_ID;
		String units = UNITS;
		long[] timeArray = TIME_ARRAY;
		double[] valueArray = VALUE_ARRAY;
		int[] qualityArray = QUALITY_ARRAY;
		int count = COUNT;
		String storeRule = STORE_RULE;
		boolean overrideProtection = OVERRIDE_PROTECTION;
		Timestamp versionDate = null;

		timeArray = new long[]{START_TIME.getTime()};
		valueArray = new double[]{9999999.0};
		qualityArray = new int[]{0};
		LOGGER.info("Office Id: " + officeId);
		LOGGER.info("Time Series ID: " + TIME_SERIES_ID);
		LOGGER.info("Storing: " + valueArray[0] + " at " + new Date(timeArray[0]));
		cwmsTsJdbc.store(connection, officeId, timeSeriesDesc, units, timeArray, valueArray, qualityArray, count,
				storeRule, overrideProtection, versionDate, false);
		LOGGER.log(Level.INFO, "Test time series stored.");
		ResultSet retrieve = cwmsTsJdbc.retrieve(connection, "LRL", new String[]{TIME_SERIES_ID}, new String[]{UNITS},
				VERSION_DATE, true, START_TIME, START_TIME, 1, false, new Timestamp[1], new String[1]);
		List<TimeValueQuality> timeValueQualities = cwmsTsJdbc.parseTimeSeriesResultSet(retrieve);
		assertFalse(timeValueQualities.isEmpty());

	}

	private void createTs(CwmsDbTsJooq cwmsTsJdbc, Connection connection) throws SQLException
	{
		String timeSeriesDesc = TIME_SERIES_ID;
		String officeId = "LRL";
		try
		{
			storeLocation(connection, officeId, LOC_ID);

			cwmsTsJdbc.createTsCodeBigInteger(connection, officeId, timeSeriesDesc, 0, 0, 0, false, true);
			connection.commit();
		}
		catch(Exception e)
		{
		}
	}

	@Test
	public void testVersion() throws SQLException
	{
		JooqCwmsDatabaseVersionInfoFactory fac = new JooqCwmsDatabaseVersionInfoFactory();

		String officeId = "LRL";
		try(Connection connection = getConnection(); DSLContext lrl = getDslContext(connection, officeId))
		{
			CwmsDatabaseVersionInfo info = fac.retrieveVersionInfo(connection);
			assertNotNull(info);
			assertFalse(info.getTitle().isEmpty());
			assertFalse(info.getApplication().isEmpty());
			assertFalse(info.getDescription().isEmpty());
			assertFalse(info.getVersion().isEmpty());
		}
	}
}