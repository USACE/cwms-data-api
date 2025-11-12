package cwms.cda.data.dao;

import static cwms.cda.data.dao.DaoTest.getConnection;
import static cwms.cda.data.dao.DaoTest.getDslContext;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

import cwms.cda.data.dto.TimeSeries;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.util.Calendar;
import java.util.Date;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.jooq.DSLContext;
import org.jooq.Record1;
import org.jooq.Result;
import org.jooq.impl.DSL;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import usace.cwms.db.jooq.codegen.packages.CWMS_LOC_PACKAGE;
import usace.cwms.db.jooq.codegen.packages.CWMS_TS_PACKAGE;
import usace.cwms.db.jooq.codegen.packages.cwms_ts.RETRIEVE_TS_2;
import usace.cwms.db.jooq.codegen.packages.cwms_ts.udt.records.DOUBLE_ARRAY;
import usace.cwms.db.jooq.codegen.packages.cwms_ts.udt.records.NUMBER_ARRAY;
import usace.cwms.db.jooq.codegen.tables.AV_DB_CHANGE_LOG;
import usace.cwms.db.jooq.codegen.tables.AV_LOC;

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
	void testCreateEmpty() throws Exception
	{

		String officeId = "LRL";
		try(Connection connection = getConnection())
		{
			DSLContext lrl = getDslContext(connection, officeId);
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
	void testCreateWithData() throws Exception
	{

		String officeId = "LRL";
		try(Connection connection = getConnection())
		{
			DSLContext lrl = getDslContext(connection, officeId);
			TimeSeriesDao dao = new TimeSeriesDaoImpl(lrl);

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

			TimeSeries ts = new TimeSeries(null, -1, 0, tsId,
					officeId, start, end, "m", Duration.ofMinutes(minutes), null,
					null, null, null, null);

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

	private void storeLocation(Connection connection, String officeId, String locationId)
	{
		CWMS_LOC_PACKAGE.call_STORE_LOCATION(getDslContext(connection, officeId).configuration(), locationId,
			null, null, null, null, null,
			null, null, locationId, null, null,
			"PST", null, null, null, null, officeId);
	}

	@Test
	void testTimeSeriesStoreRetrieve() throws Exception
	{
		Connection connection = getConnection();

		createTs(connection);
		String officeId = "LRL";
		String timeSeriesDesc = TIME_SERIES_ID;
		String units = UNITS;
		int count = COUNT;
		String storeRule = STORE_RULE;
		boolean overrideProtection = OVERRIDE_PROTECTION;
		Timestamp versionDate = null;

		long[] timeArray = new long[]{START_TIME.getTime()};
		double[] valueArray = new double[]{9999999.0};
		int[] qualityArray = new int[]{0};
		LOGGER.info("Office Id: " + officeId);
		LOGGER.info("Time Series ID: " + TIME_SERIES_ID);
		LOGGER.info("Storing: " + valueArray[0] + " at " + new Date(timeArray[0]));
		NUMBER_ARRAY times = new NUMBER_ARRAY();
		DOUBLE_ARRAY values = new DOUBLE_ARRAY();
		NUMBER_ARRAY qualities = new NUMBER_ARRAY();
		for (int i = 0; i < count; i++) {
			times.put(i, BigDecimal.valueOf(timeArray[i]));
			values.put(i, valueArray[i]);
			qualities.put(i, BigDecimal.valueOf(qualityArray[i]));
		}

		CWMS_TS_PACKAGE.call_STORE_TS__3(getDslContext(connection, officeId).configuration(), timeSeriesDesc, units,
			times, values, qualities, storeRule, overrideProtection ? "T" : "F", versionDate, officeId, "F");
		LOGGER.log(Level.INFO, "Test time series stored.");
		RETRIEVE_TS_2
			result = CWMS_TS_PACKAGE.call_RETRIEVE_TS_2(getDslContext(connection, officeId).configuration(), UNITS,
				officeId, TIME_SERIES_ID, START_TIME, END_TIME, null, 0, 1, VERSION_DATE, 1);
		assertFalse(result.getP_AT_TSV_RC().isEmpty());
	}

	private void createTs(Connection connection) throws SQLException
	{
		String timeSeriesDesc = TIME_SERIES_ID;
		String officeId = "LRL";
		try
		{
			storeLocation(connection, officeId, LOC_ID);

			CWMS_TS_PACKAGE.call_CREATE_TS_CODE(getDslContext(connection, officeId).configuration(), timeSeriesDesc,
				0, 0, 0, "F", "T", "F", officeId);
			connection.commit();
		}
		catch(Exception e)
		{
			LOGGER.log(Level.CONFIG, "Unable to create TimeSeries: " + e.getMessage());
		}
	}

	@Test
	void testVersion() throws SQLException
	{
		try(Connection connection = getConnection())
		{
			Result<usace.cwms.db.jooq.codegen.tables.records.AV_DB_CHANGE_LOG> results = DSL.using(connection).selectFrom(AV_DB_CHANGE_LOG.AV_DB_CHANGE_LOG).fetch();

			for (usace.cwms.db.jooq.codegen.tables.records.AV_DB_CHANGE_LOG rec : results) {
				String title = rec.getTITLE();
				String application = rec.getAPPLICATION();
				String description = rec.getDESCRIPTION();
				String version = rec.getVERSION();
				assertFalse(title.isEmpty());
				assertFalse(application.isEmpty());
				assertFalse(description.isEmpty());
				assertFalse(version.isEmpty());
			}
		}
	}
}