package cwms.cda.data.dao.timeseriesprofile;

import java.io.IOException;
import java.sql.SQLException;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import cwms.cda.api.DataApiTestIT;
import cwms.cda.api.enums.Nation;
import cwms.cda.data.dao.LocationsDaoImpl;
import cwms.cda.data.dto.CwmsId;
import cwms.cda.data.dto.Location;
import cwms.cda.data.dto.timeseriesprofile.ParameterInfo;
import cwms.cda.data.dto.timeseriesprofile.TimeSeriesProfile;
import cwms.cda.data.dto.timeseriesprofile.TimeSeriesProfileParser;
import fixtures.CwmsDataApiSetupCallback;
import mil.army.usace.hec.test.database.CwmsDatabaseContainer;
import org.jooq.DSLContext;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import static cwms.cda.data.dao.DaoTest.getDslContext;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertThrowsExactly;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Tag("integration")
final class TimeSeriesProfileParserDaoIT extends DataApiTestIT
{
	private static final Location TIMESERIESPROFILE_LOC = buildTestLocation("TIMESERIESPROFILE_LOC");
	private static final Location LOCATION_AAA = buildTestLocation("AAA");

	@BeforeAll
	public static void setup() throws Exception
	{
		CwmsDatabaseContainer<?> databaseLink = CwmsDataApiSetupCallback.getDatabaseLink();
		databaseLink.connection(c -> {
			DSLContext context = getDslContext(c, databaseLink.getOfficeId());
			LocationsDaoImpl locationsDao = new LocationsDaoImpl(context);
			try
			{
				locationsDao.storeLocation(TIMESERIESPROFILE_LOC);
				locationsDao.storeLocation(LOCATION_AAA);
			}
			catch(IOException e)
			{
				throw new RuntimeException(e);
			}
		});
	}

	@AfterAll
	public static void tearDown() throws Exception {

		CwmsDatabaseContainer<?> databaseLink = CwmsDataApiSetupCallback.getDatabaseLink();
		databaseLink.connection(c -> {
			DSLContext context = getDslContext(c, databaseLink.getOfficeId());
			LocationsDaoImpl locationsDao = new LocationsDaoImpl(context);
	//		locationsDao.deleteLocation(TIMESERIESPORFILE_LOC.getName(), databaseLink.getOfficeId());
		});
	}

	@Test
	void testStoreAndRetrieve() throws Exception {
		CwmsDatabaseContainer<?> databaseLink = CwmsDataApiSetupCallback.getDatabaseLink();
		databaseLink.connection(c -> {
			DSLContext context = getDslContext(c, databaseLink.getOfficeId());

			TimeSeriesProfileDao timeSeriesProfileDao = new TimeSeriesProfileDao(context);

			TimeSeriesProfile timeSeriesProfile = buildTestTimeSeriesProfile("Depth");
			timeSeriesProfileDao.storeTimeSeriesProfile(timeSeriesProfile, false);

			TimeSeriesProfileParserDao timeSeriesProfileParserDao = new TimeSeriesProfileParserDao(context);
			TimeSeriesProfileParser timeSeriesProfileParser = buildTestTimeSeriesProfileParser("Depth");
			timeSeriesProfileParserDao.storeTimeSeriesProfileParser(timeSeriesProfileParser, false);

			TimeSeriesProfileParser retrieved = timeSeriesProfileParserDao.retrieveTimeSeriesProfileParser(timeSeriesProfileParser.getLocationId().getName(),
					timeSeriesProfileParser.getKeyParameter(), timeSeriesProfileParser.getLocationId().getOfficeId());
			assertEquals(timeSeriesProfileParser, retrieved);

			timeSeriesProfileParserDao.deleteTimeSeriesProfileParser(timeSeriesProfileParser.getLocationId().getName(),
					timeSeriesProfileParser.getKeyParameter(), timeSeriesProfileParser.getLocationId().getOfficeId());

			timeSeriesProfileDao.deleteTimeSeriesProfile(timeSeriesProfile.getLocationId().getName(),timeSeriesProfile.getKeyParameter(),
					timeSeriesProfile.getLocationId().getOfficeId());
		});
	}

	@Test
	void testStoreAndDelete() throws SQLException
	{
		CwmsDatabaseContainer<?> databaseLink = CwmsDataApiSetupCallback.getDatabaseLink();
		databaseLink.connection(c -> {
			DSLContext context = getDslContext(c, databaseLink.getOfficeId());

			TimeSeriesProfileDao timeSeriesProfileDao = new TimeSeriesProfileDao(context);

			TimeSeriesProfile timeSeriesProfile = buildTestTimeSeriesProfile("Depth");
			timeSeriesProfileDao.storeTimeSeriesProfile(timeSeriesProfile, false);

			TimeSeriesProfileParserDao timeSeriesProfileParserDao = new TimeSeriesProfileParserDao(context);
			TimeSeriesProfileParser timeSeriesProfileParser = buildTestTimeSeriesProfileParser("Depth");
			timeSeriesProfileParserDao.storeTimeSeriesProfileParser(timeSeriesProfileParser, false);

			timeSeriesProfileParserDao.deleteTimeSeriesProfileParser(timeSeriesProfileParser.getLocationId().getName(),
					timeSeriesProfileParser.getKeyParameter(), timeSeriesProfileParser.getLocationId().getOfficeId());

			assertThrows(Exception.class, ()->timeSeriesProfileParserDao.retrieveTimeSeriesProfileParser(
							timeSeriesProfileParser.getLocationId().getName(),
							timeSeriesProfileParser.getKeyParameter(), timeSeriesProfileParser.getLocationId().getOfficeId()));

			timeSeriesProfileDao.deleteTimeSeriesProfile(timeSeriesProfile.getLocationId().getName(), timeSeriesProfile.getKeyParameter(),
					timeSeriesProfile.getLocationId().getOfficeId());
		});
	}

	@Test
	void testStoreAndRetrieveMultiple() throws SQLException
	{
		CwmsDatabaseContainer<?> databaseLink = CwmsDataApiSetupCallback.getDatabaseLink();
		databaseLink.connection(c -> {
			DSLContext context = getDslContext(c, databaseLink.getOfficeId());

			TimeSeriesProfileDao timeSeriesProfileDao = new TimeSeriesProfileDao(context);

			TimeSeriesProfile timeSeriesProfileDepth = buildTestTimeSeriesProfile("Depth");
			timeSeriesProfileDao.storeTimeSeriesProfile(timeSeriesProfileDepth, false);
			TimeSeriesProfile timeSeriesProfileTemp = buildTestTimeSeriesProfile("Temp-Water");
			timeSeriesProfileDao.storeTimeSeriesProfile(timeSeriesProfileTemp, false);

			TimeSeriesProfileParserDao timeSeriesProfileParserDao = new TimeSeriesProfileParserDao(context);
			TimeSeriesProfileParser timeSeriesProfileParser = buildTestTimeSeriesProfileParser("Depth");
			timeSeriesProfileParserDao.storeTimeSeriesProfileParser(timeSeriesProfileParser, false);

			timeSeriesProfileParser = buildTestTimeSeriesProfileParser("Temp-Water");
			timeSeriesProfileParserDao.storeTimeSeriesProfileParser(timeSeriesProfileParser, false);

			List<TimeSeriesProfileParser> profileParserList = timeSeriesProfileParserDao.retrieveTimeSeriesProfileParsers("*", "*", "*");
			List<TimeSeriesProfileParser> profileParserList1;
//			assertEquals(2, profileParserList.size());
			for(TimeSeriesProfileParser profileParser : profileParserList)
			{
				timeSeriesProfileParserDao.deleteTimeSeriesProfileParser(profileParser.getLocationId().getName(),profileParser.getKeyParameter(),profileParser.getLocationId().getOfficeId());
				profileParserList1 = timeSeriesProfileParserDao.retrieveTimeSeriesProfileParsers("*", "*", "*");
			}

			timeSeriesProfileDao.deleteTimeSeriesProfile(timeSeriesProfileDepth.getLocationId().getName(),timeSeriesProfileDepth.getKeyParameter(),
					timeSeriesProfileDepth.getLocationId().getOfficeId());
			timeSeriesProfileDao.deleteTimeSeriesProfile(timeSeriesProfileTemp.getLocationId().getName(),timeSeriesProfileTemp.getKeyParameter(),
					timeSeriesProfileTemp.getLocationId().getOfficeId());
		});
	}
	@Test
	void testStoreAndCopy() throws SQLException
	{
		CwmsDatabaseContainer<?> databaseLink = CwmsDataApiSetupCallback.getDatabaseLink();
		databaseLink.connection(c -> {
			DSLContext context = getDslContext(c, databaseLink.getOfficeId());

			TimeSeriesProfileDao timeSeriesProfileDao = new TimeSeriesProfileDao(context);

			TimeSeriesProfile timeSeriesProfile = buildTestTimeSeriesProfile("Depth");
			timeSeriesProfileDao.storeTimeSeriesProfile(timeSeriesProfile, false);

			TimeSeriesProfileParserDao timeSeriesProfileParserDao = new TimeSeriesProfileParserDao(context);
			TimeSeriesProfileParser timeSeriesProfileParser = buildTestTimeSeriesProfileParser("Depth");
			timeSeriesProfileParserDao.storeTimeSeriesProfileParser(timeSeriesProfileParser, false);

			TimeSeriesProfileParser retrieved = timeSeriesProfileParserDao.retrieveTimeSeriesProfileParser(timeSeriesProfileParser.getLocationId().getName(),
					timeSeriesProfileParser.getKeyParameter(), timeSeriesProfileParser.getLocationId().getOfficeId());

//			timeSeriesProfileParserDao.copyTimeSeriesProfileParser(timeSeriesProfileParser.getLocationId(),
//					timeSeriesProfileParser.getKeyParameter(), timeSeriesProfileParser.getOfficeId(), "AAA");

			TimeSeriesProfileParser retrieved1 = timeSeriesProfileParserDao.retrieveTimeSeriesProfileParser("AAA",
					timeSeriesProfileParser.getKeyParameter(), timeSeriesProfileParser.getLocationId().getOfficeId());
			assertEquals(timeSeriesProfileParser, retrieved);

		});
	}
	private static TimeSeriesProfile buildTestTimeSeriesProfile(String parameter)
	{
		String officeId = "HEC";
		CwmsId locationId = new CwmsId.Builder().withOfficeId(officeId).withName("location").build();
		return new TimeSeriesProfile.Builder()
				.withLocationId(locationId)
				.withKeyParameter(parameter)
				.withParameterList(Arrays.asList(new String[]{"Temp-Water", "Depth"}))
				.build();

	}
	private static TimeSeriesProfileParser buildTestTimeSeriesProfileParser(String keyParameter) {
		List<ParameterInfo> parameterInfoList = new ArrayList<>();
		parameterInfoList.add( new ParameterInfo.Builder()
				.withParameter("Depth")
				.withIndex(3)
				.withUnit("m")
				.build());
		parameterInfoList.add( new ParameterInfo.Builder()
				.withParameter("Temp-Water")
				.withIndex(4)
				.withUnit("F")
				.build());

		String officeId = "HEC";
		CwmsId locationId = new CwmsId.Builder().withOfficeId(officeId).withName("location").build();
		return

			new TimeSeriesProfileParser.Builder()
					.withLocationId(locationId)
					.withKeyParameter(keyParameter)
					.withRecordDelimiter((char) 10)
					.withFieldDelimiter(',')
					.withTimeFormat("MM/DD/YYYY,HH24:MI:SS")
					.withTimeZone("UTC")
					.withTimeField(1)
					.withTimeInTwoFields(false)
					.withParameterInfoList(parameterInfoList)
				.build();
	}

	private static Location buildTestLocation(String location) {
		String officeId = "HEC";//CwmsDataApiSetupCallback.getDatabaseLink().getOfficeId();
		return new Location.Builder(location, "SITE", ZoneId.of("UTC"),
				38.5613824, -121.7298432, "NVGD29", officeId)
				.withElevation(10.0)
				.withLocationType("SITE")
				.withCountyName("Sacramento")
				.withNation(Nation.US)
				.withActive(true)
				.withStateInitial("CA")
				.withBoundingOfficeId(officeId)
				.withPublishedLatitude(38.5613824)
				.withPublishedLongitude(-121.7298432)
				.withLongName("UNITED STATES")
				.withDescription("for testing")
				.build();
	}
}
