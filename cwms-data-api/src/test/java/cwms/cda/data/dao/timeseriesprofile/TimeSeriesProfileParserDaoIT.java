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
	private static final Location TIMESERIESPORFILE_LOC = buildTestLocation();

	@BeforeAll
	public static void setup() throws Exception
	{
		CwmsDatabaseContainer<?> databaseLink = CwmsDataApiSetupCallback.getDatabaseLink();
		databaseLink.connection(c -> {
			DSLContext context = getDslContext(c, databaseLink.getOfficeId());
			LocationsDaoImpl locationsDao = new LocationsDaoImpl(context);
			try
			{
				locationsDao.storeLocation(TIMESERIESPORFILE_LOC);
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
			locationsDao.deleteLocation(TIMESERIESPORFILE_LOC.getName(), databaseLink.getOfficeId());
		});
	}

	@Test
	void testStoreAndRetrieve() throws Exception {
		CwmsDatabaseContainer<?> databaseLink = CwmsDataApiSetupCallback.getDatabaseLink();
		databaseLink.connection(c -> {
			DSLContext context = getDslContext(c, databaseLink.getOfficeId());

			TimeSeriesProfileDao timeSeriesProfileDao = new TimeSeriesProfileDao(context);

			TimeSeriesProfile timeSeriesProfile = buildTestTimeSeriesProfile();
			timeSeriesProfileDao.storeTimeSeriesProfile(timeSeriesProfile, false);

			TimeSeriesProfileParserDao timeSeriesProfileParserDao = new TimeSeriesProfileParserDao(context);
			TimeSeriesProfileParser timeSeriesProfileParser = buildTestTimeSeriesProfileParser();
			timeSeriesProfileParserDao.storeTimeSeriesProfileParser(timeSeriesProfileParser, false);

			TimeSeriesProfileParser retrieved = timeSeriesProfileParserDao.retrieveTimeSeriesProfileParser(timeSeriesProfileParser.getLocationId(),
					timeSeriesProfileParser.getKeyParameter(), timeSeriesProfileParser.getOfficeId());
			assertEquals(timeSeriesProfileParser, retrieved);

			timeSeriesProfileParserDao.deleteTimeSeriesProfileParser(timeSeriesProfileParser.getLocationId(),
					timeSeriesProfileParser.getKeyParameter(), timeSeriesProfileParser.getOfficeId());
		});
	}

	@Test
	void testStoreAndDelete() throws SQLException
	{
		CwmsDatabaseContainer<?> databaseLink = CwmsDataApiSetupCallback.getDatabaseLink();
		databaseLink.connection(c -> {
			DSLContext context = getDslContext(c, databaseLink.getOfficeId());

			TimeSeriesProfileDao timeSeriesProfileDao = new TimeSeriesProfileDao(context);

			TimeSeriesProfile timeSeriesProfile = buildTestTimeSeriesProfile();
			timeSeriesProfileDao.storeTimeSeriesProfile(timeSeriesProfile, false);

			TimeSeriesProfileParserDao timeSeriesProfileParserDao = new TimeSeriesProfileParserDao(context);
			TimeSeriesProfileParser timeSeriesProfileParser = buildTestTimeSeriesProfileParser();
			timeSeriesProfileParserDao.storeTimeSeriesProfileParser(timeSeriesProfileParser, false);

			timeSeriesProfileParserDao.deleteTimeSeriesProfileParser(timeSeriesProfileParser.getLocationId(),
					timeSeriesProfileParser.getKeyParameter(), timeSeriesProfileParser.getOfficeId());

			assertThrows(Exception.class, ()->timeSeriesProfileParserDao.retrieveTimeSeriesProfileParser(
							timeSeriesProfileParser.getLocationId(),
							timeSeriesProfileParser.getKeyParameter(), timeSeriesProfileParser.getOfficeId()));

		});
	}

	@Test
	void testStoreAndRetrieveMultiple()
	{
		// TODO
	}
	@Test
	void testStoreAndCopy()
	{
		// TODO
	}
	private static TimeSeriesProfile buildTestTimeSeriesProfile()
	{
		String officeId = CwmsDataApiSetupCallback.getDatabaseLink().getOfficeId();
		return new TimeSeriesProfile.Builder()
				.withOfficeId(officeId)
				.withLocationId("TIMESERIESPROFILE_LOC")
				.withKeyParameter("Depth")
				.withParameterList(Arrays.asList(new String[]{"Pres", "Depth"}))
				.build();

	}
	private static TimeSeriesProfileParser buildTestTimeSeriesProfileParser() {
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

		String officeId = CwmsDataApiSetupCallback.getDatabaseLink().getOfficeId();
		return

			new TimeSeriesProfileParser.Builder()
					.withOfficeId(officeId)
					.withLocationId("TIMESERIESPROFILE_LOC")
					.withKeyParameter("Depth")
					.withRecordDelimiter((char) 10)
					.withFieldDelimiter(',')
					.withTimeFormat("MM/DD/YYYY,HH24:MI:SS")
					.withTimeZone("UTC")
					.withTimeField(1)
					.withTimeInTwoFields(false)
					.withParameterInfoList(parameterInfoList)
				.build();
	}

	private static Location buildTestLocation() {
		String officeId = CwmsDataApiSetupCallback.getDatabaseLink().getOfficeId();
		return new Location.Builder("TIMESERIESPROFILE_LOC", "SITE", ZoneId.of("UTC"),
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
