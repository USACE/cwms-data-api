package cwms.cda.data.dao.timeseriesprofile;

import java.io.IOException;
import java.sql.SQLException;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.List;

import cwms.cda.api.DataApiTestIT;
import cwms.cda.api.enums.Nation;
import cwms.cda.data.dao.LocationsDaoImpl;
import cwms.cda.data.dto.Location;
import cwms.cda.data.dto.timeseriesprofile.TimeSeriesProfile;
import fixtures.CwmsDataApiSetupCallback;
import mil.army.usace.hec.test.database.CwmsDatabaseContainer;
import org.jooq.DSLContext;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import static cwms.cda.data.dao.DaoTest.getDslContext;
import static org.junit.jupiter.api.Assertions.assertEquals;

@Tag("integration")
public class TimeSeriesProfileDaoIT extends DataApiTestIT
{
	private static final Location LOCATION_AAA = buildTestLocation("AAA");
	private static final Location TIMESERIESPROFILE_LOC = buildTestLocation("TIMESERIESPROFILE_LOC");
	@BeforeAll
	public static void setup() throws Exception
	{
		CwmsDatabaseContainer<?> databaseLink = CwmsDataApiSetupCallback.getDatabaseLink();
		databaseLink.connection(c -> {
			DSLContext context = getDslContext(c, databaseLink.getOfficeId());
			LocationsDaoImpl locationsDao = new LocationsDaoImpl(context);
			try
			{
				locationsDao.storeLocation(LOCATION_AAA);
				locationsDao.storeLocation(TIMESERIESPROFILE_LOC);
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
			locationsDao.deleteLocation(LOCATION_AAA.getName(), LOCATION_AAA.getOfficeId());
			locationsDao.deleteLocation(TIMESERIESPROFILE_LOC.getName(), TIMESERIESPROFILE_LOC.getOfficeId());
		});
	}

	@Test
	void testCopyTimeSeriesProfile() throws SQLException
	{
		CwmsDatabaseContainer<?> databaseLink = CwmsDataApiSetupCallback.getDatabaseLink();
		databaseLink.connection(c -> {
			DSLContext context = getDslContext(c, databaseLink.getOfficeId());

			TimeSeriesProfileDao timeSeriesProfileDao = new TimeSeriesProfileDao(context);

			TimeSeriesProfile timeSeriesProfile = buildTestTimeSeriesProfile("Depth");

			timeSeriesProfileDao.storeTimeSeriesProfile(timeSeriesProfile, false);
			timeSeriesProfileDao.copyTimeSeriesProfile(timeSeriesProfile.getLocationId(), timeSeriesProfile.getKeyParameter(),
					"AAA","",timeSeriesProfile.getOfficeId());
			TimeSeriesProfile timeSeriesProfileCopied = timeSeriesProfileDao.retrieveTimeSeriesProfile("AAA",
					timeSeriesProfile.getKeyParameter(), timeSeriesProfile.getOfficeId());
			assertEquals("AAA", timeSeriesProfileCopied.getLocationId());
			assertEquals(timeSeriesProfile.getKeyParameter(), timeSeriesProfileCopied.getKeyParameter());
			assertEquals(timeSeriesProfile.getParameterList(),timeSeriesProfileCopied.getParameterList());

			timeSeriesProfileDao.deleteTimeSeriesProfile(timeSeriesProfile.getLocationId(),timeSeriesProfile.getKeyParameter(),timeSeriesProfile.getOfficeId());
			timeSeriesProfileDao.deleteTimeSeriesProfile(timeSeriesProfileCopied.getLocationId(),timeSeriesProfileCopied.getKeyParameter(),timeSeriesProfileCopied.getOfficeId());
		});
	}
	@Test
	void testRetrieveTimeSeriesProfile() throws Exception {
		CwmsDatabaseContainer<?> databaseLink = CwmsDataApiSetupCallback.getDatabaseLink();
		databaseLink.connection(c -> {
			DSLContext context = getDslContext(c, databaseLink.getOfficeId());

			TimeSeriesProfileDao timeSeriesProfileDao = new TimeSeriesProfileDao(context);

			TimeSeriesProfile timeSeriesProfileIn = (buildTestTimeSeriesProfile("Depth"));
			timeSeriesProfileDao.storeTimeSeriesProfile(timeSeriesProfileIn, false);

			TimeSeriesProfile timeSeriesProfileOut = timeSeriesProfileDao.retrieveTimeSeriesProfile(timeSeriesProfileIn.getLocationId(),
					timeSeriesProfileIn.getKeyParameter(), timeSeriesProfileIn.getOfficeId());

			assertEquals(timeSeriesProfileOut, timeSeriesProfileIn);

			timeSeriesProfileDao.deleteTimeSeriesProfile(timeSeriesProfileIn.getLocationId(),
					timeSeriesProfileIn.getKeyParameter(), timeSeriesProfileIn.getOfficeId());
		});
	}

	@Test
	void testRetrieveCatalog() throws Exception {
		CwmsDatabaseContainer<?> databaseLink = CwmsDataApiSetupCallback.getDatabaseLink();
		databaseLink.connection(c -> {
			DSLContext context = getDslContext(c, databaseLink.getOfficeId());

			TimeSeriesProfileDao timeSeriesProfileDao = new TimeSeriesProfileDao(context);
			timeSeriesProfileDao.storeTimeSeriesProfile(buildTestTimeSeriesProfile("Depth"), false);
			timeSeriesProfileDao.storeTimeSeriesProfile(buildTestTimeSeriesProfile("Pres"), false);


			List<TimeSeriesProfile> timeSeriesProfileListBefore =
					timeSeriesProfileDao.retrieveTimeSeriesProfiles("*", "*", "*");

			for(TimeSeriesProfile timeSeriesProfile: timeSeriesProfileListBefore)
			{
				timeSeriesProfileDao.deleteTimeSeriesProfile(timeSeriesProfile.getLocationId(), timeSeriesProfile.getKeyParameter(),
						timeSeriesProfile.getOfficeId());
			}
			List<TimeSeriesProfile> timeSeriesProfileListAfter = timeSeriesProfileDao.retrieveTimeSeriesProfiles("*", "*", "*");

			assertEquals(0,timeSeriesProfileListAfter.size());
			assertEquals(2,timeSeriesProfileListBefore.size());
		});
	}
	@Test
	void testDeleteTimeSeriesProfile() throws Exception {
		CwmsDatabaseContainer<?> databaseLink = CwmsDataApiSetupCallback.getDatabaseLink();
		databaseLink.connection(c -> {
			DSLContext context = getDslContext(c, databaseLink.getOfficeId());

			TimeSeriesProfileDao timeSeriesProfileDao = new TimeSeriesProfileDao(context);
			TimeSeriesProfile timeSeriesProfile = buildTestTimeSeriesProfile("Depth");
			timeSeriesProfileDao.storeTimeSeriesProfile(buildTestTimeSeriesProfile("Depth"), false);

			List<TimeSeriesProfile> timeSeriesProfileListBefore =
					timeSeriesProfileDao.retrieveTimeSeriesProfiles("*", "*", "*");

			timeSeriesProfileDao.deleteTimeSeriesProfile(timeSeriesProfile.getLocationId(), timeSeriesProfile.getKeyParameter(),
						timeSeriesProfile.getOfficeId());

			List<TimeSeriesProfile> timeSeriesProfileListAfter =
					timeSeriesProfileDao.retrieveTimeSeriesProfiles("*", "*", "*");


			assertEquals(timeSeriesProfileListBefore.size()-1,timeSeriesProfileListAfter.size());
		});
	}
	private static Location buildTestLocation(String location) {
		String officeId = CwmsDataApiSetupCallback.getDatabaseLink().getOfficeId();
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
	private static TimeSeriesProfile buildTestTimeSeriesProfile(String keyParameter)
	{
		String officeId = CwmsDataApiSetupCallback.getDatabaseLink().getOfficeId();
		return new TimeSeriesProfile.Builder()
				.withOfficeId(officeId)
				.withLocationId("TIMESERIESPROFILE_LOC")
				.withKeyParameter(keyParameter)
				.withParameterList(Arrays.asList("Pres", "Depth"))
				.build();

	}
}
