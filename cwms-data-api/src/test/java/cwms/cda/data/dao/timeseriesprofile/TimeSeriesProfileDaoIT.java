package cwms.cda.data.dao.timeseriesprofile;

import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.ZonedDateTime;
import java.util.Arrays;
import java.util.Date;

import cwms.cda.api.DataApiTestIT;
import cwms.cda.data.dao.TimeSeriesDao;
import cwms.cda.data.dao.TimeSeriesDaoImpl;
import cwms.cda.data.dao.TimeSeriesDeleteOptions;
import cwms.cda.data.dto.CwmsId;
import cwms.cda.data.dto.TimeSeries;
import cwms.cda.data.dto.timeseriesprofile.TimeSeriesProfile;
import cwms.cda.data.dto.timeseriesprofile.TimeSeriesProfileList;
import cwms.cda.data.dto.timeseriesprofile.TimeSeriesProfileTest;
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
class TimeSeriesProfileDaoIT extends DataApiTestIT {
    private static final String OFFICE_ID = "SPK";
    private static final ZonedDateTime start = ZonedDateTime.parse("2021-06-21T08:00:00-07:00[PST8PDT]");
    private static final ZonedDateTime end = ZonedDateTime.parse("2021-06-21T09:00:00-07:00[PST8PDT]");

    @BeforeAll
    static void beforeAll() throws SQLException {
        try {
            createLocation("Glensboro", true, OFFICE_ID, "SITE");
            createLocation("Greensburg", true, OFFICE_ID, "SITE");
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        String tsId = "Greensburg.Stage.Inst.15Minutes.0.USGS-rev";

        int minutes = 15;
        int count = 60 / minutes;
        TimeSeries ts = new TimeSeries(null, -1, 0, tsId, OFFICE_ID, start, end, "m", Duration.ofMinutes(minutes));

        ZonedDateTime next = start;
        for(int i = 0; i < count; i++)
        {
            Timestamp dateTime = Timestamp.valueOf(next.toLocalDateTime());
            ts.addValue(dateTime, (double) i, 0);
            next = next.plusMinutes(minutes);
        }

        CwmsDatabaseContainer<?> databaseLink = CwmsDataApiSetupCallback.getDatabaseLink();
        databaseLink.connection(c -> {
            DSLContext context = getDslContext(c, databaseLink.getOfficeId());
            TimeSeriesDao timeSeriesDao = new TimeSeriesDaoImpl(context);
            timeSeriesDao.create(ts);
        });
    }

    @AfterAll
    static void afterAll() throws SQLException {
        CwmsDatabaseContainer<?> databaseLink = CwmsDataApiSetupCallback.getDatabaseLink();
        databaseLink.connection(c -> {
            DSLContext context = getDslContext(c, databaseLink.getOfficeId());
            TimeSeriesDeleteOptions options = new TimeSeriesDaoImpl.DeleteOptions.Builder()
                    .withStartTime(Date.from(start.toInstant())).withEndTime(Date.from(end.toInstant()))
                    .withEndTimeInclusive(true).withStartTimeInclusive(true).withMaxVersion(true)
                    .withVersionDate(Date.from(start.toInstant())).withOverrideProtection("T").build();
            TimeSeriesDao timeSeriesDao = new TimeSeriesDaoImpl(context);
            timeSeriesDao.delete(OFFICE_ID, "Greensburg.Stage.Inst.15Minutes.0.USGS-rev", options);
        });
    }

    @Test
    void testCopyTimeSeriesProfile() throws SQLException {
        CwmsDatabaseContainer<?> databaseLink = CwmsDataApiSetupCallback.getDatabaseLink();
        databaseLink.connection(c -> {
            DSLContext context = getDslContext(c, databaseLink.getOfficeId());
            TimeSeriesProfileDao timeSeriesProfileDao = new TimeSeriesProfileDao(context);
            TimeSeriesProfile timeSeriesProfile = buildTestTimeSeriesProfile( "Glensboro", "Depth");
            timeSeriesProfileDao.storeTimeSeriesProfile(timeSeriesProfile, false);
            timeSeriesProfileDao.copyTimeSeriesProfile(timeSeriesProfile.getLocationId().getName(), timeSeriesProfile.getKeyParameter(),
                    "Greensburg", null,
                    timeSeriesProfile.getLocationId().getOfficeId());
            TimeSeriesProfile timeSeriesProfileCopied = timeSeriesProfileDao.retrieveTimeSeriesProfile(
                    "Greensburg",
                    timeSeriesProfile.getKeyParameter(), timeSeriesProfile.getLocationId().getOfficeId());
            assertEquals("Greensburg", timeSeriesProfileCopied.getLocationId().getName());
            assertEquals(timeSeriesProfile.getKeyParameter(), timeSeriesProfileCopied.getKeyParameter());
            assertEquals(timeSeriesProfile.getParameterList(), timeSeriesProfileCopied.getParameterList());

            timeSeriesProfileDao.deleteTimeSeriesProfile(timeSeriesProfile.getLocationId().getName(), timeSeriesProfile.getKeyParameter(), timeSeriesProfile.getLocationId().getOfficeId());
            timeSeriesProfileDao.deleteTimeSeriesProfile(timeSeriesProfileCopied.getLocationId().getName(),
                    timeSeriesProfileCopied.getKeyParameter(), timeSeriesProfileCopied.getLocationId().getOfficeId());
        }, CwmsDataApiSetupCallback.getWebUser());
    }

    @Test
    void testRetrieveTimeSeriesProfile() throws Exception {
        CwmsDatabaseContainer<?> databaseLink = CwmsDataApiSetupCallback.getDatabaseLink();
        databaseLink.connection(c -> {
            DSLContext context = getDslContext(c, databaseLink.getOfficeId());
            TimeSeriesProfileDao timeSeriesProfileDao = new TimeSeriesProfileDao(context);

            TimeSeriesProfile timeSeriesProfileIn = buildTestTimeSeriesProfile("Glensboro","Depth");
            timeSeriesProfileDao.storeTimeSeriesProfile(timeSeriesProfileIn, false);

            TimeSeriesProfile timeSeriesProfileOut = timeSeriesProfileDao.retrieveTimeSeriesProfile(timeSeriesProfileIn.getLocationId().getName(),
                    timeSeriesProfileIn.getKeyParameter(), timeSeriesProfileIn.getLocationId().getOfficeId());

            timeSeriesProfileDao.deleteTimeSeriesProfile(timeSeriesProfileIn.getLocationId().getName(),
                    timeSeriesProfileIn.getKeyParameter(), timeSeriesProfileIn.getLocationId().getOfficeId());

            TimeSeriesProfileTest.testAssertEquals(timeSeriesProfileOut, timeSeriesProfileIn, "");

        }, CwmsDataApiSetupCallback.getWebUser());
    }

    @Test
    void testRetrieveCatalog() throws Exception {
        CwmsDatabaseContainer<?> databaseLink = CwmsDataApiSetupCallback.getDatabaseLink();
        databaseLink.connection(c -> {
            DSLContext context = getDslContext(c, databaseLink.getOfficeId());
            TimeSeriesProfileDao timeSeriesProfileDao = new TimeSeriesProfileDao(context);
            timeSeriesProfileDao.storeTimeSeriesProfile(buildTestTimeSeriesProfile("Glensboro","Depth"), false);
            timeSeriesProfileDao.storeTimeSeriesProfile(buildTestTimeSeriesProfile("Greensburg","Pres"), false);


            TimeSeriesProfileList timeSeriesProfileListBefore =
                    timeSeriesProfileDao.catalogTimeSeriesProfiles("*", "*", "*", null, -1);

            for (TimeSeriesProfile timeSeriesProfile : timeSeriesProfileListBefore.getProfileList()) {
                    timeSeriesProfileDao.deleteTimeSeriesProfile(timeSeriesProfile.getLocationId().getName(), timeSeriesProfile.getKeyParameter(),
                            timeSeriesProfile.getLocationId().getOfficeId());
            }
            TimeSeriesProfileList timeSeriesProfileListAfter = timeSeriesProfileDao.catalogTimeSeriesProfiles("*", "*", "*", null, -1);

            assertEquals(0, timeSeriesProfileListAfter.getProfileList().size());
            assertEquals(2, timeSeriesProfileListBefore.getProfileList().size());
        }, CwmsDataApiSetupCallback.getWebUser());
    }

    @Test
    void testRetrievePagedCatalog() throws Exception {
        CwmsDatabaseContainer<?> databaseLink = CwmsDataApiSetupCallback.getDatabaseLink();
        databaseLink.connection(c -> {
            DSLContext context = getDslContext(c, databaseLink.getOfficeId());
            TimeSeriesProfileDao timeSeriesProfileDao = new TimeSeriesProfileDao(context);
            timeSeriesProfileDao.storeTimeSeriesProfile(buildTestTimeSeriesProfile("Glensboro","Depth"), false);
            timeSeriesProfileDao.storeTimeSeriesProfile(buildTestTimeSeriesProfile("Greensburg","Pres"), false);

            TimeSeriesProfileList timeSeriesProfileListPage1 =
                    timeSeriesProfileDao.catalogTimeSeriesProfiles("*", "*", "*", null, 1);

            TimeSeriesProfileList timeSeriesProfileListPage2 =
                    timeSeriesProfileDao.catalogTimeSeriesProfiles("*", "*", "*",
                            timeSeriesProfileListPage1.getNextPage(), 1);

            timeSeriesProfileDao.deleteTimeSeriesProfile(timeSeriesProfileListPage1.getProfileList().get(0).getLocationId().getName(),
                    timeSeriesProfileListPage1.getProfileList().get(0).getKeyParameter(),
                    timeSeriesProfileListPage1.getProfileList().get(0).getLocationId().getOfficeId());

            timeSeriesProfileDao.deleteTimeSeriesProfile(timeSeriesProfileListPage2.getProfileList().get(0).getLocationId().getName(),
                    timeSeriesProfileListPage2.getProfileList().get(0).getKeyParameter(),
                    timeSeriesProfileListPage2.getProfileList().get(0).getLocationId().getOfficeId());

            TimeSeriesProfileList timeSeriesProfileListAfter = timeSeriesProfileDao.catalogTimeSeriesProfiles("*", "*", "*", null, -1);

            assertEquals(0, timeSeriesProfileListAfter.getProfileList().size());
            assertEquals(1, timeSeriesProfileListPage1.getProfileList().size());
            assertEquals(1, timeSeriesProfileListPage2.getProfileList().size());
        }, CwmsDataApiSetupCallback.getWebUser());
    }

    @Test
    void testDeleteTimeSeriesProfile() throws Exception {
        CwmsDatabaseContainer<?> databaseLink = CwmsDataApiSetupCallback.getDatabaseLink();
        databaseLink.connection(c -> {
            DSLContext context = getDslContext(c, databaseLink.getOfficeId());
            TimeSeriesProfileDao timeSeriesProfileDao = new TimeSeriesProfileDao(context);
            TimeSeriesProfile timeSeriesProfile = buildTestTimeSeriesProfile("Glensboro","Depth");
            timeSeriesProfileDao.storeTimeSeriesProfile(timeSeriesProfile, false);

            TimeSeriesProfileList timeSeriesProfileListBefore =
                    timeSeriesProfileDao.catalogTimeSeriesProfiles("*", "*", "*", null, -1);

            timeSeriesProfileDao.deleteTimeSeriesProfile(timeSeriesProfile.getLocationId().getName(), timeSeriesProfile.getKeyParameter(),
                    timeSeriesProfile.getLocationId().getOfficeId());

            TimeSeriesProfileList timeSeriesProfileListAfter =
                    timeSeriesProfileDao.catalogTimeSeriesProfiles("*", "*", "*", null, -1);

            assertEquals(timeSeriesProfileListBefore.getProfileList().size() - 1, timeSeriesProfileListAfter.getProfileList().size());
        }, CwmsDataApiSetupCallback.getWebUser());
    }

    private static TimeSeriesProfile buildTestTimeSeriesProfile(String location, String keyParameter) {
        CwmsId locationId = new CwmsId.Builder().withOfficeId(OFFICE_ID).withName(location).build();
        CwmsId refTsId = new CwmsId.Builder().withOfficeId(OFFICE_ID).withName("Greensburg.Stage.Inst.15Minutes.0.USGS-rev").build();
        return new TimeSeriesProfile.Builder()
                .withLocationId(locationId)
                .withKeyParameter(keyParameter)
                .withParameterList(Arrays.asList("Pres", "Depth"))
                .withDescription("description")
                .withReferenceTsId(refTsId)
                .build();

    }
}
