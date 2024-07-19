package cwms.cda.data.dao.timeseriesprofile;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;

import cwms.cda.api.DataApiTestIT;
import cwms.cda.data.dto.CwmsId;
import cwms.cda.data.dto.timeseriesprofile.TimeSeriesProfile;
import cwms.cda.data.dto.timeseriesprofile.TimeSeriesProfileTest;
import fixtures.CwmsDataApiSetupCallback;
import mil.army.usace.hec.test.database.CwmsDatabaseContainer;
import org.jooq.DSLContext;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import static cwms.cda.data.dao.DaoTest.getDslContext;
import static org.junit.jupiter.api.Assertions.assertEquals;

@Tag("integration")
class TimeSeriesProfileDaoIT extends DataApiTestIT {
    @Test
    void testCopyTimeSeriesProfile() throws SQLException {
        CwmsDatabaseContainer<?> databaseLink = CwmsDataApiSetupCallback.getDatabaseLink();
        databaseLink.connection(c -> {
            DSLContext context = getDslContext(c, databaseLink.getOfficeId());
            TimeSeriesProfileDao timeSeriesProfileDao = new TimeSeriesProfileDao(context);
            TimeSeriesProfile timeSeriesProfile = buildTestTimeSeriesProfile("Depth");
            timeSeriesProfileDao.storeTimeSeriesProfile(timeSeriesProfile, false);
            timeSeriesProfileDao.copyTimeSeriesProfile(timeSeriesProfile.getLocationId().getName(), timeSeriesProfile.getKeyParameter(),
                    "BBB", null,
                    timeSeriesProfile.getLocationId().getOfficeId());
            TimeSeriesProfile timeSeriesProfileCopied = timeSeriesProfileDao.retrieveTimeSeriesProfile(
                    "BBB",
                    timeSeriesProfile.getKeyParameter(), timeSeriesProfile.getLocationId().getOfficeId());
            assertEquals("BBB", timeSeriesProfileCopied.getLocationId().getName());
            assertEquals(timeSeriesProfile.getKeyParameter(), timeSeriesProfileCopied.getKeyParameter());
            assertEquals(timeSeriesProfile.getParameterList(), timeSeriesProfileCopied.getParameterList());

            timeSeriesProfileDao.deleteTimeSeriesProfile(timeSeriesProfile.getLocationId().getName(), timeSeriesProfile.getKeyParameter(), timeSeriesProfile.getLocationId().getOfficeId());
            timeSeriesProfileDao.deleteTimeSeriesProfile(timeSeriesProfileCopied.getLocationId().getName(),
                    timeSeriesProfileCopied.getKeyParameter(), timeSeriesProfileCopied.getLocationId().getOfficeId());
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

            TimeSeriesProfile timeSeriesProfileOut = timeSeriesProfileDao.retrieveTimeSeriesProfile(timeSeriesProfileIn.getLocationId().getName(),
                    timeSeriesProfileIn.getKeyParameter(), timeSeriesProfileIn.getLocationId().getOfficeId());

            timeSeriesProfileDao.deleteTimeSeriesProfile(timeSeriesProfileIn.getLocationId().getName(),
                    timeSeriesProfileIn.getKeyParameter(), timeSeriesProfileIn.getLocationId().getOfficeId());

            TimeSeriesProfileTest.testAssertEquals(timeSeriesProfileOut, timeSeriesProfileIn, "");

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

            for (TimeSeriesProfile timeSeriesProfile : timeSeriesProfileListBefore) {
                timeSeriesProfileDao.deleteTimeSeriesProfile(timeSeriesProfile.getLocationId().getName(), timeSeriesProfile.getKeyParameter(),
                        timeSeriesProfile.getLocationId().getOfficeId());
            }
            List<TimeSeriesProfile> timeSeriesProfileListAfter = timeSeriesProfileDao.retrieveTimeSeriesProfiles("*", "*", "*");

            assertEquals(0, timeSeriesProfileListAfter.size());
            assertEquals(2, timeSeriesProfileListBefore.size());
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

            timeSeriesProfileDao.deleteTimeSeriesProfile(timeSeriesProfile.getLocationId().getName(), timeSeriesProfile.getKeyParameter(),
                    timeSeriesProfile.getLocationId().getOfficeId());

            List<TimeSeriesProfile> timeSeriesProfileListAfter =
                    timeSeriesProfileDao.retrieveTimeSeriesProfiles("*", "*", "*");


            assertEquals(timeSeriesProfileListBefore.size() - 1, timeSeriesProfileListAfter.size());
        });
    }

    static private TimeSeriesProfile buildTestTimeSeriesProfile(String keyParameter) {
        String officeId = "HEC";
        CwmsId locationId = new CwmsId.Builder().withOfficeId(officeId).withName("AAA").build();
        CwmsId refTsId = new CwmsId.Builder().withOfficeId(officeId).withName("AAA.Pres.Inst.0.0.VERSION").build();
        return new TimeSeriesProfile.Builder()
                .withLocationId(locationId)
                .withKeyParameter(keyParameter)
                .withParameterList(Arrays.asList("Pres", "Depth"))
                .withDescription("description")
                .withReferenceTsId(refTsId)
                .build();

    }
}
