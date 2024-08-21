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
            String officeId ="SPK";
            try {
            createLocation("Glensboro", true, officeId, "SITE");
            createLocation("Greensburg", true, officeId, "SITE");
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            DSLContext context = getDslContext(c, databaseLink.getOfficeId());
            TimeSeriesProfileDao timeSeriesProfileDao = new TimeSeriesProfileDao(context);
            TimeSeriesProfile timeSeriesProfile = buildTestTimeSeriesProfile(officeId, "Glensboro", "Depth");
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
            String officeId = "SPK";
            try {
                createLocation("Glensboro", true, officeId, "SITE");
            }
            catch (Exception e) {
                throw new RuntimeException(e);
            }
            TimeSeriesProfileDao timeSeriesProfileDao = new TimeSeriesProfileDao(context);

            TimeSeriesProfile timeSeriesProfileIn = buildTestTimeSeriesProfile(officeId,"Glensboro","Depth");
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
            String officeId = "SPK";
            try {
                createLocation("Glensboro", true, officeId, "SITE");
                createLocation("Greensburg", true, officeId, "SITE");
            }
            catch (Exception e) {
                throw new RuntimeException(e);
            }
            TimeSeriesProfileDao timeSeriesProfileDao = new TimeSeriesProfileDao(context);
            timeSeriesProfileDao.storeTimeSeriesProfile(buildTestTimeSeriesProfile(officeId,"Glensboro","Depth"), false);
            timeSeriesProfileDao.storeTimeSeriesProfile(buildTestTimeSeriesProfile(officeId,"Greensburg","Pres"), false);


            List<TimeSeriesProfile> timeSeriesProfileListBefore =
                    timeSeriesProfileDao.catalogTimeSeriesProfiles("*", "*", "*");

            for (TimeSeriesProfile timeSeriesProfile : timeSeriesProfileListBefore) {
                    timeSeriesProfileDao.deleteTimeSeriesProfile(timeSeriesProfile.getLocationId().getName(), timeSeriesProfile.getKeyParameter(),
                            timeSeriesProfile.getLocationId().getOfficeId());
            }
            List<TimeSeriesProfile> timeSeriesProfileListAfter = timeSeriesProfileDao.catalogTimeSeriesProfiles("*", "*", "*");

            assertEquals(0, timeSeriesProfileListAfter.size());
            assertEquals(2, timeSeriesProfileListBefore.size());
        }, CwmsDataApiSetupCallback.getWebUser());
    }

    @Test
    void testDeleteTimeSeriesProfile() throws Exception {
        CwmsDatabaseContainer<?> databaseLink = CwmsDataApiSetupCallback.getDatabaseLink();
        databaseLink.connection(c -> {
            DSLContext context = getDslContext(c, databaseLink.getOfficeId());
            String officeId = "SPK";
            try {
                createLocation("Glensboro", true, officeId, "SITE");
            }
            catch (Exception e) {
                throw new RuntimeException(e);
            }
            TimeSeriesProfileDao timeSeriesProfileDao = new TimeSeriesProfileDao(context);
            TimeSeriesProfile timeSeriesProfile = buildTestTimeSeriesProfile(officeId,"Glensboro","Depth");
            timeSeriesProfileDao.storeTimeSeriesProfile(timeSeriesProfile, false);

            List<TimeSeriesProfile> timeSeriesProfileListBefore =
                    timeSeriesProfileDao.catalogTimeSeriesProfiles("*", "*", "*");

            timeSeriesProfileDao.deleteTimeSeriesProfile(timeSeriesProfile.getLocationId().getName(), timeSeriesProfile.getKeyParameter(),
                    timeSeriesProfile.getLocationId().getOfficeId());

            List<TimeSeriesProfile> timeSeriesProfileListAfter =
                    timeSeriesProfileDao.catalogTimeSeriesProfiles("*", "*", "*");


            assertEquals(timeSeriesProfileListBefore.size() - 1, timeSeriesProfileListAfter.size());
        }, CwmsDataApiSetupCallback.getWebUser());
    }

    private static TimeSeriesProfile buildTestTimeSeriesProfile(String officeId, String location, String keyParameter) {
        CwmsId locationId = new CwmsId.Builder().withOfficeId(officeId).withName(location).build();
        CwmsId refTsId = new CwmsId.Builder().withOfficeId(officeId).withName("Greensburg.Stage.Inst.1Hour.0.USGS-rev").build();
        return new TimeSeriesProfile.Builder()
                .withLocationId(locationId)
                .withKeyParameter(keyParameter)
                .withParameterList(Arrays.asList("Pres", "Depth"))
                .withDescription("description")
                .withReferenceTsId(refTsId)
                .build();

    }
}
