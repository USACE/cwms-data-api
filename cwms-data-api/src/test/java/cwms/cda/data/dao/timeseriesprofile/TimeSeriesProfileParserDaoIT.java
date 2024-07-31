package cwms.cda.data.dao.timeseriesprofile;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import cwms.cda.api.DataApiTestIT;
import cwms.cda.data.dto.CwmsId;
import cwms.cda.data.dto.timeseriesprofile.ParameterInfo;
import cwms.cda.data.dto.timeseriesprofile.ParameterInfoIndexed;
import cwms.cda.data.dto.timeseriesprofile.TimeSeriesProfile;
import cwms.cda.data.dto.timeseriesprofile.TimeSeriesProfileParser;
import cwms.cda.data.dto.timeseriesprofile.TimeSeriesProfileParserIndexed;
import fixtures.CwmsDataApiSetupCallback;
import mil.army.usace.hec.test.database.CwmsDatabaseContainer;
import org.jooq.DSLContext;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import static cwms.cda.data.dao.DaoTest.getDslContext;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

@Tag("integration")
final class TimeSeriesProfileParserDaoIT extends DataApiTestIT {


    @Test
    void testStoreAndRetrieve() throws Exception {
        String officeId = "LRL";
        String locationName = "Glensboro";
        String locationName1 = "Greensburg";
        String[] parameter = {"Depth", "Temp-Water"};
        CwmsDatabaseContainer<?> databaseLink = CwmsDataApiSetupCallback.getDatabaseLink();
        databaseLink.connection(c -> {
            DSLContext context = getDslContext(c, databaseLink.getOfficeId());

            TimeSeriesProfileDao timeSeriesProfileDao = new TimeSeriesProfileDao(context);

            TimeSeriesProfile timeSeriesProfile = buildTestTimeSeriesProfile(officeId, locationName, parameter[0], parameter);
            timeSeriesProfileDao.storeTimeSeriesProfile(timeSeriesProfile, false);

            timeSeriesProfile = buildTestTimeSeriesProfile(officeId, locationName1, parameter[0], parameter);
            timeSeriesProfileDao.storeTimeSeriesProfile(timeSeriesProfile, false);

            TimeSeriesProfileParserDao timeSeriesProfileParserDao = new TimeSeriesProfileParserDao(context);
            TimeSeriesProfileParserIndexed timeSeriesProfileParser = buildTestTimeSeriesProfileParserIndexed(officeId, locationName, parameter[0]);
            timeSeriesProfileParserDao.storeTimeSeriesProfileParser(timeSeriesProfileParser, false);

            TimeSeriesProfileParser retrieved = timeSeriesProfileParserDao.retrieveTimeSeriesProfileParser(timeSeriesProfileParser.getLocationId().getName(),
                    timeSeriesProfileParser.getKeyParameter(), timeSeriesProfileParser.getLocationId().getOfficeId());
            assertEquals(timeSeriesProfileParser.getLocationId().getName(), retrieved.getLocationId().getName());
            assertEquals(timeSeriesProfileParser.getLocationId().getOfficeId(), retrieved.getLocationId().getOfficeId());
            assertEquals(timeSeriesProfileParser.getTimeFormat(), retrieved.getTimeFormat());
            assertEquals(timeSeriesProfileParser.getKeyParameter(), retrieved.getKeyParameter());

            timeSeriesProfileParserDao.deleteTimeSeriesProfileParser(timeSeriesProfileParser.getLocationId().getName(),
                    timeSeriesProfileParser.getKeyParameter(), timeSeriesProfileParser.getLocationId().getOfficeId());

            timeSeriesProfileDao.deleteTimeSeriesProfile(timeSeriesProfile.getLocationId().getName(), timeSeriesProfile.getKeyParameter(),
                    timeSeriesProfile.getLocationId().getOfficeId());
        }, CwmsDataApiSetupCallback.getWebUser());
    }

    @Test
    void testStoreAndDelete() throws SQLException {
        String officeId = "LRL";
        String locationName = "Glensboro";
        String[] parameter = { "Depth", "Temp-Water"};
        CwmsDatabaseContainer<?> databaseLink = CwmsDataApiSetupCallback.getDatabaseLink();
        databaseLink.connection(c -> {
            DSLContext context = getDslContext(c, databaseLink.getOfficeId());

            TimeSeriesProfileDao timeSeriesProfileDao = new TimeSeriesProfileDao(context);

            TimeSeriesProfile timeSeriesProfile = buildTestTimeSeriesProfile(officeId, locationName, parameter[0], parameter);
            timeSeriesProfileDao.storeTimeSeriesProfile(timeSeriesProfile, false);

            TimeSeriesProfileParserDao timeSeriesProfileParserDao = new TimeSeriesProfileParserDao(context);
            TimeSeriesProfileParserIndexed timeSeriesProfileParser = buildTestTimeSeriesProfileParserIndexed(officeId, locationName, parameter[0]);
            timeSeriesProfileParserDao.storeTimeSeriesProfileParser( timeSeriesProfileParser, false);

            timeSeriesProfileParserDao.deleteTimeSeriesProfileParser(timeSeriesProfileParser.getLocationId().getName(),
                    timeSeriesProfileParser.getKeyParameter(), timeSeriesProfileParser.getLocationId().getOfficeId());

            assertThrows(Exception.class, () -> timeSeriesProfileParserDao.retrieveTimeSeriesProfileParser(
                    timeSeriesProfileParser.getLocationId().getName(),
                    timeSeriesProfileParser.getKeyParameter(), timeSeriesProfileParser.getLocationId().getOfficeId()));

            timeSeriesProfileDao.deleteTimeSeriesProfile(timeSeriesProfile.getLocationId().getName(), timeSeriesProfile.getKeyParameter(),
                    timeSeriesProfile.getLocationId().getOfficeId());
        }, CwmsDataApiSetupCallback.getWebUser());
    }

    @Test
    void testStoreAndRetrieveMultiple() throws SQLException {
        CwmsDatabaseContainer<?> databaseLink = CwmsDataApiSetupCallback.getDatabaseLink();
        String officeId = "LRL";
        String locationName = "Glensboro";
        String[] parameters =  {"Depth", "Temp-Water"};
        databaseLink.connection(c -> {
            DSLContext context = getDslContext(c, databaseLink.getOfficeId());

            TimeSeriesProfileDao timeSeriesProfileDao = new TimeSeriesProfileDao(context);

            List<TimeSeriesProfile> timeSeriesProfileList = new ArrayList<>();
            for(String parameter : parameters) {
                TimeSeriesProfile timeSeriesProfile = buildTestTimeSeriesProfile(officeId, locationName, parameter, parameters);
                timeSeriesProfileDao.storeTimeSeriesProfile(timeSeriesProfile, false);
                timeSeriesProfileList.add(timeSeriesProfile);
            }
            TimeSeriesProfileParserDao timeSeriesProfileParserDao = new TimeSeriesProfileParserDao(context);


            for(String parameter : parameters) {
                TimeSeriesProfileParserIndexed timeSeriesProfileParser = buildTestTimeSeriesProfileParserIndexed(officeId, locationName, parameter);
                timeSeriesProfileParserDao.storeTimeSeriesProfileParser(timeSeriesProfileParser, false);
            }

            List<TimeSeriesProfileParser> profileParserList = timeSeriesProfileParserDao.catalogTimeSeriesProfileParsers("*", "*", "*");
            for (TimeSeriesProfileParser profileParser : profileParserList) {
                timeSeriesProfileParserDao.deleteTimeSeriesProfileParser(profileParser.getLocationId().getName(), profileParser.getKeyParameter(), profileParser.getLocationId().getOfficeId());
            }

            for(TimeSeriesProfile timeSeriesProfile : timeSeriesProfileList) {
                timeSeriesProfileDao.deleteTimeSeriesProfile(timeSeriesProfile.getLocationId().getName(), timeSeriesProfile.getKeyParameter(),
                        timeSeriesProfile.getLocationId().getOfficeId());
            }

            assertEquals(2, profileParserList.size());
        }, CwmsDataApiSetupCallback.getWebUser());
    }

    @Test
    void testStoreAndCopy() throws SQLException {
        String officeId = "LRL";
        String locationName = "Glensboro";
        String locationName1 = "Greensburg";
        String[] parameter = {"Depth", "Temp-Water"};
        CwmsDatabaseContainer<?> databaseLink = CwmsDataApiSetupCallback.getDatabaseLink();
        databaseLink.connection(c -> {
            DSLContext context = getDslContext(c, databaseLink.getOfficeId());

            TimeSeriesProfileDao timeSeriesProfileDao = new TimeSeriesProfileDao(context);

            TimeSeriesProfile timeSeriesProfile = buildTestTimeSeriesProfile(officeId, locationName, parameter[0], parameter);
            timeSeriesProfileDao.storeTimeSeriesProfile(timeSeriesProfile, false);

            timeSeriesProfile = buildTestTimeSeriesProfile(officeId, locationName1, parameter[0], parameter);
            timeSeriesProfileDao.storeTimeSeriesProfile(timeSeriesProfile, false);

            TimeSeriesProfileParserDao timeSeriesProfileParserDao = new TimeSeriesProfileParserDao(context);
            TimeSeriesProfileParserIndexed timeSeriesProfileParser = buildTestTimeSeriesProfileParserIndexed(officeId, locationName, parameter[0]);
            timeSeriesProfileParserDao.storeTimeSeriesProfileParser( timeSeriesProfileParser, false);

            TimeSeriesProfileParser retrieved = timeSeriesProfileParserDao.retrieveTimeSeriesProfileParser(timeSeriesProfileParser.getLocationId().getName(),
                    timeSeriesProfileParser.getKeyParameter(), timeSeriesProfileParser.getLocationId().getOfficeId());

            timeSeriesProfileParserDao.copyTimeSeriesProfileParser(timeSeriesProfileParser.getLocationId().getName(),
                    timeSeriesProfileParser.getKeyParameter(), timeSeriesProfileParser.getLocationId().getOfficeId(), locationName1);

            TimeSeriesProfileParser retrieved1 = timeSeriesProfileParserDao.retrieveTimeSeriesProfileParser(locationName1,
                    timeSeriesProfileParser.getKeyParameter(), timeSeriesProfileParser.getLocationId().getOfficeId());
            assertEquals(timeSeriesProfileParser.getKeyParameter(), retrieved.getKeyParameter());
            assertEquals(locationName1, retrieved1.getLocationId().getName());
            assertEquals(timeSeriesProfileParser.getTimeFormat(), retrieved1.getTimeFormat());

        }, CwmsDataApiSetupCallback.getWebUser());
    }

    private static TimeSeriesProfile buildTestTimeSeriesProfile(String officeId, String location, String keyParameter, String[] parameters) {
        CwmsId locationId = new CwmsId.Builder().withOfficeId(officeId).withName(location).build();
        return new TimeSeriesProfile.Builder()
                .withLocationId(locationId)
                .withKeyParameter(keyParameter)
                .withParameterList(Arrays.asList(parameters))
                .build();

    }

    static private TimeSeriesProfileParserIndexed buildTestTimeSeriesProfileParserIndexed(String officeId, String location, String keyParameter) {
        List<ParameterInfo> parameterInfoList = new ArrayList<>();
        parameterInfoList.add(new ParameterInfoIndexed.Builder()
                .withIndex(6)
                .withParameter("Depth")
                .withUnit("m")
                .build());
        parameterInfoList.add(new ParameterInfoIndexed.Builder()
                .withIndex(5)
                .withParameter("Pres")
                .withUnit("bar")
                .build());
         CwmsId locationId = new CwmsId.Builder().withOfficeId(officeId).withName(location).build();
        return
                (TimeSeriesProfileParserIndexed)
                new TimeSeriesProfileParserIndexed.Builder()
                        .withFieldDelimiter(',')
                        .withTimeField(1)
                        .withLocationId(locationId)
                        .withKeyParameter(keyParameter)
                        .withRecordDelimiter((char) 10)
                         .withTimeFormat("MM/DD/YYYY,HH24:MI:SS")
                        .withTimeZone("UTC")
                         .withTimeInTwoFields(true)
                        .withParameterInfoList(parameterInfoList)
                        .build();
    }


}
