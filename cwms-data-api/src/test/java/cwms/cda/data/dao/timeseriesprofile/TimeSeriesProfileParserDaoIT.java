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
import org.jooq.impl.DSL;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import usace.cwms.db.jooq.codegen.packages.CWMS_TS_PROFILE_PACKAGE;
import usace.cwms.db.jooq.codegen.packages.cwms_ts_profile.RETRIEVE_TS_PROFILE_PARSER;

import static cwms.cda.data.dao.DaoTest.getDslContext;
import static org.junit.jupiter.api.Assertions.*;

@Tag("integration")
final class TimeSeriesProfileParserDaoIT extends DataApiTestIT {

    @BeforeAll
    static void setup() throws Exception {
        createLocation("Glensboro", true, "SPK", "SITE");
        createLocation("Greensburg", true, "SPK", "SITE");
    }

    @Test
    void testStoreAndRetrieve() throws Exception {
        String officeId = "SPK";
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

            timeSeriesProfileDao.deleteTimeSeriesProfile(locationName, parameter[0], officeId);
            timeSeriesProfileDao.deleteTimeSeriesProfile(locationName1, parameter[0], officeId);
        }, CwmsDataApiSetupCallback.getWebUser());
    }

    @Test
    void testStoreAndDelete() throws SQLException {
        String officeId = "SPK";
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
    void testCatalogTimeSeriesProfileInclusive() throws SQLException {
        CwmsDatabaseContainer<?> databaseLink = CwmsDataApiSetupCallback.getDatabaseLink();
        String officeId = "SPK";
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

            List<TimeSeriesProfileParser> profileParserList = timeSeriesProfileParserDao.catalogTimeSeriesProfileParsers("*", "*", "*",true);
            for (TimeSeriesProfileParser profileParser : profileParserList) {
                timeSeriesProfileParserDao.deleteTimeSeriesProfileParser(profileParser.getLocationId().getName(), profileParser.getKeyParameter(), profileParser.getLocationId().getOfficeId());
            }

            for(TimeSeriesProfile timeSeriesProfile : timeSeriesProfileList) {
                timeSeriesProfileDao.deleteTimeSeriesProfile(timeSeriesProfile.getLocationId().getName(), timeSeriesProfile.getKeyParameter(),
                        timeSeriesProfile.getLocationId().getOfficeId());
            }

            assertEquals(2, profileParserList.size());
            assertEquals(2, profileParserList.get(0).getParameterInfoList().size() );
            assertEquals(2, profileParserList.get(1).getParameterInfoList().size() );
        }, CwmsDataApiSetupCallback.getWebUser());

    }
    @Test
    void testStoreAndRetrieveMultiple() throws SQLException {
        CwmsDatabaseContainer<?> databaseLink = CwmsDataApiSetupCallback.getDatabaseLink();
        String officeId = "SPK";
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

            List<TimeSeriesProfileParser> profileParserList = timeSeriesProfileParserDao.catalogTimeSeriesProfileParsers("*", "*", "*", true);
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
    void testGetTimeSeriesParameterInfoList() throws Exception {
        String officeId = "SPK";
        String locationName = "Greensburg";
        String[] parameters = {"Depth", "Pres"};
        TimeSeriesProfile instance = buildTestTimeSeriesProfile(officeId, locationName, parameters[0], parameters);
        TimeSeriesProfileParser parser = buildTestTimeSeriesProfileParserIndexed(officeId, locationName, parameters[0]);
        CwmsDatabaseContainer<?> databaseLink = CwmsDataApiSetupCallback.getDatabaseLink();
        databaseLink.connection(c -> {
            DSLContext context = getDslContext(c, databaseLink.getOfficeId());
            TimeSeriesProfileDao timeSeriesProfileDao = new TimeSeriesProfileDao(context);
            timeSeriesProfileDao.storeTimeSeriesProfile(instance, false);
            TimeSeriesProfileParserDao timeSeriesProfileParserDao = new TimeSeriesProfileParserDao(context);
            timeSeriesProfileParserDao.storeTimeSeriesProfileParser((TimeSeriesProfileParserIndexed) parser, false);
            RETRIEVE_TS_PROFILE_PARSER timeSeriesProfileParser
                = CWMS_TS_PROFILE_PACKAGE.call_RETRIEVE_TS_PROFILE_PARSER(DSL.using(c).configuration(),
                locationName, parameters[0], officeId);
            String info = timeSeriesProfileParser.getP_PARAMETER_INFO();
            List<ParameterInfo> parameterInfo = TimeSeriesProfileParserDao.getParameterInfoList(info, timeSeriesProfileParser.getP_RECORD_DELIMITER(),
                    timeSeriesProfileParser.getP_FIELD_DELIMITER());
            assertAll(() -> {
                assertEquals(2, parameterInfo.size());
                assertEquals(parameters[0], parameterInfo.get(1).getParameter());
                assertEquals(parameters[1], parameterInfo.get(0).getParameter());
            });
            timeSeriesProfileParserDao.deleteTimeSeriesProfileParser(locationName, parameters[0], officeId);
            timeSeriesProfileDao.deleteTimeSeriesProfile(locationName, parameters[0], officeId);
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

    private static TimeSeriesProfileParserIndexed buildTestTimeSeriesProfileParserIndexed(String officeId, String location, String keyParameter) {
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
