package cwms.cda.data.dao.timeseriesprofile;

import cwms.cda.api.DataApiTestIT;
import cwms.cda.data.dao.StoreRule;
import cwms.cda.data.dao.TimeSeriesIdentifierDescriptorDao;
import cwms.cda.data.dto.CwmsId;
import cwms.cda.data.dto.timeseriesprofile.ParameterInfo;
import cwms.cda.data.dto.timeseriesprofile.ProfileTimeSeries;
import cwms.cda.data.dto.timeseriesprofile.TimeSeriesProfile;
import cwms.cda.data.dto.timeseriesprofile.TimeSeriesProfileInstance;
import cwms.cda.data.dto.timeseriesprofile.TimeSeriesProfileParser;
import cwms.cda.data.dto.timeseriesprofile.TimeValuePair;
import fixtures.CwmsDataApiSetupCallback;
import mil.army.usace.hec.test.database.CwmsDatabaseContainer;
import org.jooq.DSLContext;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.sql.SQLException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static cwms.cda.data.dao.DaoTest.getDslContext;
import static org.junit.jupiter.api.Assertions.*;

@Tag("integration")
class TimeSeriesProfileInstanceDaoIT extends DataApiTestIT {

    @Test
    void testStoreTimeSeriesProfileInstanceWithDataBlock() throws SQLException {
        CwmsDatabaseContainer<?> databaseLink = CwmsDataApiSetupCallback.getDatabaseLink();
        databaseLink.connection(c -> {
            String officeId = "HEC";
            String locationName = "CCC";
            try {
                createLocation(locationName, true, officeId);
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }

            String versionId = "VERSION";
            String unit = "kPa,m";
            String[] parameterArray = {"Depth", "Pres"};
            int[] parameterIndexArray = {5, 6};
            String[] parameterUnitArray = {"m", "kPa"};
            Instant versionDate = Instant.parse("2024-07-09T12:00:00.00Z");
            Instant startTime = Instant.parse("2018-07-09T19:06:20.00Z");
            Instant endTime = Instant.parse("2025-07-09T19:06:20.00Z");
            String profileData = "a\n09/09/2019,12:48:37,,5,6,7,8,9,10,11,12,13,\n09/09/2019,12:58:57,,5,6,7,8,9,10,11,12,13,\n";
            String timeZone = "UTC";
            String firstDate = "2019-09-09T12:48:57.00Z";
            boolean startInclusive = true;
            boolean endInclusive = true;
            boolean previous = true;
            boolean next = true;
            boolean maxVersion = true;
            char fieldDelimiter = ',';
            char recordDelimiter = '\n';
            int timeField = 1;
            String timeFormat = "MM/DD/YYYY,HH24:MI:SS";

            DSLContext context = getDslContext(c, databaseLink.getOfficeId());

            // store a time series profile
            TimeSeriesProfile timeSeriesProfile = buildTestTimeSeriesProfile(officeId, locationName, parameterArray[0], parameterArray[1]);
            TimeSeriesProfileDao profileDao = new TimeSeriesProfileDao(context);
            profileDao.storeTimeSeriesProfile(timeSeriesProfile, false);

            // store a time series parser
            TimeSeriesProfileParser parser = buildTestTimeSeriesProfileParser(officeId, locationName, parameterArray, parameterIndexArray, parameterUnitArray, recordDelimiter, fieldDelimiter,
                    timeFormat, timeZone, timeField);
            TimeSeriesProfileParserDao timeSeriesProfileParserDao = new TimeSeriesProfileParserDao(context);
            timeSeriesProfileParserDao.storeTimeSeriesProfileParser(parser, false);

            // create a time series profile instance and test storeTimeSeriesProfileInstance
            TimeSeriesProfileInstanceDao timeSeriesProfileInstanceDao = new TimeSeriesProfileInstanceDao(context);
            String storeRule = StoreRule.REPLACE_ALL.toString();
            timeSeriesProfileInstanceDao.storeTimeSeriesProfileInstance(timeSeriesProfile, profileData, versionDate, versionId, storeRule, false);

            try {
                // retrieve the time series profile instance we just stored
                TimeSeriesProfileInstance timeSeriesProfileInstance = retrieveTimeSeriesProfileInstance(officeId, locationName, parameterArray[0], versionId, unit,
                        startTime, endTime, timeZone, startInclusive, endInclusive, previous, next, versionDate, maxVersion);
                // cleanup: delete the instance
                timeSeriesProfileInstanceDao.deleteTimeSeriesProfileInstance(timeSeriesProfileInstance.getTimeSeriesProfile().getLocationId(),
                        timeSeriesProfileInstance.getTimeSeriesProfile().getKeyParameter(),
                        versionId, Instant.parse(firstDate), timeZone, false, versionDate);
                // check if the instant contains the timeseries we stpred
                assertEquals(parameterArray.length, timeSeriesProfileInstance.getTimeSeriesList().size());
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
            // cleanup: remove the timeseries we stored
            TimeSeriesIdentifierDescriptorDao timeSeriesDao = new TimeSeriesIdentifierDescriptorDao(context);
            for(String parameter: parameterArray) {
                timeSeriesDao.deleteAll(officeId, locationName + "." + parameter + ".Inst.0.0." + versionId);
                timeSeriesDao.deleteAll(officeId, locationName + "." + parameter + ".Inst.0.0." + versionId);
            }
        });
    }

    @Test
    void testRetrieveTimeSeriesProfileInstances() throws SQLException {
        Instant versionDate = Instant.parse("2024-07-09T12:00:00.00Z");
        String officeId = "HEC";
        String location = "AAA";
        String[] keyParameter = {"Depth", "m"};
        String[] parameter1 = {"Pres", "psi"};
        String[] versions = {"VERSION", "VERSION2", "VERSION3"};
        String officeIdMask = "*";
        String locationMask = "*";
        String parameterMask = "*";
        String versionMask = "*";
        Instant startDate = Instant.parse("2020-07-09T12:00:00.00Z");
        Instant endDate = Instant.parse("2025-07-09T12:00:00.00Z");
        String timeZone = "UTC";
        Instant firstDate = Instant.parse("2024-07-09T19:00:11.00Z");
        Instant[] dateTimeArray = {Instant.parse("2024-07-09T19:00:11.00Z"), Instant.parse("2024-07-09T20:00:22.00Z")};
        double[] valueArray = {1, 4};

        // sore a few timeseries profile instances
        for (String version : versions) {
            storeTimeSeriesProfileInstance(officeId, location, keyParameter, parameter1, version, versionDate, dateTimeArray, valueArray, timeZone);
        }
        CwmsDatabaseContainer<?> databaseLink = CwmsDataApiSetupCallback.getDatabaseLink();

        databaseLink.connection(c -> {
            DSLContext context = getDslContext(c, databaseLink.getOfficeId());
            TimeSeriesProfileInstanceDao timeSeriesProfileInstanceDao = new TimeSeriesProfileInstanceDao(context);
            // test retrieveTimeSeriesProfileInstances
            List<TimeSeriesProfileInstance> result = timeSeriesProfileInstanceDao.retrieveTimeSeriesProfileInstances(officeIdMask, locationMask, parameterMask,
                    versionMask, startDate, endDate, timeZone);

            // cleanup: delete the time series profile instances we created
            boolean overrideProtection = false;
            for (TimeSeriesProfileInstance timeSeriesProfileInstance : result) {
                timeSeriesProfileInstanceDao.deleteTimeSeriesProfileInstance(timeSeriesProfileInstance.getTimeSeriesProfile().getLocationId(),
                        timeSeriesProfileInstance.getTimeSeriesProfile().getKeyParameter(), timeSeriesProfileInstance.getVersion(),
                        firstDate, timeZone, overrideProtection, timeSeriesProfileInstance.getVersionDate());
            }
            // check if we retrieve all the instances we stored
            assertEquals(versions.length, result.size());
        });
    }

    @Test
    void testRetrieveTimeSeriesProfileInstance() throws SQLException {
        String officeId = "HEC";
        String versionID = "VERSION";
        String locationName = "AAA";
        String[] keyParameter = {"Depth", "m"};
        String[] parameter1 = {"Pres", "psi"};
        String unit = "bar,m";
        Instant startTime = Instant.parse("2023-07-09T19:00:11.00Z");
        Instant endTime = Instant.parse("2025-01-01T19:00:22.00Z");
        String timeZone = "UTC";
        String startInclusive = "T";
        String endInclusive = "T";
        String previous = "T";
        String next = "T";
        String maxVersion = "T";
        Instant[] dateTimeArray = {Instant.parse("2024-07-09T19:00:11.00Z"), Instant.parse("2024-07-09T20:00:22.00Z")};
        double[] valueArray = {1, 4};
        Instant versionDate = Instant.parse("2024-07-09T12:00:00.00Z");

        // store a time series profile instance
        storeTimeSeriesProfileInstance(officeId, locationName, keyParameter, parameter1, versionID, versionDate, dateTimeArray, valueArray, timeZone);
        CwmsDatabaseContainer<?> databaseLink = CwmsDataApiSetupCallback.getDatabaseLink();
        databaseLink.connection(c -> {
            DSLContext context = getDslContext(c, databaseLink.getOfficeId());
            TimeSeriesProfileInstanceDao timeSeriesProfileInstanceDao = new TimeSeriesProfileInstanceDao(context);
            CwmsId location = new CwmsId.Builder()
                    .withName(locationName)
                    .withOfficeId(officeId)
                    .build();
            // test the retrieveTimeSeriesProfileInstance method
            TimeSeriesProfileInstance result = timeSeriesProfileInstanceDao.retrieveTimeSeriesProfileInstance(location, keyParameter[0], versionID,
                    unit, startTime, endTime, timeZone, startInclusive, endInclusive, previous, next, versionDate,
                    maxVersion);

            // cleanup: delete the timeseries we created
            TimeSeriesIdentifierDescriptorDao timeSeriesDao = new TimeSeriesIdentifierDescriptorDao(context);
            timeSeriesDao.deleteAll(officeId, locationName + "." + keyParameter[0] + ".Inst.0.0." + versionID);
            timeSeriesDao.deleteAll(officeId, locationName + "." + parameter1[0] + ".Inst.0.0." + versionID);

            // check if the retrieved timeseries profile instance has the same tineseries as the one we stored
            assertEquals(2, result.getTimeSeriesList().size());
            assertEquals(2, result.getTimeSeriesList().get(0).getTimeValuePairList().size());

        });
    }

    @Test
    void testDeleteTimeSeriesProfileInstance() throws SQLException {
        Instant versionDate = Instant.parse("2024-07-09T12:00:00.00Z");
        String officeId = "HEC";
        String locationName = "AAA";
        String[] keyParameter = {"Depth", "m"};
        String[] parameter1 = {"Pres", "psi"};
        String unit = "kPa,m";
        String version = "VERSION";
        String timeZone = "UTC";
        Instant startTime = Instant.parse("2018-07-09T19:06:20.00Z");
        Instant endTime = Instant.parse("2025-07-09T19:06:20.00Z");
        boolean overrideProtection = false;
        boolean startInclusive = true;
        boolean endInclusive = true;
        boolean previous = true;
        boolean next = true;
        boolean maxVersion = true;
        Instant firstDate = Instant.parse("2024-07-09T19:00:11.00Z");
        Instant[] dateTimeArray = {Instant.parse("2024-07-09T19:00:11.00Z"), Instant.parse("2024-07-09T20:00:22.00Z")};
        double[] valueArray = {3, 5};

        // store a time series profile instance
        storeTimeSeriesProfileInstance(officeId, locationName, keyParameter, parameter1, version, versionDate, dateTimeArray, valueArray, timeZone);
        CwmsDatabaseContainer<?> databaseLink = CwmsDataApiSetupCallback.getDatabaseLink();
        databaseLink.connection(c -> {
            DSLContext context = getDslContext(c, databaseLink.getOfficeId());
            TimeSeriesProfileInstanceDao timeSeriesProfileInstanceDao = new TimeSeriesProfileInstanceDao(context);

            // retrieve the instance make sure it exists
            TimeSeriesProfileInstance timeSeriesProfileInstance;
            try {
                timeSeriesProfileInstance = retrieveTimeSeriesProfileInstance(officeId, locationName, keyParameter[0], version, unit,
                        startTime, endTime, timeZone, startInclusive, endInclusive, previous, next, versionDate, maxVersion);
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
            // instance exists?
            assertNotNull(timeSeriesProfileInstance);

            CwmsId location = new CwmsId.Builder()
                    .withName(locationName)
                    .withOfficeId(officeId)
                    .build();

            //  testing delete
            timeSeriesProfileInstanceDao.deleteTimeSeriesProfileInstance(location, keyParameter[0], version,
                    firstDate, timeZone, overrideProtection, versionDate);

            // check if instance was deleted
            timeSeriesProfileInstance = null;
            try {
                timeSeriesProfileInstance = retrieveTimeSeriesProfileInstance(officeId, locationName, keyParameter[0], version, unit,
                        startTime, endTime, timeZone, startInclusive, endInclusive, previous, next, versionDate, maxVersion);
            } catch (SQLException e) {
                e.printStackTrace();
            }
            // instance does not exist anymore
            assertNull(timeSeriesProfileInstance);

            // cleanup the timeseries
            TimeSeriesIdentifierDescriptorDao timeSeriesDao = new TimeSeriesIdentifierDescriptorDao(context);
            timeSeriesDao.deleteAll(officeId, locationName + "." + keyParameter[0] + ".Inst.0.0." + version);
            timeSeriesDao.deleteAll(officeId, locationName + "." + parameter1[0] + ".Inst.0.0." + version);
        });
    }

    @Test
    void testStoreTimeSeriesProfileInstance() throws SQLException {
        String versionId = "VERSION";
        String officeId = "HEC";
        String locationName = "AAA";
        String[] parameterArray = {"Depth", "Pres"};
        String[] parameterUnitArray = {"m", "bar"};
        int[] parameterIndexArray = {5, 6};
        String[] keyParameter = {parameterArray[0], parameterUnitArray[0]};
        String[] parameter1 = {parameterArray[1], parameterUnitArray[1]};
        String unit = "kPa,m";
        Instant versionDate = Instant.parse("2024-07-09T12:00:00.00Z");
        String timeZone = "UTC";
        Instant startTime = Instant.parse("2018-07-09T19:06:20.00Z");
        Instant endTime = Instant.parse("2025-07-09T19:06:20.00Z");
        Instant firstDate = Instant.parse("2024-07-09T19:00:11.00Z");
        boolean startInclusive = true;
        boolean endInclusive = true;
        boolean previous = true;
        boolean next = true;
        boolean maxVersion = true;
        Instant[] dateTimeArray = {Instant.parse("2024-07-09T19:00:11.00Z"), Instant.parse("2024-07-09T20:00:22.00Z")};
        double[] valueArray = {1.0, 5.0};
        char fieldDelimiter = ',';
        char recordDelimiter = '\n';
        int timeField = 1;
        String timeFormat = "MM/DD/YYYY,HH24:MI:SS";
        CwmsDatabaseContainer<?> databaseLink = CwmsDataApiSetupCallback.getDatabaseLink();
        databaseLink.connection(c -> {
            DSLContext context = getDslContext(c, databaseLink.getOfficeId());
            TimeSeriesProfileInstance timeSeriesProfileInstance;


            // store a profile
            TimeSeriesProfile timeSeriesProfile = buildTestTimeSeriesProfile(officeId, locationName, keyParameter[0], parameter1[0]);
            TimeSeriesProfileDao profileDao = new TimeSeriesProfileDao(context);
            profileDao.storeTimeSeriesProfile(timeSeriesProfile, false);

            // store a parser
            TimeSeriesProfileParser parser = buildTestTimeSeriesProfileParser(officeId, locationName, parameterArray, parameterIndexArray, parameterUnitArray, recordDelimiter, fieldDelimiter,
                    timeFormat, timeZone, timeField);
            TimeSeriesProfileParserDao timeSeriesProfileParserDao = new TimeSeriesProfileParserDao(context);
            timeSeriesProfileParserDao.storeTimeSeriesProfileParser(parser, false);

            String storeRule = StoreRule.REPLACE_ALL.toString();
            /// create an instance for parameter Depth
            TimeSeriesProfileInstanceDao timeSeriesProfileInstanceDao = new TimeSeriesProfileInstanceDao(context);
            TimeSeriesProfileInstance timeseriesProfileInstance = buildTestTimeSeriesProfileInstance(officeId, locationName, keyParameter, parameter1, versionId,
                    dateTimeArray, valueArray, timeZone, versionDate);
            try {
                // test storeTImeSeriesProfileInstance method
                timeSeriesProfileInstanceDao.storeTimeSeriesProfileInstance(timeseriesProfileInstance, versionId, versionDate, storeRule, null);
            } catch (Exception ex) {
                ex.printStackTrace();
                throw (ex);
            }

            // check is the timeseries profile instance can be retrieved
            try {
                timeSeriesProfileInstance = retrieveTimeSeriesProfileInstance(officeId, locationName, keyParameter[0], versionId, unit,
                        startTime, endTime, timeZone, startInclusive, endInclusive, previous, next, versionDate, maxVersion);
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
            // instance exists?
            assertNotNull(timeSeriesProfileInstance);

            // cleanup delete the timeseries profile instance and its timeseries
            boolean overrideProtection = false;
            timeSeriesProfileInstanceDao.deleteTimeSeriesProfileInstance(timeseriesProfileInstance.getTimeSeriesProfile().getLocationId(),
                    timeSeriesProfile.getKeyParameter(), versionId,
                    firstDate, timeZone, overrideProtection, versionDate);
            TimeSeriesIdentifierDescriptorDao timeSeriesDao = new TimeSeriesIdentifierDescriptorDao(context);
            timeSeriesDao.deleteAll(officeId, locationName + "." + keyParameter[0] + ".Inst.0.0." + versionId);
            timeSeriesDao.deleteAll(officeId, locationName + "." + parameter1[0] + ".Inst.0.0." + versionId);
        });
    }

    private TimeSeriesProfileInstance retrieveTimeSeriesProfileInstance(String officeId, String locationName, String keyParameter, String version, String unit,
            Instant startTime, Instant endTime, String timeZone, boolean startInclusive, boolean endInclusive, boolean previous, boolean next,
            Instant versionDate, boolean maxVersion) throws SQLException {
        final TimeSeriesProfileInstance[] result = {null};
        CwmsDatabaseContainer<?> databaseLink = CwmsDataApiSetupCallback.getDatabaseLink();
        databaseLink.connection(c -> {
            DSLContext context = getDslContext(c, databaseLink.getOfficeId());
            TimeSeriesProfileInstanceDao timeSeriesProfileInstanceDao = new TimeSeriesProfileInstanceDao(context);
            CwmsId location = new CwmsId.Builder()
                    .withOfficeId(officeId)
                    .withName(locationName)
                    .build();
            try {
                result[0] = timeSeriesProfileInstanceDao.retrieveTimeSeriesProfileInstance(location, keyParameter, version,
                        unit, startTime, endTime, timeZone, startInclusive ? "T" : "F", endInclusive ? "T" : "F", previous ? "T" : "F", next ? "T" : "F", versionDate,
                        maxVersion ? "T" : "F");
            } catch (Exception ex) {
                ex.printStackTrace();
            }
        });
        return result[0];
    }

    private void storeTimeSeriesProfileInstance(String officeId, String location, String[] keyParameter, String[] parameter1, String version, Instant versionInstant, Instant[] dateTimeArray, double[] valueArray,
            String timeZone) throws SQLException {
        CwmsDatabaseContainer<?> databaseLink = CwmsDataApiSetupCallback.getDatabaseLink();
        databaseLink.connection(c -> {
            DSLContext context = getDslContext(c, databaseLink.getOfficeId());

            /// create an instance for parameter Depth
            TimeSeriesProfileInstanceDao timeSeriesProfileInstanceDao = new TimeSeriesProfileInstanceDao(context);
            TimeSeriesProfileInstance timeseriesProfileInstance = buildTestTimeSeriesProfileInstance(officeId, location, keyParameter, parameter1, version, dateTimeArray, valueArray, timeZone, versionInstant);
            String storeRule = StoreRule.REPLACE_ALL.toString();

            try {
                timeSeriesProfileInstanceDao.storeTimeSeriesProfileInstance(timeseriesProfileInstance, version, versionInstant, storeRule, "F");
            } catch (Exception ex) {
                ex.printStackTrace();
                throw (ex);
            }
        });
    }

    private static TimeSeriesProfileInstance buildTestTimeSeriesProfileInstance(String officeId, String locationName, String[] keyParameterUnit, String[] parameterUnit1, String version,
            Instant[] dateTimeArray, double[] valueArray, String timeZone, Instant versionInstant) {

        TimeSeriesProfile timeSeriesProfile = buildTestTimeSeriesProfile(officeId, locationName, keyParameterUnit[0], parameterUnit1[0]);


        List<TimeValuePair> timeValuePairList = new ArrayList<>();
        for (int i = 0; i < dateTimeArray.length; i++) {
            TimeValuePair timeValuePair = new TimeValuePair.Builder()
                    .withValue(valueArray[i])
                    .withDateTime(dateTimeArray[i])
                    .build();
            timeValuePairList.add(timeValuePair);
        }

        ProfileTimeSeries profileTimeSeries = new ProfileTimeSeries.Builder()
                .withParameter(keyParameterUnit[0])
                .withUnit(keyParameterUnit[1])
                .withTimeZone(timeZone)
                .withTimeValuePairList(timeValuePairList)
                .build();

        List<ProfileTimeSeries> timeSeriesList = new ArrayList<>();
        timeSeriesList.add(profileTimeSeries);
        profileTimeSeries = new ProfileTimeSeries.Builder()
                .withParameter(parameterUnit1[0])
                .withUnit(parameterUnit1[1])
                .withTimeZone(timeZone)
                .withTimeValuePairList(timeValuePairList)
                .build();

        timeSeriesList.add(profileTimeSeries);
        return new TimeSeriesProfileInstance.Builder()
                .withTimeSeriesProfile(timeSeriesProfile)
                .withTimeSeriesList(timeSeriesList)
                .withVersion(version)
                .withVersionDate(versionInstant)
                .build();
    }

    static private TimeSeriesProfile buildTestTimeSeriesProfile(String officeId, String locationName, String keyParameter, String parameter1) {
        CwmsId locationId = new CwmsId.Builder().withOfficeId(officeId).withName(locationName).build();
        return new TimeSeriesProfile.Builder()
                .withLocationId(locationId)
                .withKeyParameter(keyParameter)
                .withParameterList(Arrays.asList(parameter1, keyParameter))
                .withDescription("description")
                .build();

    }

    static private TimeSeriesProfileParser buildTestTimeSeriesProfileParser(String officeId, String location, String[] parameterArray, int[] parameterIndexArray, String[] parameterUnitArray,
            char recordDelimiter, char fieldDelimiter, String timeFormat, String timeZone, int timeField) {
        List<ParameterInfo> parameterInfoList = new ArrayList<>();
        for (int i = 0; i < parameterArray.length; i++) {
            parameterInfoList.add(new ParameterInfo.Builder()
                    .withParameter(parameterArray[i])
                    .withIndex(parameterIndexArray[i])
                    .withUnit(parameterUnitArray[i])
                    .build());
        }

        CwmsId locationId = new CwmsId.Builder().withOfficeId(officeId).withName(location).build();
        return

                new TimeSeriesProfileParser.Builder()
                        .withLocationId(locationId)
                        .withKeyParameter(parameterArray[0])
                        .withRecordDelimiter(recordDelimiter)
                        .withFieldDelimiter(fieldDelimiter)
                        .withTimeFormat(timeFormat)
                        .withTimeZone(timeZone)
                        .withTimeField(timeField)
                        .withTimeInTwoFields(true)
                        .withParameterInfoList(parameterInfoList)
                        .build();
    }
}
