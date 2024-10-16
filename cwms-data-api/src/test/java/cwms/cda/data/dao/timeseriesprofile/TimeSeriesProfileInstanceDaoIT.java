package cwms.cda.data.dao.timeseriesprofile;

import cwms.cda.api.DataApiTestIT;
import cwms.cda.data.dao.StoreRule;
import cwms.cda.data.dto.CwmsId;
import cwms.cda.data.dto.timeseriesprofile.ParameterInfo;
import cwms.cda.data.dto.timeseriesprofile.ParameterInfoColumnar;
import cwms.cda.data.dto.timeseriesprofile.ParameterInfoIndexed;
import cwms.cda.data.dto.timeseriesprofile.ProfileTimeSeries;
import cwms.cda.data.dto.timeseriesprofile.TimeSeriesProfile;
import cwms.cda.data.dto.timeseriesprofile.TimeSeriesProfileInstance;
import cwms.cda.data.dto.timeseriesprofile.TimeSeriesProfileParserColumnar;
import cwms.cda.data.dto.timeseriesprofile.TimeSeriesProfileParserIndexed;
import cwms.cda.data.dto.timeseriesprofile.TimeValuePair;
import fixtures.CwmsDataApiSetupCallback;
import mil.army.usace.hec.test.database.CwmsDatabaseContainer;
import org.apache.commons.io.IOUtils;
import org.jooq.DSLContext;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import usace.cwms.db.dao.ifc.ts.CwmsDbTs;
import usace.cwms.db.dao.util.services.CwmsDbServiceLookup;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
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
            String officeId = "LRL";
            String locationName = "Glensboro";
            String versionId = "VERSION";
            String unit = "kPa,m";
            String[] parameterArray = {"Depth", "Pres"};
            int[] parameterIndexArray = {7, 8};
            String[] parameterUnitArray = {"m", "kPa"};
            Instant versionDate = Instant.parse("2024-07-09T12:00:00.00Z");
            Instant startTime = Instant.parse("2018-07-09T19:06:20.00Z");
            Instant endTime = Instant.parse("2025-07-09T19:06:20.00Z");
            String profileData;
            InputStream resource = this.getClass().getResourceAsStream("/cwms/cda/data/dto/timeseriesprofile/timeSeriesProfileData.txt");
            assertNotNull(resource);
            try {
                profileData = IOUtils.toString(resource, StandardCharsets.UTF_8);
             } catch (IOException e) {
                throw new RuntimeException(e);
            }
            String timeZone = "UTC";
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
            TimeSeriesProfileParserIndexed parser = buildTestTimeSeriesProfileParserIndexed(officeId, locationName, parameterArray, parameterIndexArray, parameterUnitArray, recordDelimiter, fieldDelimiter,
                    timeFormat, timeZone, timeField);
            TimeSeriesProfileParserDao timeSeriesProfileParserDao = new TimeSeriesProfileParserDao(context);

             // now store the new one.
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
                profileDao.deleteTimeSeriesProfile(timeSeriesProfileInstance.getTimeSeriesProfile().getLocationId().getName(), timeSeriesProfileInstance.getTimeSeriesProfile().getKeyParameter(),
                        timeSeriesProfileInstance.getTimeSeriesProfile().getLocationId().getOfficeId());
                // check if the instant contains the timeseries we stpred
                assertEquals(parameterArray.length, timeSeriesProfileInstance.getTimeSeriesList().size());
            } catch (SQLException e) {
               throw new RuntimeException(e);
            }
        }, CwmsDataApiSetupCallback.getWebUser());
    }
    @Test
    void testStoreTimeSeriesProfileInstanceWithDataBlockColumnar() throws SQLException {
        CwmsDatabaseContainer<?> databaseLink = CwmsDataApiSetupCallback.getDatabaseLink();
        databaseLink.connection(c -> {
            String officeId = "LRL";
            String locationName = "Glensboro";
            String versionId = "VERSION";
            String unit = "kPa,m";
            String[] parameterArray = {"Depth", "Pres"};
            int[][] parameterStartEndArray = {{21, 23}, {25, 27}};
            String[] parameterUnitArray = {"m", "kPa"};
            Instant versionDate = Instant.parse("2024-07-09T12:00:00.00Z");
            Instant startTime = Instant.parse("2018-07-09T19:06:20.00Z");
            Instant endTime = Instant.parse("2025-07-09T19:06:20.00Z");
            String profileData;
            InputStream resource = this.getClass().getResourceAsStream("/cwms/cda/data/dto/timeseriesprofile/timeSeriesProfileDataColumnar.txt");
            assertNotNull(resource);
            try {
                profileData = IOUtils.toString(resource, StandardCharsets.UTF_8);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            String timeZone = "UTC";
            boolean startInclusive = true;
            boolean endInclusive = true;
            boolean previous = true;
            boolean next = true;
            boolean maxVersion = true;
            char recordDelimiter = '\n';
            int[] timeStartEnd = {1, 19};

            String timeFormat = "MM/DD/YYYY,HH24:MI:SS";

            DSLContext context = getDslContext(c, databaseLink.getOfficeId());

            // store a time series profile
            TimeSeriesProfile timeSeriesProfile = buildTestTimeSeriesProfile(officeId, locationName, parameterArray[0], parameterArray[1]);
            TimeSeriesProfileDao profileDao = new TimeSeriesProfileDao(context);
            profileDao.storeTimeSeriesProfile(timeSeriesProfile, false);

            // store a time series parser
            TimeSeriesProfileParserColumnar parser = buildTestTimeSeriesProfileParserColumnar(officeId, locationName, parameterArray, parameterStartEndArray, parameterUnitArray, recordDelimiter,
                    timeFormat, timeZone, timeStartEnd);
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
                profileDao.deleteTimeSeriesProfile(timeSeriesProfileInstance.getTimeSeriesProfile().getLocationId().getName(), timeSeriesProfileInstance.getTimeSeriesProfile().getKeyParameter(),
                        timeSeriesProfileInstance.getTimeSeriesProfile().getLocationId().getOfficeId());

                // check if the instant contains the timeseries we stpred
                assertEquals(parameterArray.length, timeSeriesProfileInstance.getTimeSeriesList().size());
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
         }, CwmsDataApiSetupCallback.getWebUser());
    }


    @Test
    void testCatalogTimeSeriesProfileInstances() throws SQLException {
        Instant versionDate = Instant.parse("2024-07-09T12:00:00.00Z");
        String officeId = "LRL";
        String location = "Glensboro";
        String[] keyParameter = {"Depth", "m"};
        String[] parameter1 = {"Pres", "psi"};
        String[] versions = {"VERSION", "VERSION2", "VERSION3"};
        String officeIdMask = "*";
        String locationMask = "*";
        String parameterMask = "*";
        String versionMask = "*";
        String timeZone = "UTC";
        Instant firstDate = Instant.parse("2024-07-09T19:00:11.00Z");
        Instant[] dateTimeArray = {Instant.parse("2024-07-09T19:00:11.00Z"), Instant.parse("2024-07-09T20:00:22.00Z")};
        double[] valueArray = {1, 4};

        // store a few timeseries profile instances
        for (String version : versions) {
            storeTimeSeriesProfileInstance(officeId, location, keyParameter, parameter1, version, versionDate, dateTimeArray, valueArray, timeZone);
        }
        CwmsDatabaseContainer<?> databaseLink = CwmsDataApiSetupCallback.getDatabaseLink();

        databaseLink.connection(c -> {
            DSLContext context = getDslContext(c, databaseLink.getOfficeId());
            TimeSeriesProfileInstanceDao timeSeriesProfileInstanceDao = new TimeSeriesProfileInstanceDao(context);
            // test retrieveTimeSeriesProfileInstances
            List<TimeSeriesProfileInstance> result = timeSeriesProfileInstanceDao.catalogTimeSeriesProfileInstances(officeIdMask, locationMask, parameterMask,
                    versionMask);

            // cleanup: delete the time series profile instances we created
            boolean overrideProtection = false;
            for (TimeSeriesProfileInstance timeSeriesProfileInstance : result) {
                timeSeriesProfileInstanceDao.deleteTimeSeriesProfileInstance(timeSeriesProfileInstance.getTimeSeriesProfile().getLocationId(),
                        timeSeriesProfileInstance.getTimeSeriesProfile().getKeyParameter(), timeSeriesProfileInstance.getVersion(),
                        firstDate, timeZone, overrideProtection, timeSeriesProfileInstance.getVersionDate());
                break;
            }
            // check if we retrieve all the instances we stored
            assertEquals(versions.length, result.size(), CwmsDataApiSetupCallback.getWebUser());
        });
    }

    @Test
    void testRetrieveTimeSeriesProfileInstance() throws SQLException {
        String officeId = "LRL";
        String versionID = "VERSION";
        String locationName = "Glensboro";
        String[] keyParameter = {"Depth", "m"};
        String[] parameter1 = {"Pres", "psi"};
        String unit = "bar,m";
        Instant startTime = Instant.parse("2024-07-09T19:00:11.00Z");
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
            timeSeriesProfileInstanceDao.deleteTimeSeriesProfileInstance(result.getTimeSeriesProfile().getLocationId(),
                        result.getTimeSeriesProfile().getKeyParameter(), result.getVersion(),
                        startTime, timeZone, false, result.getVersionDate());

            // check if the retrieved timeseries profile instance has the same tineseries as the one we stored
            assertEquals(2, result.getTimeSeriesList().size());
            assertEquals(2, result.getTimeSeriesList().get(0).getValues().size());

        }, CwmsDataApiSetupCallback.getWebUser());
    }

    @Test
    void testDeleteTimeSeriesProfileInstance() throws SQLException {
        Instant versionDate = Instant.parse("2024-07-09T12:00:00.00Z");
        String officeId = "LRL";
        String locationName = "Glensboro";
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
            try {
                timeSeriesProfileInstance = retrieveTimeSeriesProfileInstance(officeId, locationName, keyParameter[0], version, unit,
                        startTime, endTime, timeZone, startInclusive, endInclusive, previous, next, versionDate, maxVersion);
            } catch (SQLException e) {
                throw(new RuntimeException(e));
            }
            // instance does not exist anymore
            assertNull(timeSeriesProfileInstance);
//
//            // cleanup the timeseries
//            try {
//                CwmsDbTs tsDao = CwmsDbServiceLookup.buildCwmsDb(CwmsDbTs.class, c);
//                tsDao.deleteAll(c, officeId, locationName + "." + keyParameter[0] + ".Inst.0.0." + version);
//                tsDao.deleteAll(c, officeId, locationName + "." + parameter1[0] + ".Inst.0.0." + version);
//            } catch (SQLException e) {
//                throw(new RuntimeException(e));
//            }
        }, CwmsDataApiSetupCallback.getWebUser());
    }

    @Test
    void testStoreTimeSeriesProfileInstance() throws SQLException {
        String versionId = "VERSION";
        String officeId = "LRL";
        String locationName = "Glensboro";
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
            TimeSeriesProfileParserIndexed parser = buildTestTimeSeriesProfileParserIndexed(officeId, locationName, parameterArray, parameterIndexArray, parameterUnitArray, recordDelimiter, fieldDelimiter,
                    timeFormat, timeZone, timeField);
            TimeSeriesProfileParserDao timeSeriesProfileParserDao = new TimeSeriesProfileParserDao(context);
            timeSeriesProfileParserDao.storeTimeSeriesProfileParser(parser, false);

            String storeRule = StoreRule.REPLACE_ALL.toString();
            /// create an instance for parameter Depth
            TimeSeriesProfileInstanceDao timeSeriesProfileInstanceDao = new TimeSeriesProfileInstanceDao(context);
            TimeSeriesProfileInstance timeseriesProfileInstance = buildTestTimeSeriesProfileInstance(officeId, locationName, keyParameter, parameter1, versionId,
                    dateTimeArray, valueArray, timeZone, versionDate);
            // test storeTImeSeriesProfileInstance method
            timeSeriesProfileInstanceDao.storeTimeSeriesProfileInstance(timeseriesProfileInstance, versionId, versionDate, storeRule, null);
             // check is the timeseries profile instance can be retrieved
            try {
                timeSeriesProfileInstance = retrieveTimeSeriesProfileInstance(officeId, locationName, keyParameter[0], versionId, unit,
                        startTime, endTime, timeZone, startInclusive, endInclusive, previous, next, versionDate, maxVersion);
            }
            catch (SQLException e)
            {
                throw new RuntimeException(e);
            }
            // instance exists?

            // cleanup delete the timeseries profile instance and its timeseries
            boolean overrideProtection = false;
            timeSeriesProfileInstanceDao.deleteTimeSeriesProfileInstance(timeseriesProfileInstance.getTimeSeriesProfile().getLocationId(),
                    timeSeriesProfile.getKeyParameter(), versionId,
                    firstDate, timeZone, overrideProtection, versionDate);

            try {
                  CwmsDbTs tsDao = CwmsDbServiceLookup.buildCwmsDb(CwmsDbTs.class, c);
                tsDao.deleteAll(c, officeId, locationName + "." + keyParameter[0] + ".Inst.0.0." + versionId);
                tsDao.deleteAll(c, officeId, locationName + "." + parameter1[0] + ".Inst.0.0." + versionId);
            } catch (SQLException e) {
                  throw new RuntimeException(e);
            }

             assertNotNull(timeSeriesProfileInstance);
        }, CwmsDataApiSetupCallback.getWebUser());
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
            }
            catch(cwms.cda.api.errors.NotFoundException ex)
            {
                // return null for not found
            }
        }, CwmsDataApiSetupCallback.getWebUser());
        return result[0];
    }

    private void storeTimeSeriesProfileInstance(String officeId, String location, String[] keyParameter, String[] parameter1, String version, Instant versionInstant, Instant[] dateTimeArray, double[] valueArray,
            String timeZone) throws SQLException {
        CwmsDatabaseContainer<?> databaseLink = CwmsDataApiSetupCallback.getDatabaseLink();
        databaseLink.connection(c -> {
            DSLContext context = getDslContext(c, databaseLink.getOfficeId());
            TimeSeriesProfile timeSeriesProfile = buildTestTimeSeriesProfile(officeId, location, keyParameter[0], parameter1[0]);
            TimeSeriesProfileDao profileDao = new TimeSeriesProfileDao(context);
            profileDao.storeTimeSeriesProfile(timeSeriesProfile, false);

            /// create an instance for parameter Depth
            TimeSeriesProfileInstanceDao timeSeriesProfileInstanceDao = new TimeSeriesProfileInstanceDao(context);
            TimeSeriesProfileInstance timeseriesProfileInstance = buildTestTimeSeriesProfileInstance(officeId, location, keyParameter, parameter1, version, dateTimeArray, valueArray, timeZone, versionInstant);
            String storeRule = StoreRule.REPLACE_ALL.toString();

            timeSeriesProfileInstanceDao.storeTimeSeriesProfileInstance(timeseriesProfileInstance, version, versionInstant, storeRule, "F");

        }, CwmsDataApiSetupCallback.getWebUser());
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
                .withValues(timeValuePairList)
                .build();

        List<ProfileTimeSeries> timeSeriesList = new ArrayList<>();
        timeSeriesList.add(profileTimeSeries);
        profileTimeSeries = new ProfileTimeSeries.Builder()
                .withParameter(parameterUnit1[0])
                .withUnit(parameterUnit1[1])
                .withTimeZone(timeZone)
                .withValues(timeValuePairList)
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
    private TimeSeriesProfileParserColumnar buildTestTimeSeriesProfileParserColumnar(String officeId, String location, String[] parameterArray, int[][] parameterStartEndArray, String[] parameterUnitArray, char recordDelimiter, String timeFormat, String timeZone, int[] timeStartEnd) {
        List<ParameterInfo> parameterInfoList = new ArrayList<>();
        for (int i = 0; i < parameterArray.length; i++) {
            parameterInfoList.add(new ParameterInfoColumnar.Builder()
                    .withStartColumn(parameterStartEndArray[i][0])
                    .withEndColumn(parameterStartEndArray[i][1])
                    .withUnit(parameterUnitArray[i])
                    .withParameter(parameterArray[i])
                    .build());
        }

        CwmsId locationId = new CwmsId.Builder().withOfficeId(officeId).withName(location).build();
        return (TimeSeriesProfileParserColumnar)

                new TimeSeriesProfileParserColumnar.Builder()
                        .withTimeStartColumn(timeStartEnd[0])
                        .withTimeEndColumn(timeStartEnd[1])
                        .withLocationId(locationId)
                        .withKeyParameter(parameterArray[0])
                        .withRecordDelimiter(recordDelimiter)
                        .withTimeFormat(timeFormat)
                        .withTimeZone(timeZone)

                        .withTimeInTwoFields(false)
                        .withParameterInfoList(parameterInfoList)
                        .build();
    }

    static private TimeSeriesProfileParserIndexed buildTestTimeSeriesProfileParserIndexed(String officeId, String location, String[] parameterArray, int[] parameterIndexArray, String[] parameterUnitArray,
            char recordDelimiter, char fieldDelimiter, String timeFormat, String timeZone, int timeField) {
        List<ParameterInfo> parameterInfoList = new ArrayList<>();
        for (int i = 0; i < parameterArray.length; i++) {
            parameterInfoList.add(new ParameterInfoIndexed.Builder()
                    .withIndex(parameterIndexArray[i])
                    .withParameter(parameterArray[i])
                    .withUnit(parameterUnitArray[i])
                    .build());
        }

        CwmsId locationId = new CwmsId.Builder().withOfficeId(officeId).withName(location).build();
        return (TimeSeriesProfileParserIndexed)

                new TimeSeriesProfileParserIndexed.Builder()
                        .withFieldDelimiter(fieldDelimiter)
                        .withTimeField(timeField)
                        .withLocationId(locationId)
                        .withKeyParameter(parameterArray[0])
                        .withRecordDelimiter(recordDelimiter)
                        .withTimeFormat(timeFormat)
                        .withTimeZone(timeZone)
                        .withTimeInTwoFields(true)
                        .withParameterInfoList(parameterInfoList)
                        .build();
    }
}
