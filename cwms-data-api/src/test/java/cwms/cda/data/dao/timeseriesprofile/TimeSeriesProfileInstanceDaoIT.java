package cwms.cda.data.dao.timeseriesprofile;

import cwms.cda.api.DataApiTestIT;
import cwms.cda.data.dao.StoreRule;
import cwms.cda.data.dto.CwmsId;
import cwms.cda.data.dto.timeseriesprofile.DataColumnInfo;
import cwms.cda.data.dto.timeseriesprofile.ParameterColumnInfo;
import cwms.cda.data.dto.timeseriesprofile.ParameterInfo;
import cwms.cda.data.dto.timeseriesprofile.ParameterInfoColumnar;
import cwms.cda.data.dto.timeseriesprofile.ParameterInfoIndexed;
import cwms.cda.data.dto.timeseriesprofile.TimeSeriesData;
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
import java.util.Map;
import java.util.TreeMap;
import java.util.logging.Level;
import java.util.logging.Logger;

import static cwms.cda.data.dao.DaoTest.getDslContext;
import static org.junit.jupiter.api.Assertions.*;

@Tag("integration")
class TimeSeriesProfileInstanceDaoIT extends DataApiTestIT {
    private static final Logger logger = Logger.getLogger(TimeSeriesProfileInstanceDaoIT.class.getName());

    @Test
    void testStoreTimeSeriesProfileInstanceWithDataBlock() throws SQLException {
        CwmsDatabaseContainer<?> databaseLink = CwmsDataApiSetupCallback.getDatabaseLink();
        databaseLink.connection(c -> {
            String officeId = "SPK";
            String locationName = "Glensboro";
            try {
                createLocation(locationName, true, officeId, "SITE");
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
            String versionId = "VERSION";
            String[] parameterArray = {"Depth", "Pres"};
            int[] parameterIndexArray = {7, 8};
            String[] parameterUnitArray = {"m", "kPa"};
            List<String> unitList = new ArrayList<>();
            unitList.add("m");
            unitList.add("kPa");
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
            boolean maxVersion = false;
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
                TimeSeriesProfileInstance timeSeriesProfileInstance = retrieveTimeSeriesProfileInstance(officeId, locationName, parameterArray[0], versionId, unitList,
                        startTime, endTime, timeZone, startInclusive, endInclusive, previous, next, versionDate, maxVersion, null, 1000);
                // cleanup: delete the instance
                profileDao.deleteTimeSeriesProfile(timeSeriesProfileInstance.getTimeSeriesProfile().getLocationId().getName(), timeSeriesProfileInstance.getTimeSeriesProfile().getKeyParameter(),
                        timeSeriesProfileInstance.getTimeSeriesProfile().getLocationId().getOfficeId());
                // check if the instance contains correct number of parameters and data
                assertEquals(parameterArray.length, timeSeriesProfileInstance.getParameterColumns().size());
                assertEquals(26, timeSeriesProfileInstance.getTimeSeriesList().size());
                assertEquals("Depth", timeSeriesProfileInstance.getParameterColumns().get(0).getParameter());
                assertEquals("m", timeSeriesProfileInstance.getParameterColumns().get(0).getUnit());
                assertEquals("Pres", timeSeriesProfileInstance.getParameterColumns().get(1).getParameter());
                assertEquals("kPa", timeSeriesProfileInstance.getParameterColumns().get(1).getUnit());
                assertEquals(2, timeSeriesProfileInstance.getTimeSeriesList().get(1568033359000L).size());
            } catch (SQLException e) {
               throw new RuntimeException(e);
            }
        }, CwmsDataApiSetupCallback.getWebUser());
    }
    @Test
    void testStoreTimeSeriesProfileInstanceWithDataBlockColumnar() throws SQLException {
        CwmsDatabaseContainer<?> databaseLink = CwmsDataApiSetupCallback.getDatabaseLink();
        databaseLink.connection(c -> {
            String officeId = "SPK";
            String locationName = "Glensboro";
            try {
                createLocation(locationName, true, officeId, "SITE");
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
            String versionId = "VERSION";
            String[] parameterArray = {"Depth", "Pres"};
            int[][] parameterStartEndArray = {{21, 23}, {25, 27}};
            String[] parameterUnitArray = {"m", "kPa"};
            List<String> unitList = new ArrayList<>();
            unitList.add("m");
            unitList.add("kPa");
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
            boolean maxVersion = false;
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
                TimeSeriesProfileInstance timeSeriesProfileInstance = retrieveTimeSeriesProfileInstance(officeId, locationName, parameterArray[0], versionId, unitList,
                        startTime, endTime, timeZone, startInclusive, endInclusive, previous, next, versionDate, maxVersion, null, 1000);
                // cleanup: delete the instance
                profileDao.deleteTimeSeriesProfile(timeSeriesProfileInstance.getTimeSeriesProfile().getLocationId().getName(), timeSeriesProfileInstance.getTimeSeriesProfile().getKeyParameter(),
                        timeSeriesProfileInstance.getTimeSeriesProfile().getLocationId().getOfficeId());

                // check if the instance parameters are the same size as the timeseries parameters we stored
                assertEquals(parameterArray.length, timeSeriesProfileInstance.getParameterColumns().size());
                assertEquals(3, timeSeriesProfileInstance.getTimeSeriesList().size());
                assertEquals("Depth", timeSeriesProfileInstance.getParameterColumns().get(0).getParameter());
                assertEquals("m", timeSeriesProfileInstance.getParameterColumns().get(0).getUnit());
                assertEquals("Pres", timeSeriesProfileInstance.getParameterColumns().get(1).getParameter());
                assertEquals("kPa", timeSeriesProfileInstance.getParameterColumns().get(1).getUnit());
                assertEquals(2, timeSeriesProfileInstance.getTimeSeriesList().get(1568033359000L).size());
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
         }, CwmsDataApiSetupCallback.getWebUser());
    }

    @Test
    void testRetrieveTimeSeriesProfileInstanceWithDataBlockColumnarAndPaging() throws SQLException {
        CwmsDatabaseContainer<?> databaseLink = CwmsDataApiSetupCallback.getDatabaseLink();
        databaseLink.connection(c -> {
            String officeId = "SPK";
            String locationName = "Glensboro";
            try {
                createLocation(locationName, true, officeId, "SITE");
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
            String versionId = "VERSION";
            String[] parameterArray = {"Depth", "Pres"};
            int[][] parameterStartEndArray = {{21, 23}, {25, 27}};
            String[] parameterUnitArray = {"m", "kPa"};
            List<String> unitList = new ArrayList<>();
            unitList.add("m");
            unitList.add("kPa");
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
            boolean maxVersion = false;
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
                // get total number of pages
                TimeSeriesProfileInstance controlInstance = retrieveTimeSeriesProfileInstance(officeId, locationName, parameterArray[0], versionId, unitList,
                        startTime, endTime, timeZone, startInclusive, endInclusive, previous, next, versionDate, maxVersion,
                        null, 1000);

                int total = controlInstance.getTotal();

                // retrieve the time series profile instance we just stored
                TimeSeriesProfileInstance timeSeriesProfileInstance = retrieveTimeSeriesProfileInstance(officeId, locationName, parameterArray[0], versionId, unitList,
                        startTime, endTime, timeZone, startInclusive, endInclusive, previous, next, versionDate, maxVersion, null, total / 2);

                TimeSeriesProfileInstance timeSeriesProfileInstance1 = retrieveTimeSeriesProfileInstance(officeId, locationName, parameterArray[0], versionId, unitList,
                        startTime, endTime, timeZone, startInclusive, endInclusive, previous, next, versionDate, maxVersion, timeSeriesProfileInstance.getNextPage(), total / 2);

                TimeSeriesProfileInstance timeSeriesProfileInstance2 = retrieveTimeSeriesProfileInstance(officeId, locationName, parameterArray[0], versionId, unitList,
                        startTime, endTime, timeZone, startInclusive, endInclusive, previous, next, versionDate, maxVersion, null, 1);

                TimeSeriesProfileInstance timeSeriesProfileInstance3 = retrieveTimeSeriesProfileInstance(officeId, locationName, parameterArray[0], versionId, unitList,
                        startTime, endTime, timeZone, startInclusive, endInclusive, previous, next, versionDate, maxVersion, timeSeriesProfileInstance2.getNextPage(), 1);

                TimeSeriesProfileInstance timeSeriesProfileInstance4 = retrieveTimeSeriesProfileInstance(officeId, locationName, parameterArray[0], versionId, unitList,
                        startTime, endTime, timeZone, startInclusive, endInclusive, previous, next, versionDate, maxVersion, timeSeriesProfileInstance3.getNextPage(), 2);

                // cleanup: delete the instance
                profileDao.deleteTimeSeriesProfile(timeSeriesProfileInstance.getTimeSeriesProfile().getLocationId().getName(), timeSeriesProfileInstance.getTimeSeriesProfile().getKeyParameter(),
                        timeSeriesProfileInstance.getTimeSeriesProfile().getLocationId().getOfficeId());

                // check if the instance parameters are the same size as the timeseries parameters we stored
                assertAll(
                        () -> assertEquals(parameterArray.length, timeSeriesProfileInstance.getParameterColumns().size(),
                                "First instance parameter count does not match"),
                        () -> assertEquals(2, timeSeriesProfileInstance.getTimeSeriesList().size(),
                                "Size of timeseries list does not match"),
                        () -> assertEquals("Depth", timeSeriesProfileInstance.getParameterColumns().get(0).getParameter(),
                                "First instance parameter does not match"),
                        () -> assertEquals("m", timeSeriesProfileInstance.getParameterColumns().get(0).getUnit(),
                                "First unit does not match"),
                        () -> assertEquals("Pres", timeSeriesProfileInstance.getParameterColumns().get(1).getParameter(),
                                "Second instance count of parameters does not match"),
                        () -> assertEquals("kPa", timeSeriesProfileInstance.getParameterColumns().get(1).getUnit(),
                                "Second instance units do not match"),
                        () -> assertThrows(IndexOutOfBoundsException.class, () -> timeSeriesProfileInstance.getParameterColumns().get(2),
                                "Parameter column size too large"),
                        () -> assertEquals(2, timeSeriesProfileInstance.getTimeSeriesList().get(1568033359000L).size(),
                                "First instance timeseries list does not match in size"),
                        () -> assertEquals(2, timeSeriesProfileInstance.getTimeSeriesList().get(1568033347000L).size(),
                                "Second instance timeseries list does not match in size"),
                        () -> assertEquals(parameterArray.length, timeSeriesProfileInstance1.getParameterColumns().size(),
                                "Second instance timeseries list does not match in size"),
                        () -> assertEquals(2, timeSeriesProfileInstance1.getTimeSeriesList().size(),
                                "Second instance timeseries list does not match in size"),
                        () -> assertEquals("Pres", timeSeriesProfileInstance1.getParameterColumns().get(1).getParameter(),
                                "Second instance count of parameters does not match"),
                        () -> assertEquals("kPa", timeSeriesProfileInstance1.getParameterColumns().get(1).getUnit(),
                                "Second instance units do not match"),
                        () -> assertEquals("Depth", timeSeriesProfileInstance1.getParameterColumns().get(0).getParameter(),
                                "First instance parameter does not match"),
                        () -> assertEquals("m", timeSeriesProfileInstance1.getParameterColumns().get(0).getUnit(),
                                "First unit does not match"),
                        () -> assertThrows(IndexOutOfBoundsException.class, () -> timeSeriesProfileInstance1.getParameterColumns().get(2),
                                "Second instance parameter column size too large"),
                        () -> assertEquals(2, timeSeriesProfileInstance1.getTimeSeriesList().get(1568033359000L).size(),
                                "Second instance timeseries list does not match in size"),
                        () -> assertEquals(2, timeSeriesProfileInstance1.getTimeSeriesList().get(1568035040000L).size(),
                                "Second instance timeseries list does not match in size"),
                        () -> assertEquals(2, timeSeriesProfileInstance1.getTimeSeriesList().size(),
                                "Second instance timeseries list does not match expected size."),
                        () -> assertEquals(parameterArray.length, timeSeriesProfileInstance2.getParameterColumns().size(),
                                "Third instance timeseries list does not match in size"),
                        () -> assertEquals(1, timeSeriesProfileInstance2.getTimeSeriesList().size(),
                                "Third instance timeseries list does not match in size"),
                        () -> assertEquals("Depth", timeSeriesProfileInstance2.getParameterColumns().get(0).getParameter(),
                                "Third instance parameter does not match"),
                        () -> assertEquals("m", timeSeriesProfileInstance2.getParameterColumns().get(0).getUnit(),
                                "Third instance unit does not match"),
                        () -> assertEquals(2, timeSeriesProfileInstance2.getTimeSeriesList().get(1568033347000L).size(),
                                "Third instance timeseries list does not match in size"),
                        () -> assertEquals(parameterArray.length, timeSeriesProfileInstance3.getParameterColumns().size(),
                                "Fourth instance timeseries list does not match in size"),
                        () -> assertEquals(1, timeSeriesProfileInstance3.getTimeSeriesList().size(),
                                "Fourth instance timeseries list does not match in size"),
                        () -> assertEquals("Depth", timeSeriesProfileInstance3.getParameterColumns().get(0).getParameter(),
                                "Fourth instance parameter does not match"),
                        () -> assertEquals("m", timeSeriesProfileInstance3.getParameterColumns().get(0).getUnit(),
                                "Fourth instance unit does not match"),
                        () -> assertEquals(2, timeSeriesProfileInstance3.getTimeSeriesList().get(1568033347000L).size(),
                                "Fourth instance timeseries list does not match in size"),
                        () -> assertEquals(parameterArray.length, timeSeriesProfileInstance4.getParameterColumns().size(),
                                "Fifth instance timeseries list does not match in size"),
                        () -> assertEquals(1, timeSeriesProfileInstance4.getTimeSeriesList().size(),
                                "Fifth instance timeseries list does not match in size"),
                        () -> assertEquals("Depth", timeSeriesProfileInstance4.getParameterColumns().get(0).getParameter(),
                                "Fifth instance parameter does not match"),
                        () -> assertEquals("m", timeSeriesProfileInstance4.getParameterColumns().get(0).getUnit(),
                                "Fifth instance unit does not match"),
                        () -> assertEquals(2, timeSeriesProfileInstance4.getTimeSeriesList().get(1568033359000L).size(),
                                "Fifth instance timeseries list does not match in size")
                );
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }, CwmsDataApiSetupCallback.getWebUser());
    }


    @Test
    void testCatalogTimeSeriesProfileInstances() throws SQLException {
        Instant versionDate = Instant.parse("2024-07-09T12:00:00.00Z");
        String officeId = "SPK";
        String location = "Glensboro";
        try {
            createLocation(location, true, officeId, "SITE");
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
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
        }, CwmsDataApiSetupCallback.getWebUser());
    }

    @Test
    void testRetrieveTimeSeriesProfileInstance() throws SQLException {
        String officeId = "SPK";
        String versionID = "VERSION";
        String locationName = "Glensboro";
        try {
            createLocation(locationName, true, officeId, "SITE");
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        String[] keyParameter = {"Depth", "m"};
        String[] parameter1 = {"Pres", "psi"};
        List<String> unitList = new ArrayList<>();
        unitList.add("m");
        unitList.add("bar");
        Instant startTime = Instant.parse("2024-07-09T19:00:11.00Z");
        Instant endTime = Instant.parse("2025-01-01T19:00:22.00Z");
        String timeZone = "UTC";
        boolean startInclusive = true;
        boolean endInclusive = true;
        boolean previous = true;
        boolean next = true;
        boolean maxVersion = false;
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
                    unitList, startTime, endTime, timeZone, startInclusive, endInclusive, previous, next, versionDate,
                    maxVersion, null, 1000);

            assert result != null;

            // cleanup: delete the timeseries we created
            timeSeriesProfileInstanceDao.deleteTimeSeriesProfileInstance(result.getTimeSeriesProfile().getLocationId(),
                        result.getTimeSeriesProfile().getKeyParameter(), result.getVersion(),
                        startTime, timeZone, false, result.getVersionDate());

            // check if the retrieved timeseries profile instance has the same tineseries as the one we stored
            assertEquals(2, result.getTimeSeriesList().size());
            Long time = result.getTimeSeriesList().keySet().iterator().next();
            assertEquals(2, result.getTimeSeriesList().get(time).size());

        }, CwmsDataApiSetupCallback.getWebUser());
    }

    @Test
    void testDeleteTimeSeriesProfileInstance() throws SQLException {
        Instant versionDate = Instant.parse("2024-07-09T12:00:00.00Z");
        String officeId = "SPK";
        String locationName = "Glensboro";
        try {
            createLocation(locationName, true, officeId, "SITE");
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        String[] keyParameter = {"Depth", "m"};
        String[] parameter1 = {"Pres", "psi"};
        List<String> unitList = new ArrayList<>();
        unitList.add("m");
        unitList.add("kPa");
        String version = "VERSION";
        String timeZone = "UTC";
        Instant startTime = Instant.parse("2018-07-09T19:06:20.00Z");
        Instant endTime = Instant.parse("2025-07-09T19:06:20.00Z");
        boolean overrideProtection = false;
        boolean startInclusive = true;
        boolean endInclusive = true;
        boolean previous = true;
        boolean next = true;
        boolean maxVersion = false;
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
                timeSeriesProfileInstance = retrieveTimeSeriesProfileInstance(officeId, locationName, keyParameter[0], version, unitList,
                        startTime, endTime, timeZone, startInclusive, endInclusive, previous, next, versionDate, maxVersion,
                        null, 1000);
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
                timeSeriesProfileInstance = retrieveTimeSeriesProfileInstance(officeId, locationName, keyParameter[0], version, unitList,
                        startTime, endTime, timeZone, startInclusive, endInclusive, previous, next, versionDate, maxVersion,
                null, 1000);
            } catch (SQLException e) {
                throw(new RuntimeException(e));
            }
            // instance does not exist anymore
            assertNull(timeSeriesProfileInstance);
        }, CwmsDataApiSetupCallback.getWebUser());
    }

    @Test
    void testStoreTimeSeriesProfileInstance() throws SQLException {
        String versionId = "VERSION";
        String officeId = "SPK";
        String locationName = "Glensboro";
        try {
            createLocation(locationName, true, officeId, "SITE");
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        String[] parameterArray = {"Depth", "Pres"};
        String[] parameterUnitArray = {"m", "kPa"};
        List<String> unitList = new ArrayList<>();
        unitList.add("m");
        unitList.add("kPa");
        int[] parameterIndexArray = {5, 6};
        String[] keyParameter = {parameterArray[0], parameterUnitArray[0]};
        String[] parameter1 = {parameterArray[1], parameterUnitArray[1]};
        Instant versionDate = Instant.parse("2024-07-09T12:00:00.00Z");
        String timeZone = "UTC";
        Instant startTime = Instant.parse("2018-07-09T19:06:20.00Z");
        Instant endTime = Instant.parse("2025-07-09T19:06:20.00Z");
        Instant firstDate = Instant.parse("2024-07-09T19:00:11.00Z");
        boolean startInclusive = true;
        boolean endInclusive = true;
        boolean previous = true;
        boolean next = true;
        boolean maxVersion = false;
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
                timeSeriesProfileInstance = retrieveTimeSeriesProfileInstance(officeId, locationName, keyParameter[0], versionId, unitList,
                        startTime, endTime, timeZone, startInclusive, endInclusive, previous, next, versionDate, maxVersion, null, 1000);
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

    private TimeSeriesProfileInstance retrieveTimeSeriesProfileInstance(String officeId, String locationName, String keyParameter, String version, List<String> unit,
            Instant startTime, Instant endTime, String timeZone, boolean startInclusive, boolean endInclusive, boolean previous, boolean next,
            Instant versionDate, boolean maxVersion, String page, int pageSize) throws SQLException {
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
                        unit, startTime, endTime, timeZone, startInclusive, endInclusive, previous, next, versionDate,
                        maxVersion, page, pageSize);
            }
            catch(cwms.cda.api.errors.NotFoundException ex)
            {
                logger.log(Level.SEVERE, "TimeSeriesProfileInstance not found");
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

        List<TimeSeriesData> profileTimeSeries = new ArrayList<>();
        profileTimeSeries.add(new TimeSeriesData(timeValuePairList.get(0).getValue(),
                timeValuePairList.get(0).getQuality()));

        List<ParameterColumnInfo> parameterColumnInfoList = new ArrayList<>();
        ParameterColumnInfo paramColumnInfo = new ParameterColumnInfo.Builder()
                .withParameter(keyParameterUnit[0])
                .withOrdinal(1)
                .withUnit(keyParameterUnit[1])
                .build();
        parameterColumnInfoList.add(paramColumnInfo);
        paramColumnInfo = new ParameterColumnInfo.Builder()
                .withParameter(parameterUnit1[0])
                .withOrdinal(2)
                .withUnit(parameterUnit1[1])
                .build();
        parameterColumnInfoList.add(paramColumnInfo);

        List<DataColumnInfo> dataColumnInfoList = new ArrayList<>();
        DataColumnInfo dataColumnInfo = new DataColumnInfo.Builder()
                .withDataType("java.lang.Double")
                .withOrdinal(1)
                .withName(keyParameterUnit[0])
                .build();
        dataColumnInfoList.add(dataColumnInfo);
        dataColumnInfo = new DataColumnInfo.Builder()
                .withDataType("java.lang.Integer")
                .withOrdinal(2)
                .withName(parameterUnit1[0])
                .build();
        dataColumnInfoList.add(dataColumnInfo);

        Map<Long, List<TimeSeriesData>> timeSeriesList = new TreeMap<>();
        timeSeriesList.put(timeValuePairList.get(0).getDateTime().toEpochMilli(), profileTimeSeries);

        profileTimeSeries.add(new TimeSeriesData(timeValuePairList.get(1).getValue(), timeValuePairList.get(1).getQuality()));

        timeSeriesList.put(timeValuePairList.get(1).getDateTime().toEpochMilli(), profileTimeSeries);
        return new TimeSeriesProfileInstance.Builder()
                .withTimeSeriesProfile(timeSeriesProfile)
                .withParameterColumns(parameterColumnInfoList)
                .withTimeSeriesList(timeSeriesList)
                .withDataColumns(dataColumnInfoList)
                .withLocationTimeZone(timeZone)
                .withPageFirstDate(dateTimeArray[0])
                .withPageLastDate(dateTimeArray[1])
                .withFirstDate(dateTimeArray[0])
                .withLastDate(dateTimeArray[1])
                .withPageSize(2)
                .withTotal(3)
                .withTimeSeriesList(timeSeriesList)
                .withVersion(version)
                .withVersionDate(versionInstant)
                .build();
    }

    private static TimeSeriesProfile buildTestTimeSeriesProfile(String officeId, String locationName, String keyParameter, String parameter1) {
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

    private static TimeSeriesProfileParserIndexed buildTestTimeSeriesProfileParserIndexed(String officeId, String location, String[] parameterArray, int[] parameterIndexArray, String[] parameterUnitArray,
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
