package cwms.cda.data.dao.timeseriesprofile;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import cwms.cda.api.DataApiTestIT;
import cwms.cda.data.dto.CwmsId;
import cwms.cda.data.dto.timeseriesprofile.ParameterInfo;
import cwms.cda.data.dto.timeseriesprofile.TimeSeriesProfile;
import cwms.cda.data.dto.timeseriesprofile.TimeSeriesProfileParser;
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
        CwmsDatabaseContainer<?> databaseLink = CwmsDataApiSetupCallback.getDatabaseLink();
        databaseLink.connection(c -> {
            DSLContext context = getDslContext(c, databaseLink.getOfficeId());

            TimeSeriesProfileDao timeSeriesProfileDao = new TimeSeriesProfileDao(context);

            TimeSeriesProfile timeSeriesProfile = buildTestTimeSeriesProfile("AAA", "Depth");
            timeSeriesProfileDao.storeTimeSeriesProfile(timeSeriesProfile, false);

            timeSeriesProfile = buildTestTimeSeriesProfile("BBB", "Depth");
            timeSeriesProfileDao.storeTimeSeriesProfile(timeSeriesProfile, false);

            TimeSeriesProfileParserDao timeSeriesProfileParserDao = new TimeSeriesProfileParserDao(context);
            TimeSeriesProfileParser timeSeriesProfileParser = buildTestTimeSeriesProfileParser("AAA", "Depth");
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
        });
    }

    @Test
    void testStoreAndDelete() throws SQLException {
        CwmsDatabaseContainer<?> databaseLink = CwmsDataApiSetupCallback.getDatabaseLink();
        databaseLink.connection(c -> {
            DSLContext context = getDslContext(c, databaseLink.getOfficeId());

            TimeSeriesProfileDao timeSeriesProfileDao = new TimeSeriesProfileDao(context);

            TimeSeriesProfile timeSeriesProfile = buildTestTimeSeriesProfile("AAA", "Depth");
            timeSeriesProfileDao.storeTimeSeriesProfile(timeSeriesProfile, false);

            TimeSeriesProfileParserDao timeSeriesProfileParserDao = new TimeSeriesProfileParserDao(context);
            TimeSeriesProfileParser timeSeriesProfileParser = buildTestTimeSeriesProfileParser("AAA", "Depth");
            timeSeriesProfileParserDao.storeTimeSeriesProfileParser(timeSeriesProfileParser, false);

            timeSeriesProfileParserDao.deleteTimeSeriesProfileParser(timeSeriesProfileParser.getLocationId().getName(),
                    timeSeriesProfileParser.getKeyParameter(), timeSeriesProfileParser.getLocationId().getOfficeId());

            assertThrows(Exception.class, () -> timeSeriesProfileParserDao.retrieveTimeSeriesProfileParser(
                    timeSeriesProfileParser.getLocationId().getName(),
                    timeSeriesProfileParser.getKeyParameter(), timeSeriesProfileParser.getLocationId().getOfficeId()));

            timeSeriesProfileDao.deleteTimeSeriesProfile(timeSeriesProfile.getLocationId().getName(), timeSeriesProfile.getKeyParameter(),
                    timeSeriesProfile.getLocationId().getOfficeId());
        });
    }

    @Test
    void testStoreAndRetrieveMultiple() throws SQLException {
        CwmsDatabaseContainer<?> databaseLink = CwmsDataApiSetupCallback.getDatabaseLink();
        databaseLink.connection(c -> {
            DSLContext context = getDslContext(c, databaseLink.getOfficeId());

            TimeSeriesProfileDao timeSeriesProfileDao = new TimeSeriesProfileDao(context);

            TimeSeriesProfile timeSeriesProfileDepth = buildTestTimeSeriesProfile("AAA", "Depth");
            timeSeriesProfileDao.storeTimeSeriesProfile(timeSeriesProfileDepth, false);
            TimeSeriesProfile timeSeriesProfileTemp = buildTestTimeSeriesProfile("BBB", "Temp-Water");
            timeSeriesProfileDao.storeTimeSeriesProfile(timeSeriesProfileTemp, false);

            TimeSeriesProfileParserDao timeSeriesProfileParserDao = new TimeSeriesProfileParserDao(context);
            TimeSeriesProfileParser timeSeriesProfileParser = buildTestTimeSeriesProfileParser("AAA", "Depth");
            timeSeriesProfileParserDao.storeTimeSeriesProfileParser(timeSeriesProfileParser, false);

            timeSeriesProfileParser = buildTestTimeSeriesProfileParser("BBB", "Temp-Water");
            timeSeriesProfileParserDao.storeTimeSeriesProfileParser(timeSeriesProfileParser, false);

            List<TimeSeriesProfileParser> profileParserList = timeSeriesProfileParserDao.retrieveTimeSeriesProfileParsers("*", "*", "*");
            for (TimeSeriesProfileParser profileParser : profileParserList) {
                timeSeriesProfileParserDao.deleteTimeSeriesProfileParser(profileParser.getLocationId().getName(), profileParser.getKeyParameter(), profileParser.getLocationId().getOfficeId());
            }

            timeSeriesProfileDao.deleteTimeSeriesProfile(timeSeriesProfileDepth.getLocationId().getName(), timeSeriesProfileDepth.getKeyParameter(),
                    timeSeriesProfileDepth.getLocationId().getOfficeId());
            timeSeriesProfileDao.deleteTimeSeriesProfile(timeSeriesProfileTemp.getLocationId().getName(), timeSeriesProfileTemp.getKeyParameter(),
                    timeSeriesProfileTemp.getLocationId().getOfficeId());
            assertEquals(2, profileParserList.size());
        });
    }

    @Test
    void testStoreAndCopy() throws SQLException {
        CwmsDatabaseContainer<?> databaseLink = CwmsDataApiSetupCallback.getDatabaseLink();
        databaseLink.connection(c -> {
            DSLContext context = getDslContext(c, databaseLink.getOfficeId());

            TimeSeriesProfileDao timeSeriesProfileDao = new TimeSeriesProfileDao(context);

            TimeSeriesProfile timeSeriesProfile = buildTestTimeSeriesProfile("AAA", "Depth");
            timeSeriesProfileDao.storeTimeSeriesProfile(timeSeriesProfile, false);

            timeSeriesProfile = buildTestTimeSeriesProfile("BBB", "Depth");
            timeSeriesProfileDao.storeTimeSeriesProfile(timeSeriesProfile, false);

            TimeSeriesProfileParserDao timeSeriesProfileParserDao = new TimeSeriesProfileParserDao(context);
            TimeSeriesProfileParser timeSeriesProfileParser = buildTestTimeSeriesProfileParser("AAA", "Depth");
            timeSeriesProfileParserDao.storeTimeSeriesProfileParser(timeSeriesProfileParser, false);

            TimeSeriesProfileParser retrieved = timeSeriesProfileParserDao.retrieveTimeSeriesProfileParser(timeSeriesProfileParser.getLocationId().getName(),
                    timeSeriesProfileParser.getKeyParameter(), timeSeriesProfileParser.getLocationId().getOfficeId());

            timeSeriesProfileParserDao.copyTimeSeriesProfileParser(timeSeriesProfileParser.getLocationId().getName(),
                    timeSeriesProfileParser.getKeyParameter(), timeSeriesProfileParser.getLocationId().getOfficeId(), "BBB");

            TimeSeriesProfileParser retrieved1 = timeSeriesProfileParserDao.retrieveTimeSeriesProfileParser("BBB",
                    timeSeriesProfileParser.getKeyParameter(), timeSeriesProfileParser.getLocationId().getOfficeId());
            assertEquals(timeSeriesProfileParser.getKeyParameter(), retrieved.getKeyParameter());
            assertEquals("BBB", retrieved1.getLocationId().getName());
            assertEquals(timeSeriesProfileParser.getTimeFormat(), retrieved1.getTimeFormat());

        });
    }

    private static TimeSeriesProfile buildTestTimeSeriesProfile(String location, String parameter) {
        String officeId = "HEC";
        CwmsId locationId = new CwmsId.Builder().withOfficeId(officeId).withName(location).build();
        return new TimeSeriesProfile.Builder()
                .withLocationId(locationId)
                .withKeyParameter(parameter)
                .withParameterList(Arrays.asList("Temp-Water", "Depth"))
                .build();

    }

    static private TimeSeriesProfileParser buildTestTimeSeriesProfileParser(String location, String keyParameter) {
        List<ParameterInfo> parameterInfoList = new ArrayList<>();
        parameterInfoList.add(new ParameterInfo.Builder()
                .withParameter("Depth")
                .withIndex(6)
                .withUnit("m")
                .build());
        parameterInfoList.add(new ParameterInfo.Builder()
                .withParameter("Pres")
                .withIndex(5)
                .withUnit("bar")
                .build());
        String officeId = "HEC";
        CwmsId locationId = new CwmsId.Builder().withOfficeId(officeId).withName(location).build();
        return

                new TimeSeriesProfileParser.Builder()
                        .withLocationId(locationId)
                        .withKeyParameter(keyParameter)
                        .withRecordDelimiter((char) 10)
                        .withFieldDelimiter(',')
                        .withTimeFormat("MM/DD/YYYY,HH24:MI:SS")
                        .withTimeZone("UTC")
                        .withTimeField(1)
                        .withTimeInTwoFields(true)
                        .withParameterInfoList(parameterInfoList)
                        .build();
    }


}
