package cwms.cda.data.dao;

import cwms.cda.api.DataApiTestIT;
import cwms.cda.api.enums.UnitSystem;
import cwms.cda.api.errors.NotFoundException;
import static cwms.cda.data.dao.DaoTest.getDslContext;
import cwms.cda.data.dto.CwmsId;
import cwms.cda.data.dto.measurement.Measurement;
import cwms.cda.data.dto.measurement.StreamflowMeasurement;
import cwms.cda.data.dto.measurement.SupplementalStreamflowMeasurement;
import cwms.cda.data.dto.measurement.UsgsMeasurement;
import cwms.cda.data.dto.stream.Bank;
import cwms.cda.data.dto.stream.Stream;
import cwms.cda.data.dto.stream.StreamLocation;
import cwms.cda.helpers.DTOMatch;
import fixtures.CwmsDataApiSetupCallback;
import fixtures.TestAccounts;
import java.sql.SQLException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import mil.army.usace.hec.test.database.CwmsDatabaseContainer;
import org.jooq.DSLContext;
import org.junit.jupiter.api.AfterAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag("integration")
final class MeasurementDaoTestIT extends DataApiTestIT {

    private static final String OFFICE_ID = TestAccounts.KeyUser.SPK_NORMAL.getOperatingOffice();
    private static final List<String> STREAM_LOC_IDS = new ArrayList<>();
    private static final List<Stream> STREAMS_CREATED = new ArrayList<>();

    @BeforeAll
    public static void setup() {
        for (int i = 0; i < 2; i++) {
            String testLoc = "STREAM_LOC" + i;
            STREAM_LOC_IDS.add(testLoc);
            try {
                createLocation(testLoc, true, OFFICE_ID, "STREAM_LOCATION");
            } catch (Exception e) {
                //ignore if already exists
            }
            try {
                StreamLocationDaoTestIT.createAndStoreTestStream("TEST_STREAM_123", OFFICE_ID);
            } catch (Exception e) {
                //ignore if already exists
            }
        }
    }

    @AfterAll
    public static void tearDown() {
        for (Stream stream : STREAMS_CREATED) {
            try {
                CwmsDatabaseContainer<?> db = CwmsDataApiSetupCallback.getDatabaseLink();
                String webUser = CwmsDataApiSetupCallback.getWebUser();
                db.connection(c -> {
                    try {
                        StreamDao streamDao = new StreamDao(getDslContext(c, OFFICE_ID));
                        streamDao.deleteStream(stream.getId().getOfficeId(), stream.getId().getName(), DeleteRule.DELETE_ALL);
                    } catch (Exception e) {
                        //ignore
                    }
                }, webUser);
            } catch (SQLException ex) {
                throw new RuntimeException(ex);
            }
        }
        STREAMS_CREATED.clear();
        STREAM_LOC_IDS.clear();
    }

    @Test
    @Disabled
    void testRoundTrip() throws Exception {
        CwmsDatabaseContainer<?> databaseLink = CwmsDataApiSetupCallback.getDatabaseLink();
        String webUser = CwmsDataApiSetupCallback.getWebUser();
        databaseLink.connection(c -> {
            DSLContext context = getDslContext(c, databaseLink.getOfficeId());
            StreamLocationDao streamLocationDao = new StreamLocationDao(context);
            //build stream locations
            String streamLocId = STREAM_LOC_IDS.get(0);
            StreamLocation streamLocation = StreamLocationDaoTestIT.buildTestStreamLocation("TEST_STREAM_123", streamLocId, OFFICE_ID, 10.0, Bank.LEFT);
            String streamLocId2 = STREAM_LOC_IDS.get(1);
            StreamLocation streamLocation2 = StreamLocationDaoTestIT.buildTestStreamLocation("TEST_STREAM_123", streamLocId2, OFFICE_ID, 11.0, Bank.RIGHT);

            try {
                //store stream locations
                streamLocationDao.storeStreamLocation(streamLocation, false);
                streamLocationDao.storeStreamLocation(streamLocation2, false);

                Measurement meas1 = buildMeasurement1(streamLocId);
                Measurement meas1B = buildMeasurement2(streamLocId);

                Measurement meas2 = buildMeasurement1(streamLocId2);

                MeasurementDao measurementDao = new MeasurementDao(context);
                measurementDao.storeMeasurement(meas1, false);
                measurementDao.storeMeasurement(meas1B, false);
                measurementDao.storeMeasurement(meas2, false);

                List<Measurement> measurements = measurementDao.retrieveMeasurements(OFFICE_ID, streamLocId, null, null, UnitSystem.EN.getValue(),
                        null, null, null, null, null, null, null, null);
                assertEquals(2, measurements.size());

                DTOMatch.assertMatch(meas1, measurements.get(0));
                DTOMatch.assertMatch(meas1B, measurements.get(1));

                List<Measurement> measurementsAll = measurementDao.retrieveMeasurements(OFFICE_ID, null, null, null, UnitSystem.EN.getValue(),
                        null, null, null, null, null, null, null, null);
                List<Measurement> meas1List = measurementsAll.stream()
                        .filter(m -> m.getLocationId().equals(streamLocId))
                        .collect(Collectors.toList());
                assertEquals(2, meas1List.size());
                DTOMatch.assertMatch(meas1, meas1List.get(0));
                DTOMatch.assertMatch(meas1B, meas1List.get(1));

                Measurement meas2Found = measurementsAll.stream()
                        .filter(m -> m.getLocationId().equals(streamLocId2))
                        .findFirst()
                        .orElse(null);
                assertNotNull(meas2Found);
                DTOMatch.assertMatch(meas2, meas2Found);

                //test update
                meas1 = buildMeasurement1(streamLocId, 200);
                measurementDao.updateMeasurement(meas1);

                List<Measurement> retrievedMeasurements = measurementDao.retrieveMeasurements(OFFICE_ID, streamLocId, null, null, UnitSystem.EN.getValue(),
                        null, null, null, null, meas1.getNumber(), meas1.getNumber(), null, null);
                DTOMatch.assertMatch(meas1, retrievedMeasurements.get(0));

                Measurement doesntExist = buildMeasurementDoesntExist(streamLocId);
                assertThrows(NotFoundException.class, () -> measurementDao.updateMeasurement(doesntExist));

                //delete measurements
                measurementDao.deleteMeasurements(meas1.getId().getOfficeId(), meas1.getId().getName(), null, null, null, null, null, null, null, null, null, null, null);
                measurementDao.deleteMeasurements(meas2.getId().getOfficeId(), meas2.getId().getName(), null, null, null, null, null, null, null, null, null, null, null);

                final Measurement meas1F  = meas1;
                final Measurement meas2F = meas2;
                assertThrows(NotFoundException.class, () -> measurementDao.retrieveMeasurements(meas1F.getId().getOfficeId(), meas1F.getId().getName(),
                        null, null, UnitSystem.EN.getValue(), null, null, null, null, null, null, null, null));
                assertThrows(NotFoundException.class, () -> measurementDao.retrieveMeasurements(meas2F.getId().getOfficeId(), meas2F.getId().getName(),
                        null, null, UnitSystem.EN.getValue(), null, null, null, null, null, null, null, null));
            } finally {
                //delete stream locations
                streamLocationDao.deleteStreamLocation(
                        streamLocation.getStreamLocationNode().getId().getOfficeId(),
                        streamLocation.getStreamLocationNode().getStreamNode().getStreamId().getName(),
                        streamLocation.getStreamLocationNode().getId().getName()
                );
                streamLocationDao.deleteStreamLocation(
                        streamLocation2.getStreamLocationNode().getId().getOfficeId(),
                        streamLocation2.getStreamLocationNode().getStreamNode().getStreamId().getName(),
                        streamLocation2.getStreamLocationNode().getId().getName()
                );
            }
        }, webUser);
    }

    @Test
    @Disabled
    void testRoundTripMultipleStore() throws Exception {
        CwmsDatabaseContainer<?> databaseLink = CwmsDataApiSetupCallback.getDatabaseLink();
        String webUser = CwmsDataApiSetupCallback.getWebUser();
        databaseLink.connection(c -> {
            DSLContext context = getDslContext(c, databaseLink.getOfficeId());
            StreamLocationDao streamLocationDao = new StreamLocationDao(context);
            //build stream locations
            String streamLocId = STREAM_LOC_IDS.get(0);
            StreamLocation streamLocation = StreamLocationDaoTestIT.buildTestStreamLocation("TEST_STREAM_123", streamLocId, OFFICE_ID,10.0, Bank.LEFT);
            String streamLocId2 = STREAM_LOC_IDS.get(1);
            StreamLocation streamLocation2 = StreamLocationDaoTestIT.buildTestStreamLocation("TEST_STREAM_123", streamLocId2, OFFICE_ID,11.0, Bank.RIGHT);

            try {
                //store stream locations
                streamLocationDao.storeStreamLocation(streamLocation, false);
                streamLocationDao.storeStreamLocation(streamLocation2, false);

                Measurement meas1 = buildMeasurement1(streamLocId);
                Measurement meas1B = buildMeasurement2(streamLocId);

                Measurement meas2 = buildMeasurement1(streamLocId2);

                MeasurementDao measurementDao = new MeasurementDao(context);
                List<Measurement> measurements = new ArrayList<>();
                measurements.add(meas1);
                measurements.add(meas1B);
                measurements.add(meas2);
                measurementDao.storeMeasurements(measurements, false);

                List<Measurement> retrievedMeasurements = measurementDao.retrieveMeasurements(OFFICE_ID, streamLocId, null, null, UnitSystem.EN.getValue(),
                        null, null, null, null, null, null, null, null);
                assertEquals(2, retrievedMeasurements.size());

                DTOMatch.assertMatch(meas1, retrievedMeasurements.get(0));
                DTOMatch.assertMatch(meas1B, retrievedMeasurements.get(1));

                List<Measurement> measurementsAll = measurementDao.retrieveMeasurements(OFFICE_ID, null, null, null, UnitSystem.EN.getValue(),
                        null, null, null, null, null, null, null, null);
                List<Measurement> meas1List = measurementsAll.stream()
                        .filter(m -> m.getLocationId().equals(streamLocId))
                        .collect(Collectors.toList());
                assertEquals(2, meas1List.size());
                DTOMatch.assertMatch(meas1, meas1List.get(0));
                DTOMatch.assertMatch(meas1B, meas1List.get(1));

                Measurement meas2Found = measurementsAll.stream()
                        .filter(m -> m.getLocationId().equals(streamLocId2))
                        .findFirst()
                        .orElse(null);
                assertNotNull(meas2Found);
                DTOMatch.assertMatch(meas2, meas2Found);

                //test update
                meas1 = buildMeasurement1(streamLocId, 400);
                meas1B = buildMeasurement2(streamLocId, 500);
                List<Measurement> updatedMeasurements = new ArrayList<>();
                updatedMeasurements.add(meas1);
                updatedMeasurements.add(meas1B);
                measurementDao.updateMeasurements(updatedMeasurements);

                retrievedMeasurements = measurementDao.retrieveMeasurements(OFFICE_ID, streamLocId, null, null, UnitSystem.EN.getValue(),
                        null, null, null, null, null, null, null, null);

                Measurement finalMeas1 = meas1;
                Measurement finalMeas1B = meas1B;
                Measurement retrievedMeas1 = retrievedMeasurements.stream()
                        .filter(m -> m.getNumber().equals(finalMeas1.getNumber()))
                        .findFirst()
                        .orElse(null);
                assertNotNull(retrievedMeas1);
                DTOMatch.assertMatch(meas1, retrievedMeas1);
                Measurement retrievedMeas1B = retrievedMeasurements.stream()
                        .filter(m -> m.getNumber().equals(finalMeas1B.getNumber()))
                        .findFirst()
                        .orElse(null);
                assertNotNull(retrievedMeas1B);
                DTOMatch.assertMatch(meas1B, retrievedMeas1B);

                Measurement doesntExist = buildMeasurementDoesntExist(streamLocId);
                updatedMeasurements.add(doesntExist);
                assertThrows(NotFoundException.class, () -> measurementDao.updateMeasurements(updatedMeasurements));

                //delete measurements
                measurementDao.deleteMeasurements(meas1.getId().getOfficeId(), meas1.getId().getName(), null, null, null, null, null, null, null, null, null, null, null);
                measurementDao.deleteMeasurements(meas2.getId().getOfficeId(), meas2.getId().getName(), null, null, null, null, null, null, null, null, null, null, null);

                final Measurement meas1F  = meas1;
                final Measurement meas2F = meas2;
                assertThrows(NotFoundException.class, () -> measurementDao.retrieveMeasurements(meas1F.getId().getOfficeId(), meas1F.getId().getName(),
                        null, null, UnitSystem.EN.getValue(), null, null, null, null, null, null, null, null));
                assertThrows(NotFoundException.class, () -> measurementDao.retrieveMeasurements(meas2F.getId().getOfficeId(), meas2F.getId().getName(),
                        null, null, UnitSystem.EN.getValue(), null, null, null, null, null, null, null, null));
            } finally {
                //delete stream locations
                streamLocationDao.deleteStreamLocation(
                        streamLocation.getStreamLocationNode().getId().getOfficeId(),
                        streamLocation.getStreamLocationNode().getStreamNode().getStreamId().getName(),
                        streamLocation.getStreamLocationNode().getId().getName()
                );
                streamLocationDao.deleteStreamLocation(
                        streamLocation2.getStreamLocationNode().getId().getOfficeId(),
                        streamLocation2.getStreamLocationNode().getStreamNode().getStreamId().getName(),
                        streamLocation2.getStreamLocationNode().getId().getName()
                );
            }
        }, webUser);
    }

    private Measurement buildMeasurement1(String streamLocId) {
        return buildMeasurement1(streamLocId, 100.0);
    }

    private Measurement buildMeasurement1(String streamLocId, double flow) {
        return new Measurement.Builder()
                .withNumber("12345")
                .withAgency("USGS")
                .withParty("SomeParty")
                .withInstant(Instant.parse("2024-01-01T00:00:00Z"))
                .withWmComments("Test comment")
                .withAreaUnit("ft2")
                .withFlowUnit("cfs")
                .withHeightUnit("ft")
                .withVelocityUnit("fps")
                .withTempUnit("F")
                .withUsed(true)
                .withId(new CwmsId.Builder()
                        .withName(streamLocId)
                        .withOfficeId(OFFICE_ID)
                        .build())
                .withStreamflowMeasurement(new StreamflowMeasurement.Builder()
                        .withFlow(flow)
                        .withGageHeight(2.0)
                        .withQuality("G")
                        .build())
                .withUsgsMeasurement(new UsgsMeasurement.Builder()
                        .withAirTemp(11.0)
                        .withCurrentRating("1")
                        .withControlCondition("FILL")
                        .withFlowAdjustment("OTHR")
                        .withDeltaHeight(0.5)
                        .withDeltaTime(60.0)
                        .withPercentDifference(10.0)
                        .withRemarks("Some remarks")
                        .withShiftUsed(11.0)
                        .withWaterTemp(15.0)
                        .build())
                .withSupplementalStreamflowMeasurement(new SupplementalStreamflowMeasurement.Builder()
                        .withAvgVelocity(1.5)
                        .withChannelFlow(100.0)
                        .withMeanGage(3.0)
                        .withMaxVelocity(2.0)
                        .withOverbankFlow(50.0)
                        .withOverbankArea(200.0)
                        .withTopWidth(10.0)
                        .withSurfaceVelocity(1.0)
                        .withChannelMaxDepth(5.0)
                        .withMainChannelArea(150.0)
                        .withOverbankMaxDepth(2.0)
                        .withEffectiveFlowArea(75.0)
                        .withCrossSectionalArea(60.0)
                        .build())
                .build();
    }

    private Measurement buildMeasurement2(String streamLocId) {
        return buildMeasurement2(streamLocId, 200.0);
    }

    private Measurement buildMeasurement2(String streamLocId, double flow) {
        //same as buildMeasurement but with different values (same office)
        return new Measurement.Builder()
                .withNumber("54321")
                .withAgency("USGS")
                .withParty("SomeParty2")
                .withInstant(Instant.parse("2024-02-01T00:00:00Z"))
                .withWmComments("Test comment2")
                .withAreaUnit("ft2")
                .withFlowUnit("cfs")
                .withHeightUnit("ft")
                .withVelocityUnit("fps")
                .withTempUnit("F")
                .withUsed(true)
                .withId(new CwmsId.Builder()
                        .withName(streamLocId)
                        .withOfficeId(OFFICE_ID)
                        .build())
                .withStreamflowMeasurement(new StreamflowMeasurement.Builder()
                        .withFlow(flow)
                        .withGageHeight(4.0)
                        .withQuality("G")
                        .build())
                .withUsgsMeasurement(new UsgsMeasurement.Builder()
                        .withAirTemp(26.0)
                        .withCurrentRating("2")
                        .withControlCondition("FILL")
                        .withFlowAdjustment("OTHR")
                        .withDeltaHeight(0.6)
                        .withDeltaTime(61.0)
                        .withPercentDifference(11.0)
                        .withRemarks("Some remarks")
                        .withShiftUsed(12.0)
                        .withWaterTemp(16.0)
                        .build())
                .withSupplementalStreamflowMeasurement(new SupplementalStreamflowMeasurement.Builder()
                        .withAvgVelocity(1.6)
                        .withChannelFlow(101.0)
                        .withMeanGage(3.1)
                        .withMaxVelocity(2.1)
                        .withOverbankFlow(50.1)
                        .withOverbankArea(201.0)
                        .withTopWidth(11.0)
                        .withSurfaceVelocity(1.1)
                        .withChannelMaxDepth(5.1)
                        .withMainChannelArea(150.1)
                        .withOverbankMaxDepth(2.1)
                        .withEffectiveFlowArea(75.1)
                        .withCrossSectionalArea(60.1)
                        .build())
                .build();
    }

    private Measurement buildMeasurementDoesntExist(String streamLocId) {
        return new Measurement.Builder()
                .withNumber("0981273")
                .withAgency("USGS")
                .withParty("SomeParty")
                .withInstant(Instant.parse("2024-01-01T00:00:00Z"))
                .withWmComments("Test comment")
                .withAreaUnit("ft2")
                .withFlowUnit("cfs")
                .withHeightUnit("ft")
                .withVelocityUnit("fps")
                .withTempUnit("F")
                .withUsed(true)
                .withId(new CwmsId.Builder()
                        .withName(streamLocId)
                        .withOfficeId(OFFICE_ID)
                        .build())
                .withStreamflowMeasurement(new StreamflowMeasurement.Builder()
                        .withFlow(100.0)
                        .withGageHeight(2.0)
                        .withQuality("G")
                        .build())
                .withUsgsMeasurement(new UsgsMeasurement.Builder()
                        .withAirTemp(25.0)
                        .withCurrentRating("1")
                        .withControlCondition("FILL")
                        .withFlowAdjustment("OTHR")
                        .withDeltaHeight(0.5)
                        .withDeltaTime(60.0)
                        .withPercentDifference(10.0)
                        .withRemarks("Some remarks")
                        .withShiftUsed(11.0)
                        .withWaterTemp(15.0)
                        .build())
                .withSupplementalStreamflowMeasurement(new SupplementalStreamflowMeasurement.Builder()
                        .withAvgVelocity(1.5)
                        .withChannelFlow(100.0)
                        .withMeanGage(3.0)
                        .withMaxVelocity(2.0)
                        .withOverbankFlow(50.0)
                        .withOverbankArea(200.0)
                        .withTopWidth(10.0)
                        .withSurfaceVelocity(1.0)
                        .withChannelMaxDepth(5.0)
                        .withMainChannelArea(150.0)
                        .withOverbankMaxDepth(2.0)
                        .withEffectiveFlowArea(75.0)
                        .withCrossSectionalArea(60.0)
                        .build())
                .build();
    }
}
