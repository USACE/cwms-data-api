/*
 * MIT License
 *
 * Copyright (c) 2024 Hydrologic Engineering Center
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */
package cwms.cda.data.dao;

import cwms.cda.api.DataApiTestIT;
import cwms.cda.api.errors.NotFoundException;
import static cwms.cda.data.dao.DaoTest.getDslContext;
import cwms.cda.data.dto.CwmsId;
import cwms.cda.data.dto.stream.Bank;
import cwms.cda.data.dto.stream.Stream;
import cwms.cda.data.dto.stream.StreamLocation;
import cwms.cda.data.dto.stream.StreamLocationNode;
import cwms.cda.data.dto.stream.StreamNode;
import cwms.cda.data.dto.stream.StreamReach;
import cwms.cda.helpers.DTOMatch;
import fixtures.CwmsDataApiSetupCallback;
import fixtures.TestAccounts;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import mil.army.usace.hec.test.database.CwmsDatabaseContainer;
import org.jooq.DSLContext;
import org.junit.jupiter.api.AfterAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag("integration")
final class StreamReachDaoTestIT extends DataApiTestIT {

    private static final String OFFICE_ID = TestAccounts.KeyUser.SWT_NORMAL.getOperatingOffice();
    private static final List<String> REACH_IDS = new ArrayList<>();
    private static final List<Stream> STREAMS_CREATED = new ArrayList<>();
    private static final List<StreamLocation> STREAM_locationsCreated = new ArrayList<>();

    @BeforeAll
    public static void setup() throws SQLException {
        for (int i = 0; i < 2; i++) {
            String testLoc = "TEST_REACH" + i;
            createLocation(testLoc, true, OFFICE_ID, "STREAM_REACH");
            REACH_IDS.add(testLoc);
            createLocation(testLoc + "_UP", true, OFFICE_ID, "STREAM_LOCATION");
            createLocation(testLoc + "_DOWN", true, OFFICE_ID, "STREAM_LOCATION");
            String streamId = testLoc + "_STREAM";
            createAndStoreTestStream(streamId);
            createAndStoreTestStreamLocation(testLoc + "_UP", streamId, 25.0);
            createAndStoreTestStreamLocation(testLoc + "_DOWN", streamId, 20.0);
        }
    }

    private static void createAndStoreTestStream(String testStreamId) throws SQLException {
        createLocation(testStreamId, true, OFFICE_ID, "STREAM");
        CwmsDatabaseContainer<?> db = CwmsDataApiSetupCallback.getDatabaseLink();
        db.connection(c-> {
            StreamDao streamDao = new StreamDao(getDslContext(c, OFFICE_ID));
            Stream streamToStore = new Stream.Builder()
                    .withId(new CwmsId.Builder()
                            .withOfficeId(OFFICE_ID)
                            .withName(testStreamId)
                            .build())
                    .withLength(100.0)
                    .withLengthUnits("km")
                    .build();
            STREAMS_CREATED.add(streamToStore);
            streamDao.storeStream(streamToStore, true);
        }, CwmsDataApiSetupCallback.getWebUser());
    }

    private static void createAndStoreTestStreamLocation(String testLoc, String streamId, Double station) throws SQLException {
        CwmsDatabaseContainer<?> db = CwmsDataApiSetupCallback.getDatabaseLink();
        db.connection(c-> {
            StreamLocationDao streamLocationDao = new StreamLocationDao(getDslContext(c, OFFICE_ID));
            StreamLocation streamLoc = new StreamLocation.Builder()
                    .withStreamLocationNode(new StreamLocationNode.Builder()
                            .withId(new CwmsId.Builder()
                                    .withOfficeId(OFFICE_ID)
                                    .withName(testLoc)
                                    .build())
                            .withStreamNode(new StreamNode.Builder()
                                    .withStreamId(new CwmsId.Builder()
                                            .withOfficeId(OFFICE_ID)
                                            .withName(streamId)
                                            .build())
                                    .withBank(Bank.LEFT)
                                    .withStation(station)
                                    .withStationUnits("km")
                                    .build())
                            .build())
                    .build();
            STREAM_locationsCreated.add(streamLoc);
            streamLocationDao.storeStreamLocation(streamLoc, true);
        }, CwmsDataApiSetupCallback.getWebUser());
    }

    @AfterAll
    public static void tearDown() {
        for (Stream stream : STREAMS_CREATED) {
            try {
                CwmsDatabaseContainer<?> db = CwmsDataApiSetupCallback.getDatabaseLink();
                db.connection(c -> {
                    StreamDao streamDao = new StreamDao(getDslContext(c, OFFICE_ID));
                    try {
                        streamDao.deleteStream(stream.getId().getOfficeId(), stream.getId().getName(), DeleteRule.DELETE_ALL);
                    } catch (NotFoundException e) {
                        //ignore
                    }
                }, CwmsDataApiSetupCallback.getWebUser());
            } catch (SQLException ex) {
                throw new RuntimeException(ex);
            }
        }

        STREAMS_CREATED.clear();

        for (StreamLocation streamLocation : STREAM_locationsCreated) {
            try {
                CwmsDatabaseContainer<?> db = CwmsDataApiSetupCallback.getDatabaseLink();
                db.connection(c -> {
                    StreamLocationDao streamLocationDao = new StreamLocationDao(getDslContext(c, OFFICE_ID));
                    try {
                        streamLocationDao.deleteStreamLocation(streamLocation.getStreamLocationNode().getId().getOfficeId(),
                                streamLocation.getStreamId().getName(),
                                streamLocation.getStreamLocationNode().getId().getName());
                    } catch (NotFoundException e) {
                        //ignore
                    }
                }, CwmsDataApiSetupCallback.getWebUser());
            } catch (SQLException ex) {
                throw new RuntimeException(ex);
            }
        }

        STREAM_locationsCreated.clear();
        REACH_IDS.clear();
    }

    @Test
    void testRoundTrip() throws Exception {
        CwmsDatabaseContainer<?> databaseLink = CwmsDataApiSetupCallback.getDatabaseLink();
        databaseLink.connection(c -> {
            DSLContext context = getDslContext(c, databaseLink.getOfficeId());
            StreamReachDao streamReachDao = new StreamReachDao(context);

            String reachId = REACH_IDS.get(0);
            StreamReach reach = buildTestStreamReach(reachId);
            String reachId2 = REACH_IDS.get(1);
            StreamReach reach2 = buildTestStreamReach(reachId2);

            streamReachDao.storeStreamReach(reach, false);
            streamReachDao.storeStreamReach(reach2, false);

            StreamReach retrievedReach = streamReachDao.retrieveStreamReach(reach.getId().getOfficeId(), reach.getStreamId().getName(), reach.getId().getName(), reach.getUpstreamNode().getStreamNode().getStationUnits());
            DTOMatch.assertMatch(reach, retrievedReach);

            StreamReach retrievedReach2 = streamReachDao.retrieveStreamReach(reach2.getId().getOfficeId(), reach2.getStreamId().getName(), reach2.getId().getName(), reach2.getUpstreamNode().getStreamNode().getStationUnits());
            DTOMatch.assertMatch(reach2, retrievedReach2);

            List<StreamReach> retrievedReaches = streamReachDao.retrieveStreamReaches(OFFICE_ID, null, null, null, "km");
            assertFalse(retrievedReaches.isEmpty());
            retrievedReaches = retrievedReaches.stream()
                    .filter(r -> r.getId().getName().equalsIgnoreCase(reach.getId().getName()) || r.getId().getName().equalsIgnoreCase(reach2.getId().getName()))
                    .collect(Collectors.toList());

            assertEquals(2, retrievedReaches.size());
            DTOMatch.assertMatch(reach, retrievedReaches.get(0));
            DTOMatch.assertMatch(reach2, retrievedReaches.get(1));

            streamReachDao.deleteStreamReach(reach.getId().getOfficeId(), reach.getId().getName());
            streamReachDao.deleteStreamReach(reach2.getId().getOfficeId(), reach2.getId().getName());

            CwmsId reach1Id = reach.getId();
            CwmsId reach2Id = reach2.getId();
            String stationUnits1 = reach.getUpstreamNode().getStreamNode().getStationUnits();
            String stationUnits2 = reach2.getUpstreamNode().getStreamNode().getStationUnits();
            assertThrows(NotFoundException.class, () -> streamReachDao.retrieveStreamReach(reach1Id.getOfficeId(), reach.getStreamId().getName(), reach1Id.getName(), stationUnits1));
            assertThrows(NotFoundException.class, () -> streamReachDao.retrieveStreamReach(reach2Id.getOfficeId(), reach.getStreamId().getName(), reach2Id.getName(), stationUnits2));
        }, CwmsDataApiSetupCallback.getWebUser());
    }

    @Test
    void testRename() throws Exception {
        CwmsDatabaseContainer<?> databaseLink = CwmsDataApiSetupCallback.getDatabaseLink();
        databaseLink.connection(c -> {
            DSLContext context = getDslContext(c, databaseLink.getOfficeId());
            StreamReachDao streamReachDao = new StreamReachDao(context);

            String reachId = REACH_IDS.get(0);
            StreamReach reach = buildTestStreamReach(reachId);

            streamReachDao.storeStreamReach(reach, false);
            String originalId = reach.getId().getName();
            String office = reach.getId().getOfficeId();
            String newId = reach.getId().getName() + "Rename";
            streamReachDao.renameStreamReach(office, originalId, newId);
            String streamId = reachId + "_STREAM";

            assertThrows(NotFoundException.class, () -> streamReachDao.retrieveStreamReach(office, streamId, originalId, reach.getUpstreamNode().getStreamNode().getStationUnits()));

            StreamReach retrievedReach = streamReachDao.retrieveStreamReach(office, streamId, newId, reach.getUpstreamNode().getStreamNode().getStationUnits());
            assertEquals(newId, retrievedReach.getId().getName());
            streamReachDao.deleteStreamReach(office, newId);
            LocationsDaoImpl locationDao = new LocationsDaoImpl(getDslContext(c, OFFICE_ID));
            locationDao.deleteLocation(newId, OFFICE_ID);
        }, CwmsDataApiSetupCallback.getWebUser());
    }

    private static StreamReach buildTestStreamReach(String reachId) {
        return new StreamReach.Builder()
                .withId(new CwmsId.Builder()
                        .withName(reachId)
                        .withOfficeId(OFFICE_ID)
                        .build())
                .withDownstreamNode(new StreamLocationNode.Builder()
                        .withId(new CwmsId.Builder()
                                .withName(reachId + "_DOWN")
                                .withOfficeId(OFFICE_ID)
                                .build())
                        .withStreamNode(new StreamNode.Builder()
                                .withStreamId(new CwmsId.Builder()
                                        .withName(reachId + "_STREAM")
                                        .withOfficeId(OFFICE_ID)
                                        .build())
                                .withBank(Bank.LEFT)
                                .withStation(20.0)
                                .withStationUnits("km")
                                .build())
                        .build())
                .withUpstreamNode(new StreamLocationNode.Builder()
                        .withId(new CwmsId.Builder()
                                .withName(reachId + "_UP")
                                .withOfficeId(OFFICE_ID)
                                .build())
                        .withStreamNode(new StreamNode.Builder()
                                .withStreamId(new CwmsId.Builder()
                                        .withName(reachId + "_STREAM")
                                        .withOfficeId(OFFICE_ID)
                                        .build())
                                .withBank(Bank.LEFT)
                                .withStation(25.0)
                                .withStationUnits("km")
                                .build())
                        .build())
                .withStreamId(new CwmsId.Builder()
                        .withName(reachId + "_STREAM")
                        .withOfficeId(OFFICE_ID)
                        .build())
                .withConfigurationId(new CwmsId.Builder()
                        .withName("OTHER")
                        .withOfficeId(OFFICE_ID)
                        .build())
                .withComment("Test Comment")
                .build();
    }
}