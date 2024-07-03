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
import cwms.cda.api.enums.Nation;
import cwms.cda.api.errors.NotFoundException;
import static cwms.cda.data.dao.DaoTest.getDslContext;
import cwms.cda.data.dto.CwmsId;
import cwms.cda.data.dto.Location;
import cwms.cda.data.dto.stream.Bank;
import cwms.cda.data.dto.stream.Stream;
import cwms.cda.data.dto.stream.StreamLocation;
import cwms.cda.data.dto.stream.StreamLocationNode;
import cwms.cda.data.dto.stream.StreamNode;
import cwms.cda.data.dto.stream.StreamReach;
import cwms.cda.helpers.DTOMatch;
import fixtures.CwmsDataApiSetupCallback;
import fixtures.TestAccounts;
import java.io.IOException;
import java.sql.SQLException;
import java.time.ZoneId;
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
    private static final List<Location> LOCATIONS_CREATED = new ArrayList<>();
    private static final List<String> REACH_IDS = new ArrayList<>();
    private static final List<Stream> STREAMS_CREATED = new ArrayList<>();
    private static final List<StreamLocation> STREAM_LOCATIONS_CREATED = new ArrayList<>();

    @BeforeAll
    public static void setup() {
        try {
            for (int i = 0; i < 2; i++) {
                String testLoc = "LOC" + System.currentTimeMillis() / 10;
                createAndStoreTestLocation(testLoc, "STREAM_REACH");
                REACH_IDS.add(testLoc);
                createAndStoreTestLocation(testLoc + "U", "STREAM_LOCATION");
                createAndStoreTestLocation(testLoc + "D", "STREAM_LOCATION");
                String streamId = testLoc + "S";
                createAndStoreTestStream(streamId);
                createAndStoreTestStreamLocation(testLoc + "U", streamId, 25.0);
                createAndStoreTestStreamLocation(testLoc + "D", streamId, 20.0);
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private static void createAndStoreTestStream(String testStreamId) throws SQLException {
        createAndStoreTestLocation(testStreamId, "STREAM");
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
        });
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
            STREAM_LOCATIONS_CREATED.add(streamLoc);
            streamLocationDao.storeStreamLocation(streamLoc, true);
        });
    }

    @AfterAll
    public static void tearDown() {
        for (Location location : LOCATIONS_CREATED) {
            try {
                CwmsDatabaseContainer<?> db = CwmsDataApiSetupCallback.getDatabaseLink();
                db.connection(c -> {
                    LocationsDao locationsDao = new LocationsDaoImpl(getDslContext(c, OFFICE_ID));
                    locationsDao.deleteLocation(location.getName(), location.getOfficeId(), true);
                });
            } catch (SQLException ex) {
                throw new RuntimeException(ex);
            }
        }
        LOCATIONS_CREATED.clear();

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
                });
            } catch (SQLException ex) {
                throw new RuntimeException(ex);
            }
        }

        STREAMS_CREATED.clear();

        for (StreamLocation streamLocation : STREAM_LOCATIONS_CREATED) {
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
                });
            } catch (SQLException ex) {
                throw new RuntimeException(ex);
            }
        }

        STREAM_LOCATIONS_CREATED.clear();
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
        });
    }

    @Test
    void testRename() throws Exception {
        CwmsDatabaseContainer<?> databaseLink = CwmsDataApiSetupCallback.getDatabaseLink();
        databaseLink.connection(c -> {
            DSLContext context = getDslContext(c, databaseLink.getOfficeId());
            StreamReachDao streamReachDao = new StreamReachDao(context);

            String reachId = LOCATIONS_CREATED.get(0).getName();
            StreamReach reach = buildTestStreamReach(reachId);

            streamReachDao.storeStreamReach(reach, false);
            String originalId = reach.getId().getName();
            String office = reach.getId().getOfficeId();
            String newId = reach.getId().getName() + "N";
            streamReachDao.renameStreamReach(office, originalId, newId);
            String streamId = reachId + "S";

            assertThrows(NotFoundException.class, () -> streamReachDao.retrieveStreamReach(office, streamId, originalId, reach.getUpstreamNode().getStreamNode().getStationUnits()));

            StreamReach retrievedReach = streamReachDao.retrieveStreamReach(office, streamId, newId, reach.getUpstreamNode().getStreamNode().getStationUnits());
            assertEquals(newId, retrievedReach.getId().getName());

            streamReachDao.deleteStreamReach(office, newId);
        });
    }

    private static StreamReach buildTestStreamReach(String reachId) {
        return new StreamReach.Builder()
                .withId(new CwmsId.Builder()
                        .withName(reachId)
                        .withOfficeId(OFFICE_ID)
                        .build())
                .withDownstreamNode(new StreamLocationNode.Builder()
                        .withId(new CwmsId.Builder()
                                .withName(reachId + "D")
                                .withOfficeId(OFFICE_ID)
                                .build())
                        .withStreamNode(new StreamNode.Builder()
                                .withStreamId(new CwmsId.Builder()
                                        .withName(reachId + "S")
                                        .withOfficeId(OFFICE_ID)
                                        .build())
                                .withBank(Bank.LEFT)
                                .withStation(20.0)
                                .withStationUnits("km")
                                .build())
                        .build())
                .withUpstreamNode(new StreamLocationNode.Builder()
                        .withId(new CwmsId.Builder()
                                .withName(reachId + "U")
                                .withOfficeId(OFFICE_ID)
                                .build())
                        .withStreamNode(new StreamNode.Builder()
                                .withStreamId(new CwmsId.Builder()
                                        .withName(reachId + "S")
                                        .withOfficeId(OFFICE_ID)
                                        .build())
                                .withBank(Bank.LEFT)
                                .withStation(25.0)
                                .withStationUnits("km")
                                .build())
                        .build())
                .withStreamId(new CwmsId.Builder()
                        .withName(reachId + "S")
                        .withOfficeId(OFFICE_ID)
                        .build())
                .withConfigurationId(new CwmsId.Builder()
                        .withName("OTHER")
                        .withOfficeId(OFFICE_ID)
                        .build())
                .withComment("Test Comment")
                .build();
    }

    private static void createAndStoreTestLocation(String locationName, String kind) throws SQLException {
        CwmsDatabaseContainer<?> db = CwmsDataApiSetupCallback.getDatabaseLink();
        Location locationForReach = new Location.Builder(locationName,
                kind,
                ZoneId.of("UTC"),
                0.0,
                0.0,
                "WGS84",
                OFFICE_ID)
                .withActive(true)
                .withCountyName("Douglas")
                .withNation(Nation.US)
                .withStateInitial("CO")
                .build();
        if (LOCATIONS_CREATED.contains(locationForReach)) {
            return;
        }
        db.connection(c -> {
            try {
                LocationsDaoImpl locationsDao = new LocationsDaoImpl(getDslContext(c, OFFICE_ID));
                locationsDao.storeLocation(locationForReach);
                LOCATIONS_CREATED.add(locationForReach);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
    }
}