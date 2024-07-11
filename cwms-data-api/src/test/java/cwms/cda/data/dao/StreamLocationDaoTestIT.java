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
final class StreamLocationDaoTestIT extends DataApiTestIT {

    private static final String OFFICE_ID = TestAccounts.KeyUser.SWT_NORMAL.getOperatingOffice();
    private static final List<String> STREAM_LOC_IDS = new ArrayList<>();
    private static final List<Stream> STREAMS_CREATED = new ArrayList<>();

    @BeforeAll
    public static void setup() throws SQLException {
        for (int i = 0; i < 2; i++) {
            String testLoc = "STREAM_LOC" + i;
            STREAM_LOC_IDS.add(testLoc);
            createLocation(testLoc, true, OFFICE_ID, "STREAM_LOCATION");
            createAndStoreTestStream(testLoc + "_STREAM");
        }
    }

    private static void createAndStoreTestStream(String testLoc) throws SQLException {
        createLocation(testLoc, true, OFFICE_ID, "STREAM");
        CwmsDatabaseContainer<?> db = CwmsDataApiSetupCallback.getDatabaseLink();
        String webUser = CwmsDataApiSetupCallback.getWebUser();
        db.connection(c-> {
            StreamDao streamDao = new StreamDao(getDslContext(c, OFFICE_ID));
            Stream streamToStore = new Stream.Builder()
                    .withId(new CwmsId.Builder()
                            .withOfficeId(OFFICE_ID)
                            .withName(testLoc)
                            .build())
                    .withLength(100.0)
                    .withLengthUnits("km")
                    .build();
            STREAMS_CREATED.add(streamToStore);
            streamDao.storeStream(streamToStore, true);
        }, webUser);
    }

    @AfterAll
    public static void tearDown() {
        for(Stream stream : STREAMS_CREATED){
            try {
                CwmsDatabaseContainer<?> db = CwmsDataApiSetupCallback.getDatabaseLink();
                String webUser = CwmsDataApiSetupCallback.getWebUser();
                db.connection(c-> {
                    try
                    {
                        StreamDao streamDao = new StreamDao(getDslContext(c, OFFICE_ID));
                        streamDao.deleteStream(stream.getId().getOfficeId(), stream.getId().getName(), DeleteRule.DELETE_ALL);
                    } catch (Exception e) {
                        //ignore
                    }
                }, webUser);
            } catch(SQLException ex) {
                throw new RuntimeException(ex);
            }
        }
        STREAMS_CREATED.clear();
        STREAM_LOC_IDS.clear();
    }

    @Test
    void testRoundTrip() throws Exception {
        CwmsDatabaseContainer<?> databaseLink = CwmsDataApiSetupCallback.getDatabaseLink();
        String webUser = CwmsDataApiSetupCallback.getWebUser();
        databaseLink.connection(c -> {
            DSLContext context = getDslContext(c, databaseLink.getOfficeId());
            StreamLocationDao streamLocationDao = new StreamLocationDao(context);
            //build stream locations
            String streamLocId = STREAM_LOC_IDS.get(0);
            StreamLocation streamLocation = buildTestStreamLocation(streamLocId+"_STREAM", streamLocId, 10.0, Bank.LEFT);
            String streamLocId2 = STREAM_LOC_IDS.get(1);
            StreamLocation streamLocation2 = buildTestStreamLocation(streamLocId2+"_STREAM", streamLocId2, 11.0, Bank.RIGHT);

            //store stream locations
            streamLocationDao.storeStreamLocation(streamLocation, false);
            streamLocationDao.storeStreamLocation(streamLocation2, false);

            //retrieve stream locations individually
            StreamLocation retrievedStreamLocation = streamLocationDao.retrieveStreamLocation(
                    streamLocation.getStreamLocationNode().getId().getOfficeId(),
                    streamLocation.getStreamLocationNode().getStreamNode().getStreamId().getName(),
                    streamLocId,
                    streamLocation.getStationUnits(),
                    streamLocation.getStageUnits(),
                    streamLocation.getAreaUnits()
            );
            DTOMatch.assertMatch(streamLocation, retrievedStreamLocation);

            StreamLocation retrievedStreamLocation2 = streamLocationDao.retrieveStreamLocation(
                    streamLocation2.getStreamLocationNode().getId().getOfficeId(),
                    streamLocation2.getStreamLocationNode().getStreamNode().getStreamId().getName(),
                    streamLocation2.getStreamLocationNode().getId().getName(),
                    streamLocation2.getStationUnits(),
                    streamLocation2.getStageUnits(),
                    streamLocation2.getAreaUnits()
            );
            DTOMatch.assertMatch(streamLocation2, retrievedStreamLocation2);

            //also test retrieve in bulk
            List<StreamLocation> retrievedStreamLocations = streamLocationDao.retrieveStreamLocations(
                    null, null, "km", "m", "km2", OFFICE_ID
            );
            assertFalse(retrievedStreamLocations.isEmpty());
            retrievedStreamLocations = retrievedStreamLocations.stream()
                    .filter(s -> s.getStreamLocationNode().getStreamNode().getStreamId().getName().equalsIgnoreCase(streamLocation.getStreamLocationNode().getStreamNode().getStreamId().getName())
                            || s.getStreamLocationNode().getStreamNode().getStreamId().getName().equalsIgnoreCase(streamLocation2.getStreamLocationNode().getStreamNode().getStreamId().getName()))
                    .collect(Collectors.toList());

            assertEquals(2, retrievedStreamLocations.size());
            DTOMatch.assertMatch(streamLocation, retrievedStreamLocations.get(0));
            DTOMatch.assertMatch(streamLocation2, retrievedStreamLocations.get(1));

            StreamLocation updatedStreamLocation = new StreamLocation.Builder()
                    //update the values of the stream location
                    .withStreamLocationNode(streamLocation.getStreamLocationNode())
                    .withPublishedStation(20.0)
                    .withNavigationStation(21.0)
                    .withLowestMeasurableStage(19.0)
                    .withTotalDrainageArea(200.0)
                    .withUngagedDrainageArea(20.0)
                    .withAreaUnits("km2")
                    .withStageUnits("m")
                    .build();
            streamLocationDao.updateStreamLocation(updatedStreamLocation);

            StreamLocation retrievedStreamLocationThatWasUpdated = streamLocationDao.retrieveStreamLocation(
                    streamLocation.getStreamLocationNode().getId().getOfficeId(),
                    streamLocation.getStreamLocationNode().getStreamNode().getStreamId().getName(),
                    streamLocation.getStreamLocationNode().getId().getName(),
                    streamLocation.getStationUnits(),
                    streamLocation.getStageUnits(),
                    streamLocation.getAreaUnits()
            );

            DTOMatch.assertMatch(updatedStreamLocation, retrievedStreamLocationThatWasUpdated);

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

            assertThrows(NotFoundException.class, () -> streamLocationDao.retrieveStreamLocation(
                    streamLocation.getStreamLocationNode().getId().getOfficeId(),
                    streamLocation.getStreamLocationNode().getStreamNode().getStreamId().getName(),
                    streamLocation.getStreamLocationNode().getId().getName(),
                    streamLocation.getStationUnits(),
                    streamLocation.getStageUnits(),
                    streamLocation.getAreaUnits()
            ));

            assertThrows(NotFoundException.class, () -> streamLocationDao.retrieveStreamLocation(
                    streamLocation2.getStreamLocationNode().getId().getOfficeId(),
                    streamLocation2.getStreamLocationNode().getStreamNode().getStreamId().getName(),
                    streamLocation2.getStreamLocationNode().getId().getName(),
                    streamLocation2.getStationUnits(),
                    streamLocation2.getStageUnits(),
                    streamLocation2.getAreaUnits()
            ));
        }, webUser);
    }

    private static StreamLocation buildTestStreamLocation(String streamId, String locationId, double station, Bank bank) {
        StreamLocationNode streamLocationNode = new StreamLocationNode.Builder()
                .withId(new CwmsId.Builder()
                        .withName(locationId)
                        .withOfficeId(OFFICE_ID)
                        .build())
                .withStreamNode(new StreamNode.Builder()
                        .withStreamId(new CwmsId.Builder()
                                .withName(streamId)
                                .withOfficeId(OFFICE_ID)
                                .build())
                        .withStation(station)
                        .withBank(bank)
                        .withStationUnits("km")
                        .build())
                .build();
        return new StreamLocation.Builder()
                .withStreamLocationNode(streamLocationNode)
                .withPublishedStation(station)
                .withNavigationStation(station + 1)
                .withLowestMeasurableStage(station - 1)
                .withTotalDrainageArea(100.0)
                .withUngagedDrainageArea(10.0)
                .withAreaUnits("km2")
                .withStageUnits("m")
                .build();
    }
}