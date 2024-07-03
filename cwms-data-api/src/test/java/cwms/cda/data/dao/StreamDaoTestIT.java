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
import cwms.cda.data.dto.stream.StreamNode;
import fixtures.CwmsDataApiSetupCallback;
import fixtures.TestAccounts;
import cwms.cda.helpers.DTOMatch;
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
final class StreamDaoTestIT extends DataApiTestIT {

    private static final String OFFICE_ID = TestAccounts.KeyUser.SWT_NORMAL.getOperatingOffice();
    private static final List<Location> LOCATIONS_CREATED = new ArrayList<>();


    @BeforeAll
    public static void setup() {
        try {
            for (int i = 0; i < 2; i++) {
                String testLoc = "STREAM_ID_" + System.currentTimeMillis()/10;
                createAndStoreTestLocation(testLoc);
                createAndStoreTestLocation("A" + testLoc);
                createAndStoreTestLocation("B" + testLoc);
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @AfterAll
    public static void tearDown() {
        for(Location location : LOCATIONS_CREATED){
            try {
                CwmsDatabaseContainer<?> db = CwmsDataApiSetupCallback.getDatabaseLink();
                db.connection(c-> {
                    LocationsDao locationsDao = new LocationsDaoImpl(getDslContext(c, OFFICE_ID));
                    locationsDao.deleteLocation(location.getName(), location.getOfficeId(), true);
                });
            } catch(SQLException ex) {
                throw new RuntimeException(ex);
            }
        }
        LOCATIONS_CREATED.clear();
    }

    @Test
    void testRoundTrip() throws Exception {
        CwmsDatabaseContainer<?> databaseLink = CwmsDataApiSetupCallback.getDatabaseLink();
        databaseLink.connection(c -> {
            DSLContext context = getDslContext(c, databaseLink.getOfficeId());
            StreamDao streamDao = new StreamDao(context);
            //build streams with their flows into and diverts from nodes
            String streamId = LOCATIONS_CREATED.get(0).getName();
            Stream stream = buildTestStream(streamId, buildStreamNode("A" + streamId, 10.0, Bank.LEFT),
                    buildStreamNode("B" + streamId, 20.0, Bank.RIGHT));
            String streamId2 = LOCATIONS_CREATED.get(3).getName();
            Stream stream2 = buildTestStream(streamId2, buildStreamNode("A" + streamId2, 11.0, Bank.LEFT),
                    buildStreamNode("B" + streamId2, 21.0, Bank.RIGHT));
            //store tributaries/confluence streams first
            Stream flowsIntoStream = buildTestStream("A" + streamId, null, null);
            streamDao.storeStream(flowsIntoStream, false);
            Stream divertsFromStream = buildTestStream("B" + streamId, null, null);
            streamDao.storeStream(divertsFromStream, false);
            Stream flowsIntoStream2 = buildTestStream("A" + streamId2, null, null);
            streamDao.storeStream(flowsIntoStream2, false);
            Stream divertsFromStream2 = buildTestStream("B" + streamId2, null, null);
            streamDao.storeStream(divertsFromStream2, false);

            //store main streams
            streamDao.storeStream(stream, false);
            streamDao.storeStream(stream2, false);

            //retrieve streams individually
            Stream retrievedStream = streamDao.retrieveStream(stream.getId().getOfficeId(), stream.getId().getName(), stream.getLengthUnits());
            DTOMatch.assertMatch(stream, retrievedStream);
            Stream retrievedStream2 = streamDao.retrieveStream(stream2.getId().getOfficeId(), stream2.getId().getName(), stream2.getLengthUnits());
            DTOMatch.assertMatch(stream2, retrievedStream2);
            //also test retrieve in bulk
            List<Stream> retrievedStreams = streamDao.retrieveStreams(OFFICE_ID, null, null, "km");
            assertFalse(retrievedStreams.isEmpty());
            retrievedStreams = retrievedStreams.stream()
                    .filter(s -> s.getId().getName().equalsIgnoreCase(stream.getId().getName()) || s.getId().getName().equalsIgnoreCase(stream2.getId().getName()))
                    .collect(Collectors.toList());

            assertEquals(2, retrievedStreams.size());
            DTOMatch.assertMatch(stream, retrievedStreams.get(0));
            DTOMatch.assertMatch(stream2, retrievedStreams.get(1));
            //delete streams
            streamDao.deleteStream(stream.getId().getOfficeId(), stream.getId().getName(), DeleteRule.DELETE_ALL);
            streamDao.deleteStream(stream2.getId().getOfficeId(), stream2.getId().getName(), DeleteRule.DELETE_ALL);
            assertThrows(NotFoundException.class, () -> streamDao.retrieveStream(stream.getId().getOfficeId(), stream.getId().getName(), stream.getLengthUnits()));
            assertThrows(NotFoundException.class, () -> streamDao.retrieveStream(stream2.getId().getOfficeId(), stream2.getId().getName(), stream.getLengthUnits()));
            streamDao.deleteStream(flowsIntoStream.getId().getOfficeId(), flowsIntoStream.getId().getName(), DeleteRule.DELETE_ALL);
            streamDao.deleteStream(divertsFromStream.getId().getOfficeId(), divertsFromStream.getId().getName(), DeleteRule.DELETE_ALL);
        });
    }

    @Test
    void testRename() throws Exception {
        CwmsDatabaseContainer<?> databaseLink = CwmsDataApiSetupCallback.getDatabaseLink();
        databaseLink.connection(c -> {
            DSLContext context = getDslContext(c, databaseLink.getOfficeId());
            StreamDao streamDao = new StreamDao(context);
            String streamId = LOCATIONS_CREATED.get(0).getName();
            Stream stream = buildTestStream(streamId, buildStreamNode("A" + streamId, 10.0, Bank.LEFT),
                    buildStreamNode("B" + streamId, 20.0, Bank.RIGHT));
            Stream flowsIntoStream = buildTestStream("A" + streamId, null, null);
            streamDao.storeStream(flowsIntoStream, false);
            Stream divertsFromStream = buildTestStream("B" + streamId, null, null);
            streamDao.storeStream(divertsFromStream, false);
            streamDao.storeStream(stream, false);
            String originalId = stream.getId().getName();
            String office = stream.getId().getOfficeId();
            String newId = stream.getId().getName() + "N";
            streamDao.renameStream(office, originalId, newId);
            assertThrows(NotFoundException.class, () -> streamDao.retrieveStream(office, originalId, stream.getLengthUnits()));
            Stream retrievedStream = streamDao.retrieveStream(office, newId, stream.getLengthUnits());
            assertEquals(newId, retrievedStream.getId().getName());
            streamDao.deleteStream(office, newId, DeleteRule.DELETE_ALL);
            streamDao.deleteStream(flowsIntoStream.getId().getOfficeId(), flowsIntoStream.getId().getName(), DeleteRule.DELETE_ALL);
            streamDao.deleteStream(divertsFromStream.getId().getOfficeId(), divertsFromStream.getId().getName(), DeleteRule.DELETE_ALL);
        });
    }

    private static Stream buildTestStream(String streamId, StreamNode flowsIntoNode, StreamNode divertsFromNode) {
        return new Stream.Builder()
                .withStartsDownstream(true)
                .withId(new CwmsId.Builder()
                        .withName(streamId)
                        .withOfficeId(OFFICE_ID)
                        .build())
                .withFlowsIntoStreamNode(flowsIntoNode)
                .withDivertsFromStreamNode(divertsFromNode)
                .withLength(100.0)
                .withAverageSlope(5.0)
                .withComment("Test Comment")
                .withLengthUnits("km")
                .withSlopeUnits("%")
                .build();
    }

    private static StreamNode buildStreamNode(String streamId, double station, Bank bank) {
        return new StreamNode.Builder()
                .withStreamId(new CwmsId.Builder()
                        .withName(streamId)
                        .withOfficeId(OFFICE_ID)
                        .build())
                .withStation(station)
                .withBank(bank)
                .withStationUnits("km")
                .build();
    }

    private static void createAndStoreTestLocation(String locationName) throws SQLException {
        CwmsDatabaseContainer<?> db = CwmsDataApiSetupCallback.getDatabaseLink();
        Location locationForStream = new Location.Builder(locationName,
                "STREAM",
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
        if(LOCATIONS_CREATED.contains(locationForStream)){
            return;
        }
        db.connection(c -> {
            try {
                LocationsDaoImpl locationsDao = new LocationsDaoImpl(getDslContext(c, OFFICE_ID));
                locationsDao.storeLocation(locationForStream);
                LOCATIONS_CREATED.add(locationForStream);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
    }
}