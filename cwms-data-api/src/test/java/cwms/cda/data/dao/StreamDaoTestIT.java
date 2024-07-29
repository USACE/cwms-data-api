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
import cwms.cda.data.dto.stream.StreamNode;
import fixtures.CwmsDataApiSetupCallback;
import fixtures.TestAccounts;
import cwms.cda.helpers.DTOMatch;
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
final class StreamDaoTestIT extends DataApiTestIT {

    private static final String OFFICE_ID = TestAccounts.KeyUser.SWT_NORMAL.getOperatingOffice();
    private static final List<String> STREAM_IDS = new ArrayList<>();

    @BeforeAll
    public static void setup() throws SQLException {
        for (int i = 0; i < 2; i++) {
            String testLoc = "TEST_STREAM" + i;
            createLocation(testLoc, true, OFFICE_ID, "STREAM");
            STREAM_IDS.add(testLoc);
            createLocation("INTO_" + testLoc, true, OFFICE_ID, "STREAM");
            STREAM_IDS.add("INTO_" + testLoc);
            createLocation("FROM_" + testLoc, true, OFFICE_ID, "STREAM");
            STREAM_IDS.add("FROM_" + testLoc);
        }
    }

    @AfterAll
    public static void tearDown() {
      STREAM_IDS.clear();
    }

    @Test
    void testRoundTrip() throws Exception {
        CwmsDatabaseContainer<?> databaseLink = CwmsDataApiSetupCallback.getDatabaseLink();
        databaseLink.connection(c -> {
            DSLContext context = getDslContext(c, databaseLink.getOfficeId());
            StreamDao streamDao = new StreamDao(context);
            //build streams with their flows into and diverts from nodes
            String streamId = STREAM_IDS.get(0);
            Stream stream = buildTestStream(streamId, buildStreamNode("INTO_" + streamId, 10.0, Bank.LEFT),
                    buildStreamNode("FROM_" + streamId, 20.0, Bank.RIGHT));
            String streamId2 = STREAM_IDS.get(3);
            Stream stream2 = buildTestStream(streamId2, buildStreamNode("INTO_" + streamId2, 11.0, Bank.LEFT),
                    buildStreamNode("FROM_" + streamId2, 21.0, Bank.RIGHT));
            //store tributaries/confluence streams first
            Stream flowsIntoStream = buildTestStream("INTO_" + streamId, null, null);
            streamDao.storeStream(flowsIntoStream, false);
            Stream divertsFromStream = buildTestStream("FROM_" + streamId, null, null);
            streamDao.storeStream(divertsFromStream, false);
            Stream flowsIntoStream2 = buildTestStream("INTO_" + streamId2, null, null);
            streamDao.storeStream(flowsIntoStream2, false);
            Stream divertsFromStream2 = buildTestStream("FROM_" + streamId2, null, null);
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
            List<Stream> retrievedStreams = streamDao.retrieveStreams(OFFICE_ID, null,null, null, "km");
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
        }, CwmsDataApiSetupCallback.getWebUser());
    }

    @Test
    void testRename() throws Exception {
        CwmsDatabaseContainer<?> databaseLink = CwmsDataApiSetupCallback.getDatabaseLink();
        databaseLink.connection(c -> {
            DSLContext context = getDslContext(c, databaseLink.getOfficeId());
            StreamDao streamDao = new StreamDao(context);
            String streamId = STREAM_IDS.get(0);
            Stream stream = buildTestStream(streamId, buildStreamNode("INTO_" + streamId, 10.0, Bank.LEFT),
                    buildStreamNode("FROM_" + streamId, 20.0, Bank.RIGHT));
            Stream flowsIntoStream = buildTestStream("INTO_" + streamId, null, null);
            streamDao.storeStream(flowsIntoStream, false);
            Stream divertsFromStream = buildTestStream("FROM_" + streamId, null, null);
            streamDao.storeStream(divertsFromStream, false);
            streamDao.storeStream(stream, false);
            String originalId = stream.getId().getName();
            String office = stream.getId().getOfficeId();
            String newId = stream.getId().getName() + "Rename";
            streamDao.renameStream(office, originalId, newId);
            assertThrows(NotFoundException.class, () -> streamDao.retrieveStream(office, originalId, stream.getLengthUnits()));
            Stream retrievedStream = streamDao.retrieveStream(office, newId, stream.getLengthUnits());
            assertEquals(newId, retrievedStream.getId().getName());
            streamDao.deleteStream(office, newId, DeleteRule.DELETE_ALL);
            streamDao.deleteStream(flowsIntoStream.getId().getOfficeId(), flowsIntoStream.getId().getName(), DeleteRule.DELETE_ALL);
            streamDao.deleteStream(divertsFromStream.getId().getOfficeId(), divertsFromStream.getId().getName(), DeleteRule.DELETE_ALL);
            LocationsDaoImpl locationDao = new LocationsDaoImpl(getDslContext(c, OFFICE_ID));
            locationDao.deleteLocation(newId, OFFICE_ID);
        }, CwmsDataApiSetupCallback.getWebUser());
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
}