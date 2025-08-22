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

import cwms.cda.data.dto.stream.Bank;
import cwms.cda.data.dto.stream.Stream;
import cwms.cda.data.dto.stream.StreamNode;
import org.jooq.Record;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import usace.cwms.db.jooq.codegen.udt.records.STREAM_T;

final class StreamDaoTest {

    @Test
    void testFromJooqStream() {
        STREAM_T streamT = new STREAM_T();
        streamT.setSTATIONING_STARTS_DS("1")
                .setFLOWS_INTO_STREAM("INTO_STREAM")
                .setFLOWS_INTO_STATION(10.0)
                .setFLOWS_INTO_BANK("LEFT")
                .setDIVERTS_FROM_STREAM("FROM_STREAM")
                .setDIVERTS_FROM_STATION(20.0)
                .setDIVERTS_FROM_BANK("RIGHT")
                .setLENGTH(100.0)
                .setAVERAGE_SLOPE(5.0)
                .setCOMMENTS("Test Comment")
                .setNAME("STREAM_NAME")
                .setOFFICE_ID("OFFICE_ID")
                .setUNIT("km");

        Stream result = StreamDao.fromJooqStream(streamT);

        assertNotNull(result);
        assertEquals(JooqDao.parseBool(streamT.getSTATIONING_STARTS_DS()), result.getStartsDownstream());
        assertEquals(streamT.getNAME(), result.getId().getName());
        assertEquals(streamT.getOFFICE_ID(), result.getId().getOfficeId());
        assertEquals(streamT.getFLOWS_INTO_STREAM(), result.getFlowsIntoStreamNode().getStreamId().getName());
        assertEquals(streamT.getFLOWS_INTO_STATION(), result.getFlowsIntoStreamNode().getStation());
        assertEquals("km", result.getFlowsIntoStreamNode().getStationUnits());
        assertEquals(Bank.fromCode(streamT.getFLOWS_INTO_BANK()), result.getFlowsIntoStreamNode().getBank());
        assertEquals(streamT.getDIVERTS_FROM_STREAM(), result.getDivertsFromStreamNode().getStreamId().getName());
        assertEquals(streamT.getDIVERTS_FROM_STATION(), result.getDivertsFromStreamNode().getStation());
        assertEquals("km", result.getDivertsFromStreamNode().getStationUnits());
        assertEquals(Bank.fromCode(streamT.getDIVERTS_FROM_BANK()), result.getDivertsFromStreamNode().getBank());
        assertEquals(streamT.getLENGTH(), result.getLength());
        assertEquals(streamT.getAVERAGE_SLOPE(), result.getAverageSlope());
        assertEquals(streamT.getCOMMENTS(), result.getComment());
        assertEquals("%", result.getSlopeUnits());
        assertEquals("km", result.getLengthUnits());
    }

    @Test
    void testFromJooqStreamRecord() {
        Record record = Mockito.mock(Record.class);
        Mockito.when(record.get(StreamDao.STATIONING_STARTS_DS_COLUMN, Boolean.class)).thenReturn(true);
        Mockito.when(record.get(StreamDao.STREAM_STREAM_ID_COLUMN, String.class)).thenReturn("STREAM_ID");
        Mockito.when(record.get(StreamDao.STREAM_OFFICE_ID_COLUMN, String.class)).thenReturn("SPK");
        Mockito.when(record.get(StreamDao.STREAM_FLOWS_INTO_STREAM_COLUMN, String.class)).thenReturn("INTO_STREAM");
        Mockito.when(record.get(StreamDao.STREAM_FLOWS_INTO_STATION_COLUMN, Double.class)).thenReturn(10.0);
        Mockito.when(record.get(StreamDao.STREAM_FLOWS_INTO_BANK_COLUMN, String.class)).thenReturn("L");
        Mockito.when(record.get(StreamDao.STREAM_DIVERTS_FROM_STREAM_COLUMN, String.class)).thenReturn("FROM_STREAM");
        Mockito.when(record.get(StreamDao.STREAM_DIVERTS_FROM_STATION_COLUMN, Double.class)).thenReturn(20.0);
        Mockito.when(record.get(StreamDao.STREAM_DIVERTS_FROM_BANK_COLUMN, String.class)).thenReturn("R");
        Mockito.when(record.get(StreamDao.STREAM_STREAM_LENGTH_COLUMN, Double.class)).thenReturn(100.0);
        Mockito.when(record.get(StreamDao.STREAM_AVERAGE_SLOPE_COLUMN, Double.class)).thenReturn(5.0);
        Mockito.when(record.get(StreamDao.STREAM_COMMENTS_COLUMN, String.class)).thenReturn("Test Comment");

        Stream stream = StreamDao.fromJooqStreamRecord(record, "km");

        assertNotNull(stream);
        assertEquals(true, stream.getStartsDownstream());
        assertEquals("STREAM_ID", stream.getId().getName());
        assertEquals("SPK", stream.getId().getOfficeId());
        assertEquals("INTO_STREAM", stream.getFlowsIntoStreamNode().getStreamId().getName());
        assertEquals(10.0, stream.getFlowsIntoStreamNode().getStation());
        assertEquals(Bank.LEFT, stream.getFlowsIntoStreamNode().getBank());
        assertEquals("FROM_STREAM", stream.getDivertsFromStreamNode().getStreamId().getName());
        assertEquals(20.0, stream.getDivertsFromStreamNode().getStation());
        assertEquals(Bank.RIGHT, stream.getDivertsFromStreamNode().getBank());
        assertEquals(100.0, stream.getLength());
        assertEquals(5.0, stream.getAverageSlope());
        assertEquals("Test Comment", stream.getComment());
        assertEquals("km", stream.getLengthUnits());
        assertEquals("%", stream.getSlopeUnits());
    }
    @Test
    void testBuildStreamNode() {
        String officeId = "SPK";
        String streamId = "STREAM_ID";
        double station = 10.0;
        Bank bank = Bank.LEFT;
        String stationUnits = "km";

        StreamNode streamNode = StreamDao.buildStreamNode(officeId, streamId, station, bank, stationUnits);

        assertNotNull(streamNode);
        assertEquals(streamId, streamNode.getStreamId().getName());
        assertEquals(officeId, streamNode.getStreamId().getOfficeId());
        assertEquals(station, streamNode.getStation());
        assertEquals(bank, streamNode.getBank());
        assertEquals(stationUnits, streamNode.getStationUnits());
    }

    @Test
    void testBuildStreamNodeNull() {
        String officeId = "SPK";
        String streamId = null;
        double station = 10.0;
        Bank bank = Bank.LEFT;
        String stationUnits = "km";

        StreamNode streamNode = StreamDao.buildStreamNode(officeId, streamId, station, bank, stationUnits);

        assertNull(streamNode);
    }


}