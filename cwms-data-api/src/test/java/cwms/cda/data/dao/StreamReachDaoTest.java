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
import cwms.cda.data.dto.stream.StreamReach;
import org.jooq.Record;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import usace.cwms.db.jooq.codegen.packages.cwms_stream.RETRIEVE_STREAM_REACH;

final class StreamReachDaoTest {

    @Test
    void testFromJooqStreamReachRecord() {
        Record record = Mockito.mock(Record.class);
        Mockito.when(record.get(StreamReachDao.REACH_OFFICE_ID_COLUMN, String.class)).thenReturn("SPK");
        Mockito.when(record.get(StreamReachDao.REACH_CONFIGURATION_COLUMN, String.class)).thenReturn("CONFIG_ID");
        Mockito.when(record.get(StreamReachDao.REACH_STREAM_ID_COLUMN, String.class)).thenReturn("STREAM_ID");
        Mockito.when(record.get(StreamReachDao.REACH_REACH_LOCATION_COLUMN, String.class)).thenReturn("REACH_ID");
        Mockito.when(record.get(StreamReachDao.REACH_UPSTREAM_LOCATION_COLUMN, String.class)).thenReturn("UPSTREAM_LOC");
        Mockito.when(record.get(StreamReachDao.REACH_UPSTREAM_STATION_COLUMN, Double.class)).thenReturn(1.0);
        Mockito.when(record.get(StreamReachDao.REACH_DOWNSTREAM_LOCATION_COLUMN, String.class)).thenReturn("DOWNSTREAM_LOC");
        Mockito.when(record.get(StreamReachDao.REACH_DOWNSTREAM_STATION_COLUMN, Double.class)).thenReturn(2.0);
        Mockito.when(record.get(StreamReachDao.REACH_COMMENTS_COLUMN, String.class)).thenReturn("Test Comment");

        StreamReach result = StreamReachDao.fromJooqStreamReachRecord(record, "km", Bank.fromCode("L"), Bank.fromCode("R"));

        assertNotNull(result);
        assertEquals("Test Comment", result.getComment());
        assertEquals("DOWNSTREAM_LOC", result.getDownstreamNode().getId().getName());
        assertEquals(2.0, result.getDownstreamNode().getStreamNode().getStation());
        assertEquals("UPSTREAM_LOC", result.getUpstreamNode().getId().getName());
        assertEquals(1.0, result.getUpstreamNode().getStreamNode().getStation());
        assertEquals("CONFIG_ID", result.getConfigurationId().getName());
        assertEquals("STREAM_ID", result.getStreamId().getName());
        assertEquals("REACH_ID", result.getId().getName());
        assertEquals("SPK", result.getId().getOfficeId());
        assertEquals("km", result.getUpstreamNode().getStreamNode().getStationUnits());
        assertEquals("km", result.getDownstreamNode().getStreamNode().getStationUnits());
        assertEquals(Bank.LEFT, result.getUpstreamNode().getStreamNode().getBank());
        assertEquals(Bank.RIGHT, result.getDownstreamNode().getStreamNode().getBank());
    }

    @Test
    void testFromJooqStreamReach() {
        RETRIEVE_STREAM_REACH retrieveStreamReach = Mockito.mock(RETRIEVE_STREAM_REACH.class);

        Mockito.when(retrieveStreamReach.getP_UPSTREAM_LOCATION()).thenReturn("UPSTREAM_LOC");
        Mockito.when(retrieveStreamReach.getP_DOWNSTREAM_LOCATION()).thenReturn("DOWNSTREAM_LOC");
        Mockito.when(retrieveStreamReach.getP_CONFIGURATION_ID()).thenReturn("CONFIG_ID");
        Mockito.when(retrieveStreamReach.getP_UPSTREAM_STATION()).thenReturn(1.0);
        Mockito.when(retrieveStreamReach.getP_DOWNSTREAM_STATION()).thenReturn(2.0);
        Mockito.when(retrieveStreamReach.getP_COMMENTS()).thenReturn("Test Comment");
        Mockito.when(retrieveStreamReach.getName()).thenReturn("REACH_ID");

        // Call the method under test
        StreamReach result = StreamReachDao.fromJooqStreamReach(retrieveStreamReach, "SPK", "STREAM_ID", "km",
                Bank.LEFT, Bank.RIGHT);

        // Verify the result
        assertNotNull(result);
        assertEquals("Test Comment", result.getComment());
        assertEquals("DOWNSTREAM_LOC", result.getDownstreamNode().getId().getName());
        assertEquals(2.0, result.getDownstreamNode().getStreamNode().getStation());
        assertEquals("UPSTREAM_LOC", result.getUpstreamNode().getId().getName());
        assertEquals(1.0, result.getUpstreamNode().getStreamNode().getStation());
        assertEquals("CONFIG_ID", result.getConfigurationId().getName());
        assertEquals("STREAM_ID", result.getStreamId().getName());
        assertEquals("REACH_ID", result.getId().getName());
        assertEquals("SPK", result.getId().getOfficeId());
        assertEquals("km", result.getUpstreamNode().getStreamNode().getStationUnits());
        assertEquals("km", result.getDownstreamNode().getStreamNode().getStationUnits());
        assertEquals(Bank.LEFT, result.getUpstreamNode().getStreamNode().getBank());
        assertEquals(Bank.RIGHT, result.getDownstreamNode().getStreamNode().getBank());
    }

}