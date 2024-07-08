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
import cwms.cda.data.dto.stream.StreamLocation;
import cwms.cda.data.dto.stream.StreamLocationNode;
import org.jooq.Record;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import usace.cwms.db.jooq.codegen.packages.cwms_stream.RETRIEVE_STREAM_LOCATION;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

final class StreamLocationDaoTest {

    @Test
    void testFromJooqStreamLocation() {
        RETRIEVE_STREAM_LOCATION retrieveStreamLocation = Mockito.mock(RETRIEVE_STREAM_LOCATION.class);

        Mockito.when(retrieveStreamLocation.getP_PUBLISHED_STATION()).thenReturn(100.0);
        Mockito.when(retrieveStreamLocation.getP_NAVIGATION_STATION()).thenReturn(200.0);
        Mockito.when(retrieveStreamLocation.getP_LOWEST_MEASURABLE_STAGE()).thenReturn(2.5);
        Mockito.when(retrieveStreamLocation.getP_DRAINAGE_AREA()).thenReturn(500.0);
        Mockito.when(retrieveStreamLocation.getP_UNGAGED_DRAINAGE_AREA()).thenReturn(100.0);
        Mockito.when(retrieveStreamLocation.getP_BANK()).thenReturn(Bank.LEFT.getCode());

        StreamLocation result = StreamLocationDao.fromJooqStreamLocation(retrieveStreamLocation, "LOCATION_ID",
                "STREAM_ID", "SPK", "km", "m", "km2");

        assertNotNull(result);
        assertEquals("LOCATION_ID", result.getStreamLocationNode().getId().getName());
        assertEquals("SPK", result.getStreamLocationNode().getId().getOfficeId());
        assertEquals("STREAM_ID", result.getStreamLocationNode().getStreamNode().getStreamId().getName());
        assertEquals(100.0, result.getPublishedStation());
        assertEquals(200.0, result.getNavigationStation());
        assertEquals(2.5, result.getLowestMeasurableStage());
        assertEquals(500.0, result.getTotalDrainageArea());
        assertEquals(100.0, result.getUngagedDrainageArea());
        assertEquals(Bank.LEFT, result.getStreamLocationNode().getStreamNode().getBank());
        assertEquals("km", result.getStreamLocationNode().getStreamNode().getStationUnits());
        assertEquals("km2", result.getAreaUnits());
        assertEquals("m", result.getStageUnits());
    }

    @Test
    void testFromJooqStreamLocationRecord() {
        Record record = Mockito.mock(Record.class);
        Mockito.when(record.get(StreamLocationDao.STREAM_LOCATION_OFFICE_ID_COLUMN, String.class)).thenReturn("SPK");
        Mockito.when(record.get(StreamLocationDao.STREAM_LOCATION_STREAM_ID_COLUMN, String.class)).thenReturn("STREAM_ID");
        Mockito.when(record.get(StreamLocationDao.STREAM_LOCATION_LOCATION_ID_COLUMN, String.class)).thenReturn("LOCATION_ID");
        Mockito.when(record.get(StreamLocationDao.STREAM_LOCATION_STATION_COLUMN, Double.class)).thenReturn(10.0);
        Mockito.when(record.get(StreamLocationDao.STREAM_LOCATION_PUBLISHED_STATION_COLUMN, Double.class)).thenReturn(100.0);
        Mockito.when(record.get(StreamLocationDao.STREAM_LOCATION_NAVIGATION_STATION_COLUMN, Double.class)).thenReturn(200.0);
        Mockito.when(record.get(StreamLocationDao.STREAM_LOCATION_LOWEST_MEASURABLE_STAGE_COLUMN, Double.class)).thenReturn(2.5);
        Mockito.when(record.get(StreamLocationDao.STREAM_LOCATION_DRAINAGE_AREA_COLUMN, Double.class)).thenReturn(500.0);
        Mockito.when(record.get(StreamLocationDao.STREAM_LOCATION_UNGAGED_DRAINAGE_AREA_COLUMN, Double.class)).thenReturn(100.0);
        Mockito.when(record.get(StreamLocationDao.STREAM_LOCATION_BANK_COLUMN, String.class)).thenReturn("L");
        Mockito.when(record.get(StreamLocationDao.STREAM_LOCATION_STATION_UNITS_COLUMN, String.class)).thenReturn("km");
        Mockito.when(record.get(StreamLocationDao.STREAM_LOCATION_STAGE_UNITS_COLUMN, String.class)).thenReturn("m");
        Mockito.when(record.get(StreamLocationDao.STREAM_LOCATION_AREA_UNITS_COLUMN, String.class)).thenReturn("km2");


        StreamLocation result = StreamLocationDao.fromJooqStreamLocationRecord(record);

        assertNotNull(result);
        assertEquals("LOCATION_ID", result.getStreamLocationNode().getId().getName());
        assertEquals("SPK", result.getStreamLocationNode().getId().getOfficeId());
        assertEquals("STREAM_ID", result.getStreamLocationNode().getStreamNode().getStreamId().getName());
        assertEquals(10.0, result.getStreamLocationNode().getStreamNode().getStation());
        assertEquals(Bank.LEFT, result.getStreamLocationNode().getStreamNode().getBank());
        assertEquals(100.0, result.getPublishedStation());
        assertEquals(200.0, result.getNavigationStation());
        assertEquals(2.5, result.getLowestMeasurableStage());
        assertEquals(500.0, result.getTotalDrainageArea());
        assertEquals(100.0, result.getUngagedDrainageArea());
        assertEquals("km", result.getStreamLocationNode().getStreamNode().getStationUnits());
        assertEquals("m", result.getStageUnits());
        assertEquals("km2", result.getAreaUnits());
    }

    @Test
    void testBuildStreamLocationNode() {
        String officeId = "SPK";
        String streamId = "STREAM_ID";
        String locationId = "LOCATION_ID";
        Double station = 10.0;
        Bank bank = Bank.LEFT;
        String stationUnits = "km";

        StreamLocationNode result = StreamLocationDao.buildStreamLocationNode(officeId, streamId, locationId, station, bank, stationUnits);

        assertNotNull(result);
        assertEquals(locationId, result.getId().getName());
        assertEquals(officeId, result.getId().getOfficeId());
        assertEquals(streamId, result.getStreamNode().getStreamId().getName());
        assertEquals(officeId, result.getStreamNode().getStreamId().getOfficeId());
        assertEquals(station, result.getStreamNode().getStation());
        assertEquals(bank, result.getStreamNode().getBank());
        assertEquals(stationUnits, result.getStreamNode().getStationUnits());
    }
}