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

import cwms.cda.api.errors.NotFoundException;
import cwms.cda.data.dto.CwmsId;
import cwms.cda.data.dto.stream.Bank;
import cwms.cda.data.dto.stream.StreamLocation;
import cwms.cda.data.dto.stream.StreamLocationNode;
import cwms.cda.data.dto.stream.StreamNode;
import java.sql.Connection;
import org.jooq.DSLContext;
import org.jooq.Record;
import org.jooq.Result;
import org.jooq.exception.DataAccessException;
import org.jooq.impl.DSL;
import usace.cwms.db.dao.util.OracleTypeMap;
import usace.cwms.db.jooq.codegen.packages.CWMS_STREAM_PACKAGE;
import usace.cwms.db.jooq.codegen.packages.cwms_stream.RETRIEVE_STREAM_LOCATION;

import java.util.List;

import static java.util.stream.Collectors.toList;

public final class StreamLocationDao extends JooqDao<StreamLocation> {

    static final String STREAM_LOCATION_OFFICE_ID_COLUMN = "OFFICE_ID";
    static final String STREAM_LOCATION_STREAM_ID_COLUMN = "STREAM_ID";
    static final String STREAM_LOCATION_LOCATION_ID_COLUMN = "LOCATION_ID";
    static final String STREAM_LOCATION_STATION_COLUMN = "STATION";
    static final String STREAM_LOCATION_PUBLISHED_STATION_COLUMN = "PUBLISHED_STATION";
    static final String STREAM_LOCATION_NAVIGATION_STATION_COLUMN = "NAVIGATION_STATION";
    static final String STREAM_LOCATION_BANK_COLUMN = "BANK";
    static final String STREAM_LOCATION_LOWEST_MEASURABLE_STAGE_COLUMN = "LOWEST_MEASURABLE_STAGE";
    static final String STREAM_LOCATION_DRAINAGE_AREA_COLUMN = "DRAINAGE_AREA";
    static final String STREAM_LOCATION_UNGAGED_DRAINAGE_AREA_COLUMN = "UNGAGED_DRAINAGE_AREA";
    static final String STREAM_LOCATION_AREA_UNITS_COLUMN = "AREA_UNIT";
    static final String STREAM_LOCATION_STAGE_UNITS_COLUMN = "STAGE_UNIT";
    static final String STREAM_LOCATION_STATION_UNITS_COLUMN = "STATION_UNIT";

    public StreamLocationDao(DSLContext dsl) {
        super(dsl);
    }

    /**
     * Retrieve a list of stream locations
     * @param streamIdMask - the stream id mask
     * @param locationIdMask - the location id mask
     * @param stationUnit - the station units used for stations
     * @param stageUnit - the stage units
     * @param areaUnit - the area units
     * @param officeIdMask - the office id mask
     * @return a list of stream locations
     */
    public List<StreamLocation> retrieveStreamLocations(String streamIdMask, String locationIdMask, String stationUnit, String stageUnit, String areaUnit, String officeIdMask) {
        return connectionResult(dsl, conn -> {
            Result<Record> records = CWMS_STREAM_PACKAGE.call_CAT_STREAM_LOCATIONS(DSL.using(conn).configuration(),
                    streamIdMask, locationIdMask, stationUnit, stageUnit, areaUnit, officeIdMask);
            return records.stream().map(StreamLocationDao::fromJooqStreamLocationRecord)
                    .collect(toList());
        });
    }

    /**
     * Retrieve a specific stream location
     * @param locationId - the id of the stream location
     * @param streamId - the id of the stream
     * @param officeId - the office id
     * @return the stream location
     */
    public StreamLocation retrieveStreamLocation(String officeId, String streamId, String locationId, String stationUnit, String stageUnit, String areaUnit) {
        return connectionResult(dsl, conn -> {
            try {
                setOffice(conn, officeId);
                return retrieveStreamLocation(officeId, streamId, locationId, stationUnit, stageUnit, areaUnit, conn);
            } catch (DataAccessException e) {
                throw wrapException(e);
            }
        });
    }

    static StreamLocation retrieveStreamLocation(String officeId, String streamId, String locationId, String stationUnit, String stageUnit, String areaUnit, Connection conn) {
        RETRIEVE_STREAM_LOCATION retrieveStreamLocation = CWMS_STREAM_PACKAGE.call_RETRIEVE_STREAM_LOCATION(DSL.using(conn).configuration(),
                locationId, streamId, stationUnit, stageUnit, areaUnit, officeId);
        return fromJooqStreamLocation(retrieveStreamLocation, locationId, streamId, officeId, stationUnit, stageUnit, areaUnit);
    }

    /**
     * Store a stream location
     * @param streamLocation - the stream location to store
     * @param failIfExists - if true, fail if the stream location already exists
     */
    public void storeStreamLocation(StreamLocation streamLocation, boolean failIfExists) {
        connectionResult(dsl, conn -> {
            setOffice(conn, streamLocation.getId().getOfficeId());
            String failsIfExistsStr = OracleTypeMap.formatBool(failIfExists);
            String ignoreNullsStr = OracleTypeMap.formatBool(true);
            StreamLocationNode streamLocationNode = streamLocation.getStreamLocationNode();
            StreamNode streamNode = streamLocationNode.getStreamNode();
            String bank = streamNode.getBank() == null ? null : streamNode.getBank().getCode();
            CWMS_STREAM_PACKAGE.call_STORE_STREAM_LOCATION(DSL.using(conn).configuration(),
                    streamLocationNode.getId().getName(),
                    streamNode.getStreamId().getName(),
                    failsIfExistsStr, ignoreNullsStr, streamLocation.getStation(), streamLocation.getStationUnits(),
                    streamLocation.getPublishedStation(), streamLocation.getNavigationStation(), bank,
                    streamLocation.getLowestMeasurableStage(), streamLocation.getStageUnits(), streamLocation.getTotalDrainageArea(),
                    streamLocation.getUngagedDrainageArea(), streamLocation.getAreaUnits(),streamLocationNode.getId().getOfficeId());
            return null;
        });
    }

    /**
     * Update a stream location
     * @param streamLocation - the stream location to update
     */
    public void updateStreamLocation(StreamLocation streamLocation) {
        StreamLocation existingStreamLocation = retrieveStreamLocation(streamLocation.getId().getOfficeId(), streamLocation.getStreamLocationNode().getStreamNode().getStreamId().getName(), streamLocation.getId().getName(),
                streamLocation.getStreamLocationNode().getStreamNode().getStationUnits(), streamLocation.getStageUnits(), streamLocation.getAreaUnits());
        if (existingStreamLocation == null) {
            throw new NotFoundException("Could not find stream location to update.");
        }
        storeStreamLocation(streamLocation, false);
    }

    /**
     * Delete a stream location
     * @param locationId - the id of the stream location
     * @param streamId - the id of the stream
     * @param officeId - the office id
     */
    public void deleteStreamLocation(String officeId, String streamId, String locationId) {
        connection(dsl, conn -> {
            setOffice(conn, officeId);
            CWMS_STREAM_PACKAGE.call_DELETE_STREAM_LOCATION(DSL.using(conn).configuration(), locationId, streamId, officeId);
        });
    }

    static StreamLocation fromJooqStreamLocation(RETRIEVE_STREAM_LOCATION streamLocation, String locationId, String streamId, String officeId, String stationUnit, String stageUnit, String areaUnit) {
        return new StreamLocation.Builder()
                .withStreamLocationNode(buildStreamLocationNode(officeId,
                        streamId,
                        locationId,
                        streamLocation.getP_STATION(),
                        Bank.fromCode(streamLocation.getP_BANK()),
                        stationUnit))
                .withPublishedStation(streamLocation.getP_PUBLISHED_STATION())
                .withNavigationStation(streamLocation.getP_NAVIGATION_STATION())
                .withLowestMeasurableStage(streamLocation.getP_LOWEST_MEASURABLE_STAGE())
                .withTotalDrainageArea(streamLocation.getP_DRAINAGE_AREA())
                .withUngagedDrainageArea(streamLocation.getP_UNGAGED_DRAINAGE_AREA())
                .withAreaUnits(areaUnit)
                .withStageUnits(stageUnit)
                .build();
    }

    static StreamLocationNode buildStreamLocationNode(String officeId, String streamId, String locationId, Double station, Bank bank, String stationUnits) {
        return new StreamLocationNode.Builder()
                .withId(new CwmsId.Builder()
                        .withName(locationId)
                        .withOfficeId(officeId)
                        .build())
                .withStreamNode(new StreamNode.Builder()
                        .withStreamId(new CwmsId.Builder()
                                .withName(streamId)
                                .withOfficeId(officeId)
                                .build())
                        .withStation(station)
                        .withBank(bank)
                        .withStationUnits(stationUnits)
                        .build())
                .build();
    }

    static StreamLocation fromJooqStreamLocationRecord(Record record) {
        return new StreamLocation.Builder()
                .withStreamLocationNode(buildStreamLocationNode(
                        record.get(STREAM_LOCATION_OFFICE_ID_COLUMN, String.class),
                        record.get(STREAM_LOCATION_STREAM_ID_COLUMN, String.class),
                        record.get(STREAM_LOCATION_LOCATION_ID_COLUMN, String.class),
                        record.get(STREAM_LOCATION_STATION_COLUMN, Double.class),
                        Bank.fromCode(record.get(STREAM_LOCATION_BANK_COLUMN, String.class)),
                        record.get(STREAM_LOCATION_STATION_UNITS_COLUMN, String.class)))
                .withPublishedStation(record.get(STREAM_LOCATION_PUBLISHED_STATION_COLUMN, Double.class))
                .withNavigationStation(record.get(STREAM_LOCATION_NAVIGATION_STATION_COLUMN, Double.class))
                .withLowestMeasurableStage(record.get(STREAM_LOCATION_LOWEST_MEASURABLE_STAGE_COLUMN, Double.class))
                .withTotalDrainageArea(record.get(STREAM_LOCATION_DRAINAGE_AREA_COLUMN, Double.class))
                .withUngagedDrainageArea(record.get(STREAM_LOCATION_UNGAGED_DRAINAGE_AREA_COLUMN, Double.class))
                .withAreaUnits(record.get(STREAM_LOCATION_AREA_UNITS_COLUMN, String.class))
                .withStageUnits(record.get(STREAM_LOCATION_STAGE_UNITS_COLUMN, String.class))
                .build();
    }
}