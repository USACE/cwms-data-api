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
import cwms.cda.data.dto.stream.StreamReach;
import static java.util.stream.Collectors.toList;
import org.jooq.DSLContext;
import org.jooq.Record;
import org.jooq.Result;
import org.jooq.impl.DSL;
import usace.cwms.db.dao.util.OracleTypeMap;
import usace.cwms.db.jooq.codegen.packages.CWMS_STREAM_PACKAGE;
import usace.cwms.db.jooq.codegen.packages.cwms_stream.RETRIEVE_STREAM_REACH;
import java.util.List;

public final class StreamReachDao extends JooqDao<StreamReach> {

    static final String REACH_OFFICE_ID_COLUMN = "OFFICE_ID";
    static final String REACH_CONFIGURATION_COLUMN = "CONFIGURATION";
    static final String REACH_STREAM_ID_COLUMN = "STREAM_LOCATION";
    static final String REACH_REACH_LOCATION_COLUMN = "REACH_LOCATION";
    static final String REACH_UPSTREAM_LOCATION_COLUMN = "UPSTREAM_LOCATION";
    static final String REACH_UPSTREAM_STATION_COLUMN = "UPSTREAM_STATION";
    static final String REACH_DOWNSTREAM_LOCATION_COLUMN = "DOWNSTREAM_LOCATION";
    static final String REACH_DOWNSTREAM_STATION_COLUMN = "DOWNSTREAM_STATION";
    static final String REACH_COMMENTS_COLUMN = "COMMENTS";
    private final StreamLocationDao _streamLocationDao;

    public StreamReachDao(DSLContext dsl) {
        super(dsl);
        _streamLocationDao = new StreamLocationDao(dsl);
    }

    /**
     * Retrieve a list of stream reaches
     * @param streamIdMask - the stream id mask
     * @param reachIdMask - the reach id mask
     * @param configurationIdMask - the configuration id mask
     * @param stationUnits - the station units used for stations
     * @param officeIdMask - the office id mask
     * @return a list of stream reaches
     */
    public List<StreamReach> retrieveStreamReaches(String officeIdMask, String streamIdMask, String reachIdMask, String configurationIdMask, String stationUnits) {
        return connectionResult(dsl, conn -> {
            Result<Record> records = CWMS_STREAM_PACKAGE.call_CAT_STREAM_REACHES(DSL.using(conn).configuration(),
                    streamIdMask, reachIdMask, configurationIdMask, null, stationUnits, officeIdMask);
            return records.stream().map(r ->
                        fromJooqStreamReachRecord(r, stationUnits,
                            getBank(r.get(REACH_OFFICE_ID_COLUMN, String.class),
                                    r.get(REACH_STREAM_ID_COLUMN, String.class),
                                    r.get(REACH_UPSTREAM_LOCATION_COLUMN, String.class)),
                            getBank(r.get(REACH_OFFICE_ID_COLUMN, String.class),
                                    r.get(REACH_STREAM_ID_COLUMN, String.class),
                                    r.get(REACH_DOWNSTREAM_LOCATION_COLUMN, String.class))))
                    .collect(toList());
        });
    }

    /**
     * Retrieve a specific stream reach
     * @param reachId - the id of the stream reach
     * @param stationUnits - the station units used for stations
     * @param officeId - the office id
     * @return the stream reach
     */
    public StreamReach retrieveStreamReach( String officeId, String streamId, String reachId, String stationUnits) {
        return retrieveStreamReaches(officeId, streamId, reachId, null, stationUnits).stream()
                .findFirst()
                .orElseThrow(() -> new NotFoundException("Stream Reach: " + reachId + " not found"));
    }

    /**
     * Store a stream reach
     * @param streamReach - the stream reach to store
     * @param failIfExists - if true, fail if the stream reach already exists
     */
    public void storeStreamReach(StreamReach streamReach, boolean failIfExists) {
        connectionResult(dsl, conn -> {
            setOffice(conn, streamReach.getId().getOfficeId());
            String failsIfExistsStr = OracleTypeMap.formatBool(failIfExists);
            String ignoreNullsStr = OracleTypeMap.formatBool(true);
            String downstreamLocId = streamReach.getDownstreamNode().getId().getName();
            String upstreamLocId = streamReach.getUpstreamNode().getId().getName();
            String configId = streamReach.getConfigurationId() == null ? null : streamReach.getConfigurationId().getName();
            CWMS_STREAM_PACKAGE.call_STORE_STREAM_REACH(DSL.using(conn).configuration(), streamReach.getId().getName(), streamReach.getStreamId().getName(),
                    failsIfExistsStr, ignoreNullsStr, upstreamLocId, downstreamLocId, configId, streamReach.getComment(),
                    streamReach.getId().getOfficeId());
            return null;
        });
    }

    /**
     * Rename a stream reach
     * @param oldReachId - the old reach Id
     * @param newReachId - the new reach Id
     * @param officeId - the office Id
     */
    public void renameStreamReach(String officeId, String oldReachId, String newReachId) {
        connection(dsl, conn -> {
            setOffice(conn, officeId);
            CWMS_STREAM_PACKAGE.call_RENAME_STREAM_REACH(DSL.using(conn).configuration(), oldReachId, newReachId, officeId);
        });
    }

    /**
     * Delete a stream reach
     * @param reachId - the id of the stream reach
     * @param officeId - the office id
     */
    public void deleteStreamReach(String officeId, String reachId) {
        connection(dsl, conn -> {
            setOffice(conn, officeId);
            CWMS_STREAM_PACKAGE.call_DELETE_STREAM_REACH(DSL.using(conn).configuration(), reachId, officeId);
        });
    }

    static StreamReach fromJooqStreamReach(RETRIEVE_STREAM_REACH streamReach, String officeId, String streamId, String stationUnits,
                                           Bank upstreamBank, Bank downstreamBank) {
        return new StreamReach.Builder()
                .withComment(streamReach.getP_COMMENTS())
                .withDownstreamNode(buildStreamLocationNode(officeId,
                        streamId,
                        streamReach.getP_DOWNSTREAM_LOCATION(),
                        streamReach.getP_DOWNSTREAM_STATION(),
                        downstreamBank,
                        stationUnits))
                .withUpstreamNode(buildStreamLocationNode(officeId,
                        streamId,
                        streamReach.getP_UPSTREAM_LOCATION(),
                        streamReach.getP_UPSTREAM_STATION(),
                        upstreamBank,
                        stationUnits))
                .withConfigurationId(new CwmsId.Builder()
                        .withName(streamReach.getP_CONFIGURATION_ID())
                        .withOfficeId(officeId)
                        .build())
                .withStreamId(new CwmsId.Builder()
                        .withName(streamId)
                        .withOfficeId(officeId)
                        .build())
                .withId(new CwmsId.Builder()
                        .withName(streamReach.getName())
                        .withOfficeId(officeId)
                        .build())
                .build();
    }

    static StreamLocationNode buildStreamLocationNode(String officeId, String streamId, String locationId, Double station,
                                                       Bank bank, String stationUnits) {
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

    static StreamReach fromJooqStreamReachRecord(Record record, String stationUnits, Bank upstreamBank, Bank downstreamBank) {
        String officeId = record.get(REACH_OFFICE_ID_COLUMN, String.class);
        String streamId = record.get(REACH_STREAM_ID_COLUMN, String.class);
        return new StreamReach.Builder()
                .withComment(record.get(REACH_COMMENTS_COLUMN, String.class))
                .withDownstreamNode(buildStreamLocationNode(
                        officeId,
                        streamId,
                        record.get(REACH_DOWNSTREAM_LOCATION_COLUMN, String.class),
                        record.get(REACH_DOWNSTREAM_STATION_COLUMN, Double.class),
                        downstreamBank,
                        stationUnits))
                .withUpstreamNode(buildStreamLocationNode(
                        officeId,
                        streamId,
                        record.get(REACH_UPSTREAM_LOCATION_COLUMN, String.class),
                        record.get(REACH_UPSTREAM_STATION_COLUMN, Double.class),
                        upstreamBank,
                        stationUnits))
                .withConfigurationId(new CwmsId.Builder()
                        .withOfficeId(officeId)
                        .withName(record.get(REACH_CONFIGURATION_COLUMN, String.class))
                        .build())
                .withStreamId(new CwmsId.Builder()
                        .withOfficeId(officeId)
                        .withName(record.get(REACH_STREAM_ID_COLUMN, String.class))
                        .build())
                .withId(new CwmsId.Builder()
                        .withName(record.get(REACH_REACH_LOCATION_COLUMN, String.class))
                        .withOfficeId(officeId)
                        .build())
                .build();
    }

    //This should get removed once db is fixed to return bank
    private Bank getBank(String officeId, String streamId, String streamLocationId) {
        Bank retVal = null;
        StreamLocation streamLoc = _streamLocationDao.retrieveStreamLocation(officeId, streamId, streamLocationId, "km", "m", "km2");
        if(streamLoc != null)
        {
            retVal = streamLoc.getStreamLocationNode().getStreamNode().getBank();
        }
        return retVal;
    }
}