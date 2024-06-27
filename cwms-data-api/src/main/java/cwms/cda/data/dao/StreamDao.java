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

import cwms.cda.api.errors.FieldException;
import cwms.cda.api.errors.NotFoundException;
import cwms.cda.data.dto.CwmsId;
import cwms.cda.data.dto.stream.Bank;
import cwms.cda.data.dto.stream.Stream;
import cwms.cda.data.dto.stream.StreamNode;
import java.sql.ResultSet;
import java.util.ArrayList;
import org.jooq.DSLContext;
import org.jooq.impl.DSL;
import usace.cwms.db.dao.util.OracleTypeMap;
import usace.cwms.db.jooq.codegen.packages.CWMS_STREAM_PACKAGE;
import usace.cwms.db.jooq.codegen.udt.records.STREAM_T;

import java.sql.SQLException;
import java.util.List;

public final class StreamDao extends JooqDao<Stream> {

    static final int STREAM_OFFICE_ID_COLUMN_INDEX = 1;
    static final int STREAM_STREAM_ID_COLUMN_INDEX = 2;
    static final int STATIONING_STARTS_DS_COLUMN_INDEX = 3;
    static final int STREAM_FLOWS_INTO_STREAM_COLUMN_INDEX = 4;
    static final int STREAM_FLOWS_INTO_STATION_COLUMN_INDEX = 5;
    static final int STREAM_FLOWS_INTO_BANK_COLUMN_INDEX = 6;
    static final int STREAM_DIVERTS_FROM_STREAM_COLUMN_INDEX = 7;
    static final int STREAM_DIVERTS_FROM_STATION_COLUMN_INDEX = 8;
    static final int STREAM_DIVERTS_FROM_BANK_COLUMN_INDEX = 9;
    static final int STREAM_STREAM_LENGTH_COLUMN_INDEX = 10;
    static final int STREAM_AVERAGE_SLOPE_COLUMN_INDEX = 11;
    static final int STREAM_COMMENTS_COLUMN_INDEX = 12;
    private static final String DB_STATION_UNITS = "km";
    public static final String DB_STREAM_SLOPE_UNITS = "%";

    public StreamDao(DSLContext dsl) {
        super(dsl);
    }

    /**
     * Retrieve a list of streams
     * @param officeIdMask - the office id mask
     * @param streamIdMask - the stream id mask
     * @return a list of streams
     */
    public List<Stream> retrieveStreams(String officeIdMask, String streamIdMask) {
        return connectionResult(dsl, conn -> {
            setOffice(conn, officeIdMask);
            ResultSet streams = CWMS_STREAM_PACKAGE.call_CAT_STREAMS(DSL.using(conn).configuration(), streamIdMask,
                    DB_STATION_UNITS, null, null, null,
                    null, null, null, null,
                    null, null, null, null, null,
                    null, null, officeIdMask)
                    .intoResultSet();
            return buildStreamListFromResultSet(streams);
        });
    }

    /**
     * Retrieve a specific stream
     * @param streamId - the id of the stream
     * @param officeId - the office id
     * @return the stream
     */
    public Stream retrieveStream(String officeId, String streamId) {
        return connectionResult(dsl, conn -> {
            setOffice(conn, officeId);
            STREAM_T streamT = CWMS_STREAM_PACKAGE.call_RETRIEVE_STREAM_F(DSL.using(conn).configuration(), streamId, DB_STATION_UNITS, officeId);
            if (streamT == null) {
                throw new NotFoundException("Stream: " + officeId + "." + streamId + " not found");
            }
            return fromJooqStream(streamT);
        });
    }

    /**
     * Store a stream
     * @param stream - the stream to store
     * @param failIfExists - if true, fail if the stream already exists
     */
    public void storeStream(Stream stream, boolean failIfExists) {
        connectionResult(dsl, conn -> {
            setOffice(conn, stream.getOfficeId());
            String failsIfExistsStr = OracleTypeMap.formatBool(failIfExists);
            String ignoreNullsStr = OracleTypeMap.formatBool(true);
            String startsDownstream = OracleTypeMap.formatBool(stream.getStartsDownstream());
            String stationUnits = getStationUnits(stream);
            String flowIntoStream = null;
            Double flowIntoStation = null;
            String flowIntoBank = null;
            String divertFromStream = null;
            Double divertFromStation = null;
            String divertFromBank = null;
            if(stream.getFlowsIntoStreamNode() != null) {
                flowIntoStream = stream.getFlowsIntoStreamNode().getStreamId().getName();
                flowIntoStation = stream.getFlowsIntoStreamNode().getStation();
                flowIntoBank = stream.getFlowsIntoStreamNode().getBank() == null ? null : stream.getFlowsIntoStreamNode().getBank().getCode();
            }
            if(stream.getDivertsFromStreamNode() != null) {
                divertFromStream = stream.getDivertsFromStreamNode().getStreamId().getName();
                divertFromStation = stream.getDivertsFromStreamNode().getStation();
                divertFromBank = stream.getDivertsFromStreamNode().getBank() == null ? null : stream.getDivertsFromStreamNode().getBank().getCode();
            }
            CWMS_STREAM_PACKAGE.call_STORE_STREAM(DSL.using(conn).configuration(), stream.getId().getName(), failsIfExistsStr, ignoreNullsStr,
                    stationUnits, startsDownstream, flowIntoStream, flowIntoStation, flowIntoBank,
                    divertFromStream, divertFromStation, divertFromBank,
                    stream.getLength(), stream.getAverageSlope(), stream.getComment(), stream.getOfficeId());
            return null;
        });
    }

    /**
     * @param officeId - the office Id
     * @param oldStreamId - the old stream Id
     * @param newStreamId - the newly named stream Id
     */
    public void renameStream(String officeId, String oldStreamId, String newStreamId) {
        connection(dsl, conn -> {
            setOffice(conn, officeId);
            CWMS_STREAM_PACKAGE.call_RENAME_STREAM(DSL.using(conn).configuration(), oldStreamId,
                    newStreamId, officeId);
        });
    }

    /**
     * Delete a stream
     * @param officeId - the office id
     * @param streamId - the id of the stream
     */
    public void deleteStream(String officeId, String streamId, DeleteRule deleteRule) {
        Stream streamToDelete = retrieveStream(officeId, streamId);
        if (streamToDelete == null) {
            throw new NotFoundException("Could not find stream " + streamId + "to delete.");
        }
        connectionResult(dsl, conn -> {
            setOffice(conn, officeId);
            CWMS_STREAM_PACKAGE.call_DELETE_STREAM(DSL.using(conn).configuration(),
                    streamId,
                    deleteRule == null ? null : deleteRule.getRule(),
                    officeId);
            return null;
        });
    }

    private String getStationUnits(Stream stream) {
        String stationUnits = stream.getLengthUnits();
        if(stationUnits == null || stationUnits.isEmpty()) {
            if(stream.getFlowsIntoStreamNode() != null) {
                stationUnits = stream.getFlowsIntoStreamNode().getStationUnits();
            }
            if((stationUnits == null || stationUnits.isEmpty()) && stream.getDivertsFromStreamNode() != null) {
                stationUnits = stream.getDivertsFromStreamNode().getStationUnits();
            }
        }
        return stationUnits;
    }

    static Stream fromJooqStream(STREAM_T stream) {
        return new Stream.Builder()
                .withStartsDownstream(OracleTypeMap.parseBool(stream.getSTATIONING_STARTS_DS()))
                .withFlowsIntoStreamNode(buildStreamNode(stream.getOFFICE_ID(),
                        stream.getFLOWS_INTO_STREAM(),
                        stream.getFLOWS_INTO_STATION(),
                        Bank.fromCode(stream.getFLOWS_INTO_BANK()),
                        DB_STATION_UNITS))
                .withDivertsFromStreamNode(buildStreamNode(stream.getOFFICE_ID(),
                        stream.getDIVERTS_FROM_STREAM(),
                        stream.getDIVERTS_FROM_STATION(),
                        Bank.fromCode(stream.getDIVERTS_FROM_BANK()),
                        DB_STATION_UNITS))
                .withLength(stream.getLENGTH())
                .withAverageSlope(stream.getAVERAGE_SLOPE())
                .withComment(stream.getCOMMENTS())
                .withId(new CwmsId.Builder()
                        .withName(stream.getNAME())
                        .withOfficeId(stream.getOFFICE_ID())
                        .build())
                .withLengthUnits(DB_STATION_UNITS)
                .withSlopeUnits(DB_STREAM_SLOPE_UNITS)
                .build();
    }

    static List<Stream> buildStreamListFromResultSet(ResultSet result) throws SQLException
    {
        List<Stream> retVal = new ArrayList<>();
        while(result.next())
        {
            Stream stream = new Stream.Builder()
                    .withStartsDownstream(result.getBoolean(STATIONING_STARTS_DS_COLUMN_INDEX))
                    .withId(new CwmsId.Builder()
                            .withName(result.getString(STREAM_STREAM_ID_COLUMN_INDEX))
                            .withOfficeId(result.getString(STREAM_OFFICE_ID_COLUMN_INDEX))
                            .build())
                    .withFlowsIntoStreamNode(buildStreamNode(result.getString(STREAM_OFFICE_ID_COLUMN_INDEX),
                            result.getString(STREAM_FLOWS_INTO_STREAM_COLUMN_INDEX),
                            result.getDouble(STREAM_FLOWS_INTO_STATION_COLUMN_INDEX),
                            Bank.fromCode(result.getString(STREAM_FLOWS_INTO_BANK_COLUMN_INDEX)),
                            DB_STATION_UNITS))
                    .withDivertsFromStreamNode(buildStreamNode(result.getString(STREAM_OFFICE_ID_COLUMN_INDEX),
                            result.getString(STREAM_DIVERTS_FROM_STREAM_COLUMN_INDEX),
                            result.getDouble(STREAM_DIVERTS_FROM_STATION_COLUMN_INDEX),
                            Bank.fromCode(result.getString(STREAM_DIVERTS_FROM_BANK_COLUMN_INDEX)),
                            DB_STATION_UNITS))
                    .withLength(result.getDouble(STREAM_STREAM_LENGTH_COLUMN_INDEX))
                    .withAverageSlope(result.getDouble(STREAM_AVERAGE_SLOPE_COLUMN_INDEX))
                    .withComment(result.getString(STREAM_COMMENTS_COLUMN_INDEX))
                    .withLengthUnits(DB_STATION_UNITS)
                    .withSlopeUnits(DB_STREAM_SLOPE_UNITS)
                    .build();
            retVal.add(stream);
        }
        return retVal;
    }

    static StreamNode buildStreamNode(String officeId, String streamId, Double station, Bank bank, String stationUnits) {
        StreamNode retVal;
        try {
            retVal = new StreamNode.Builder()
                    .withStreamId(new CwmsId.Builder()
                            .withName(streamId)
                            .withOfficeId(officeId)
                            .build())
                    .withStation(station)
                    .withBank(bank)
                    .withStationUnits(stationUnits)
                    .build();
            retVal.validate();
        } catch (FieldException e) {
            retVal = null; //if missing required fields, don't build a stream node, just return null
        }
        return retVal;
    }
}