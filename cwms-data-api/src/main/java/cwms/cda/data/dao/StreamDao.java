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
import cwms.cda.data.dto.stream.Stream;
import cwms.cda.data.dto.stream.StreamNode;
import static java.util.stream.Collectors.toList;
import org.jooq.DSLContext;
import org.jooq.Record;
import org.jooq.Result;
import org.jooq.impl.DSL;
import usace.cwms.db.dao.util.OracleTypeMap;
import usace.cwms.db.jooq.codegen.packages.CWMS_STREAM_PACKAGE;
import usace.cwms.db.jooq.codegen.udt.records.STREAM_T;

import java.util.List;

public final class StreamDao extends JooqDao<Stream> {

    static final int STREAM_OFFICE_ID_COLUMN_INDEX = 0;
    static final int STREAM_STREAM_ID_COLUMN_INDEX = 1;
    static final int STATIONING_STARTS_DS_COLUMN_INDEX = 2;
    static final int STREAM_FLOWS_INTO_STREAM_COLUMN_INDEX = 3;
    static final int STREAM_FLOWS_INTO_STATION_COLUMN_INDEX = 4;
    static final int STREAM_FLOWS_INTO_BANK_COLUMN_INDEX = 5;
    static final int STREAM_DIVERTS_FROM_STREAM_COLUMN_INDEX = 6;
    static final int STREAM_DIVERTS_FROM_STATION_COLUMN_INDEX = 7;
    static final int STREAM_DIVERTS_FROM_BANK_COLUMN_INDEX = 8;
    static final int STREAM_STREAM_LENGTH_COLUMN_INDEX = 9;
    static final int STREAM_AVERAGE_SLOPE_COLUMN_INDEX = 10;
    static final int STREAM_COMMENTS_COLUMN_INDEX = 11;
    public static final String DB_STREAM_SLOPE_UNITS = "%";

    public StreamDao(DSLContext dsl) {
        super(dsl);
    }

    /**
     * Retrieve a list of streams
     * @param officeIdMask - the office id mask
     * @param streamIdMask - the stream id mask
     * @param stationUnits - the station units used for stations and length of stream
     * @return a list of streams
     */
    public List<Stream> retrieveStreams(String officeIdMask, String streamIdMask, String stationUnits) {
        return connectionResult(dsl, conn -> {
            setOffice(conn, officeIdMask);
            Result<Record> records = CWMS_STREAM_PACKAGE.call_CAT_STREAMS(DSL.using(conn).configuration(), streamIdMask,
                            stationUnits, null, null, null,
                            null, null, null, null,
                            null, null, null, null, null,
                            null, null, officeIdMask);
            return records.stream().map(r -> fromJooqStreamRecord(r, stationUnits))
                    .collect(toList());
        });
    }

    /**
     * Retrieve a specific stream
     * @param streamId - the id of the stream
     * @param officeId - the office id
     * @param stationUnits - the station units used for stations and length of stream
     * @return the stream
     */
    public Stream retrieveStream(String officeId, String streamId, String stationUnits) {
        return connectionResult(dsl, conn -> {
            setOffice(conn, officeId);
            STREAM_T streamT = CWMS_STREAM_PACKAGE.call_RETRIEVE_STREAM_F(DSL.using(conn).configuration(), streamId, stationUnits, officeId);
            if (streamT == null) {
                throw new NotFoundException("Stream: " + officeId + "." + streamId + " not found");
            }
            streamT.setUNIT(stationUnits);
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
        Stream streamToDelete = retrieveStream(officeId, streamId, "km");
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
            StreamNode flowsIntoStreamNode = stream.getFlowsIntoStreamNode();
            if(flowsIntoStreamNode != null) {
                stationUnits = flowsIntoStreamNode.getStationUnits();
            }
            StreamNode divertsFromStreamNode = stream.getDivertsFromStreamNode();
            if((stationUnits == null || stationUnits.isEmpty()) &&  divertsFromStreamNode != null) {
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
                        stream.getUNIT()))
                .withDivertsFromStreamNode(buildStreamNode(stream.getOFFICE_ID(),
                        stream.getDIVERTS_FROM_STREAM(),
                        stream.getDIVERTS_FROM_STATION(),
                        Bank.fromCode(stream.getDIVERTS_FROM_BANK()),
                        stream.getUNIT()))
                .withLength(stream.getLENGTH())
                .withAverageSlope(stream.getAVERAGE_SLOPE())
                .withComment(stream.getCOMMENTS())
                .withId(new CwmsId.Builder()
                        .withName(stream.getNAME())
                        .withOfficeId(stream.getOFFICE_ID())
                        .build())
                .withLengthUnits(stream.getUNIT())
                .withSlopeUnits(DB_STREAM_SLOPE_UNITS)
                .build();
    }

    static Stream fromJooqStreamRecord(Record record, String stationUnits) {
        return new Stream.Builder()
                .withStartsDownstream(record.get(STATIONING_STARTS_DS_COLUMN_INDEX, Boolean.class))
                .withId(new CwmsId.Builder()
                        .withName(record.get(STREAM_STREAM_ID_COLUMN_INDEX, String.class))
                        .withOfficeId(record.get(STREAM_OFFICE_ID_COLUMN_INDEX, String.class))
                        .build())
                .withFlowsIntoStreamNode(buildStreamNode(
                        record.get(STREAM_OFFICE_ID_COLUMN_INDEX, String.class),
                        record.get(STREAM_FLOWS_INTO_STREAM_COLUMN_INDEX, String.class),
                        record.get(STREAM_FLOWS_INTO_STATION_COLUMN_INDEX, Double.class),
                        Bank.fromCode(record.get(STREAM_FLOWS_INTO_BANK_COLUMN_INDEX, String.class)),
                        stationUnits))
                .withDivertsFromStreamNode(buildStreamNode(
                        record.get(STREAM_OFFICE_ID_COLUMN_INDEX, String.class),
                        record.get(STREAM_DIVERTS_FROM_STREAM_COLUMN_INDEX, String.class),
                        record.get(STREAM_DIVERTS_FROM_STATION_COLUMN_INDEX, Double.class),
                        Bank.fromCode(record.get(STREAM_DIVERTS_FROM_BANK_COLUMN_INDEX, String.class)),
                        stationUnits))
                .withLength(record.get(STREAM_STREAM_LENGTH_COLUMN_INDEX, Double.class))
                .withAverageSlope(record.get(STREAM_AVERAGE_SLOPE_COLUMN_INDEX, Double.class))
                .withComment(record.get(STREAM_COMMENTS_COLUMN_INDEX, String.class))
                .withLengthUnits(stationUnits)
                .withSlopeUnits(DB_STREAM_SLOPE_UNITS)
                .build();
    }

    static StreamNode buildStreamNode(String officeId, String streamId, Double station, Bank bank, String stationUnits) {
        StreamNode retVal = null;
        if(streamId != null) {
            retVal = new StreamNode.Builder()
                    .withStreamId(new CwmsId.Builder()
                            .withName(streamId)
                            .withOfficeId(officeId)
                            .build())
                    .withStation(station)
                    .withBank(bank)
                    .withStationUnits(stationUnits)
                    .build();
        }
        return retVal;
    }
}