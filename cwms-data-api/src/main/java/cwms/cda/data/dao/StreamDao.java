package cwms.cda.data.dao;

import cwms.cda.api.enums.Unit;
import cwms.cda.api.enums.UnitSystem;
import cwms.cda.data.dto.basinconnectivity.Stream;
import cwms.cda.data.dto.basinconnectivity.StreamLocation;
import cwms.cda.data.dto.basinconnectivity.StreamReach;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.LinkedHashSet;
import java.util.Set;
import org.jetbrains.annotations.Nullable;
import org.jooq.DSLContext;
import usace.cwms.db.dao.ifc.stream.StreamT;
import usace.cwms.db.jooq.dao.CwmsDbStreamJooq;


public class StreamDao extends JooqDao<Stream> {
    public StreamDao(DSLContext dsl) {
        super(dsl);
    }

    public Stream getStream(String streamId, String unitSystem, String officeId) {
        String pStationUnit = UnitSystem.EN.value().equals(unitSystem) ? Unit.MILE.getValue() :
                Unit.KILOMETER.getValue();
        CwmsDbStreamJooq streamJooq = new CwmsDbStreamJooq();

        return connectionResult(dsl, c -> {
            StreamT streamResult = streamJooq.retrieveStreamF(c, streamId, pStationUnit, officeId);
            return new Stream.Builder(streamId, streamResult.getStartsDownstream(),
                    streamResult.getLength(), streamResult.getOfficeId())
                    .withDivertingStreamId(streamResult.getDivertsFromStream())
                    .withDiversionStation(streamResult.getDivertsFromStation())
                    .withDiversionBank(streamResult.getDivertsFromBank())
                    .withReceivingStreamId(streamResult.getFlowsIntoStream())
                    .withConfluenceStation(streamResult.getFlowsIntoStation())
                    .withConfluenceBank(streamResult.getFlowsIntoBank())
                    .withComment(streamResult.getComments())
                    .withAverageSlope(streamResult.getAverageSlope())
                    .withStreamLocations(getStreamLocationsOnStream(streamId, unitSystem, officeId))
                    .withTributaries(getTributaries(streamId, unitSystem, officeId))
                    .withStreamReaches(getReaches(streamId, officeId))
                    .build();
        });
    }

    private Set<StreamLocation> getStreamLocationsOnStream(String streamId, String unitSystem,
                                                           String officeId) throws SQLException {
        StreamLocationDao streamLocationDao = new StreamLocationDao(dsl);
        return streamLocationDao.getStreamLocations(streamId, unitSystem, officeId);
    }

    private Set<StreamReach> getReaches(String streamId, String officeId) throws SQLException {
        StreamReachDao streamReachDao = new StreamReachDao(dsl);
        return streamReachDao.getReachesOnStream(streamId, officeId);
    }

    private Set<Stream> getTributaries(String streamId, String unitSystem, String officeId)  {
        CwmsDbStreamJooq streamJooq = new CwmsDbStreamJooq();

        return connectionResult(dsl, c -> {
            String pStationUnit = UnitSystem.EN.value().equals(unitSystem)
                    ? Unit.MILE.getValue() : Unit.KILOMETER.getValue();
            try (ResultSet rs = streamJooq.catStreams(c, null, pStationUnit, null,
                    streamId, null, null, null,
                    null, null, null, null,
                    null, null, null, null, null, officeId)) {

                return buildStreamsFromResultSet(rs, streamId, unitSystem);
            }
        });
    }

    private Set<Stream> buildStreamsFromResultSet(ResultSet result, String parentStreamId,
                                                  String unitSystem) throws SQLException {
        Set<Stream> retVal = new LinkedHashSet<>();

        while (result.next()) {
            Stream stream = buildStreamFromRow(result, parentStreamId, unitSystem);
            if (stream != null) {
                retVal.add(stream);
            }
        }

        return retVal;
    }

    private @Nullable Stream buildStreamFromRow(ResultSet result, String parentStreamId, String unitSystem) throws SQLException {
        Stream stream = null;
        String officeId = result.getString("OFFICE_ID");
        String streamId = result.getString("STREAM_ID");
        String receivingStreamId = result.getString("FLOWS_INTO_STREAM");
        if (receivingStreamId != null && receivingStreamId.equals(parentStreamId)) {
            Double confluenceStation = null;
            Object confluenceObject = result.getObject("FLOWS_INTO_STATION");
            if (confluenceObject instanceof Double) {
                confluenceStation = (Double) confluenceObject;
            }
            String confluenceBank = result.getString("FLOWS_INTO_BANK");
            String divertingStreamId = result.getString("DIVERTS_FROM_STREAM");
            Double diversionStation = null;
            Object diversionObject = result.getObject("DIVERTS_FROM_STATION");
            if (diversionObject instanceof Double) {
                diversionStation = (Double) diversionObject;
            }
            String diversionBank = result.getString("DIVERTS_FROM_BANK");
            Double streamLength = toDouble(result.getBigDecimal("STREAM_LENGTH"));
            boolean startsDownstream = result.getBoolean("STATIONING_STARTS_DS");
            Double averageSlope = toDouble(result.getBigDecimal("AVERAGE_SLOPE"));
            String comment = result.getString("COMMENTS");
            stream = new Stream.Builder(streamId, startsDownstream, streamLength,
                    officeId)
                    .withDivertingStreamId(divertingStreamId)
                    .withDiversionStation(diversionStation)
                    .withDiversionBank(diversionBank)
                    .withReceivingStreamId(receivingStreamId)
                    .withConfluenceStation(confluenceStation)
                    .withConfluenceBank(confluenceBank)
                    .withComment(comment)
                    .withAverageSlope(averageSlope)
                    .withStreamLocations(getStreamLocationsOnStream(streamId, unitSystem,
                            officeId))
                    .withTributaries(getTributaries(streamId, unitSystem, officeId))
                    .withStreamReaches(getReaches(streamId, officeId))
                    .build();

        }
        return stream;
    }
}
