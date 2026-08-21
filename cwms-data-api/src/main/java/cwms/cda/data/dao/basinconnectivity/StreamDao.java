package cwms.cda.data.dao.basinconnectivity;

import cwms.cda.api.enums.Unit;
import cwms.cda.api.enums.UnitSystem;
import cwms.cda.data.dao.JooqDao;
import cwms.cda.data.dto.basinconnectivity.Stream;
import cwms.cda.data.dto.basinconnectivity.StreamLocation;
import cwms.cda.data.dto.basinconnectivity.StreamReach;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.LinkedHashSet;
import java.util.Set;
import org.jetbrains.annotations.Nullable;
import org.jooq.DSLContext;
import org.jooq.Record;
import org.jooq.Result;
import usace.cwms.db.jooq.codegen.packages.CWMS_STREAM_PACKAGE;
import usace.cwms.db.jooq.codegen.packages.cwms_stream.RETRIEVE_STREAM;


public class StreamDao extends JooqDao<Stream> {
    public StreamDao(DSLContext dsl) {
        super(dsl);
    }

    public Stream getStream(String streamId, String unitSystem, String officeId) {
        String pStationUnit = UnitSystem.EN.value().equals(unitSystem) ? Unit.MILE.getValue() :
                Unit.KILOMETER.getValue();

        return connectionResult(dsl, c -> {
            var configuration = getDslContext(c, officeId).configuration();
            RETRIEVE_STREAM stream = CWMS_STREAM_PACKAGE.call_RETRIEVE_STREAM(configuration, streamId, pStationUnit, officeId);
            return new Stream.Builder(streamId, parseBool(stream.getP_STATIONING_STARTS_DS()),
                    stream.getP_LENGTH(), officeId)
                    .withDivertingStreamId(stream.getP_DIVERTS_FROM_STREAM())
                    .withDiversionStation(stream.getP_DIVERTS_FROM_STATION())
                    .withDiversionBank(stream.getP_DIVERTS_FROM_BANK())
                    .withReceivingStreamId(stream.getP_FLOWS_INTO_STREAM())
                    .withConfluenceStation(stream.getP_FLOWS_INTO_STATION())
                    .withConfluenceBank(stream.getP_FLOWS_INTO_BANK())
                    .withComment(stream.getP_COMMENTS())
                    .withAverageSlope(stream.getP_AVERAGE_SLOPE())
                    .withStreamLocations(getStreamLocationsOnStream(streamId, unitSystem, officeId))
                    .withTributaries(getTributaries(streamId, unitSystem, officeId))
                    .withStreamReaches(getReaches(streamId, officeId))
                    .build();

        });
    }

    private Set<StreamLocation> getStreamLocationsOnStream(String streamId, String unitSystem,
                                                           String officeId) {
        StreamLocationDao streamLocationDao = new StreamLocationDao(dsl);
        return streamLocationDao.getStreamLocations(streamId, unitSystem, officeId);
    }

    private Set<StreamReach> getReaches(String streamId, String officeId) {
        StreamReachDao streamReachDao = new StreamReachDao(dsl);
        return streamReachDao.getReachesOnStream(streamId, officeId);
    }

    private Set<Stream> getTributaries(String streamId, String unitSystem, String officeId)  {
        return connectionResult(dsl, c -> {
            String pStationUnit = UnitSystem.EN.value().equals(unitSystem)
                    ? Unit.MILE.getValue() : Unit.KILOMETER.getValue();
            var configuration = getDslContext(c, officeId).configuration();
            Result<Record> rs = CWMS_STREAM_PACKAGE.call_CAT_STREAMS(configuration, null, pStationUnit, null, null,
                null, null, null, null, null, null, null,
                null, null, null, null, null, officeId);
            return buildStreamsFromResultSet(rs.intoResultSet(), streamId, unitSystem);
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
