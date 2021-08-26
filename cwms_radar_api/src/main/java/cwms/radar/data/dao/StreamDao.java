package cwms.radar.data.dao;

import cwms.radar.data.dto.basinconnectivity.Stream;
import cwms.radar.data.dto.basinconnectivity.StreamBuilder;
import cwms.radar.data.dto.basinconnectivity.StreamLocation;
import cwms.radar.data.dto.basinconnectivity.StreamReach;
import cwms.radar.data.util.BasinUnitsConverter;
import org.jooq.DSLContext;
import usace.cwms.db.dao.ifc.stream.StreamT;
import usace.cwms.db.jooq.dao.CwmsDbStreamJooq;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.Set;


public class StreamDao extends JooqDao<Stream>
{
    public StreamDao(DSLContext dsl)
    {
        super(dsl);
    }

    public Stream getStream(String streamId, String unitSystem, String officeId) throws SQLException
    {
        String pStationUnit = "km";
        String stationUnitOut = "km";
        if(unitSystem.equals("EN"))
        {
            stationUnitOut = "mi";
        }
        String pOfficeId = null;
        if (officeId != null)
        {
            pOfficeId = officeId;
        }
        CwmsDbStreamJooq streamJooq = new CwmsDbStreamJooq();
        Connection c = dsl.configuration().connectionProvider().acquire();
        StreamT streamResult = streamJooq.retrieveStreamF(c, streamId, pStationUnit, pOfficeId);
        Double streamLength = BasinUnitsConverter.convertUnits(streamResult.getLength(), pStationUnit, stationUnitOut);
        return new StreamBuilder(streamId, streamResult.getStartsDownstream(), streamLength, streamResult.getOfficeId())
                .withDivertingStreamId( streamResult.getDivertsFromStream())
                    .withDiversionStation(BasinUnitsConverter.convertUnits(streamResult.getDivertsFromStation(), pStationUnit, stationUnitOut))
                    .withDiversionBank(streamResult.getDivertsFromBank())
                .withReceivingStreamId(streamResult.getFlowsIntoStream())
                    .withConfluenceStation(BasinUnitsConverter.convertUnits(streamResult.getFlowsIntoStation(), pStationUnit, stationUnitOut))
                    .withConfluenceBank(streamResult.getFlowsIntoBank())
                .withComment(streamResult.getComments())
                .withAverageSlope(streamResult.getAverageSlope())
                .withStreamLocations(getStreamLocationsOnStream(streamId, unitSystem, officeId))
                .withTributaries(getTributaries(streamId, unitSystem, pStationUnit, stationUnitOut, officeId))
                .withStreamReaches(getReaches(streamId, officeId))
                .build();
    }

    private Set<StreamLocation> getStreamLocationsOnStream(String streamId, String unitSystem, String officeId) throws SQLException
    {
        StreamLocationDao streamLocationDao = new StreamLocationDao(dsl);
        return streamLocationDao.getStreamLocations(streamId, unitSystem, officeId);
    }

    private Set<StreamReach> getReaches(String streamId, String officeId) throws SQLException
    {
        StreamReachDao streamReachDao = new StreamReachDao(dsl);
        return streamReachDao.getReachesOnStream(streamId, officeId);
    }

    private Set<Stream> getTributaries(String streamId, String unitSystem, String stationUnitIn, String stationUnitOut, String officeId)  throws SQLException
    {
        CwmsDbStreamJooq streamJooq = new CwmsDbStreamJooq();
        Connection c = dsl.configuration().connectionProvider().acquire();
        ResultSet rs = streamJooq.catStreams(c, null, "km", null, streamId, null, null, null, null, null, null, null, null, null, null, null, null, officeId);
        return buildStreamsFromResultSet(rs, streamId, unitSystem, stationUnitIn, stationUnitOut);
    }

    private Set<Stream> buildStreamsFromResultSet(ResultSet result, String parentStreamId, String unitSystem, String stationUnitIn, String stationUnitOut) throws SQLException
    {
        Set<Stream> retVal = new HashSet<>();

        while (result.next())
        {
            String officeId = result.getString(1);
            String streamId = result.getString(2);
            String receivingStreamId = result.getString(4);
            if(receivingStreamId != null && receivingStreamId.equals(parentStreamId))
            {
                Double confluenceStation = null;
                Object confluenceObject = result.getObject(5);
                if (confluenceObject instanceof Double)
                {
                    confluenceStation = BasinUnitsConverter.convertUnits((Double) confluenceObject, stationUnitIn, stationUnitOut);
                }
                String confluenceBank = result.getString(6);
                String divertingStreamId = result.getString(7);
                Double diversionStation = null;
                Object diversionObject = result.getObject(8);
                if (diversionObject instanceof Double)
                {
                    diversionStation = BasinUnitsConverter.convertUnits((Double) diversionObject, stationUnitIn, stationUnitOut);
                }
                String diversionBank = result.getString(9);
                Double streamLength = toDouble(result.getBigDecimal(10));
                streamLength = BasinUnitsConverter.convertUnits((Double) streamLength, stationUnitIn, stationUnitOut);
                boolean startsDownstream = result.getBoolean(3);
                Double averageSlope = toDouble(result.getBigDecimal(11));
                String comment = result.getString(12);
                Stream stream = new StreamBuilder(streamId, startsDownstream, streamLength, officeId)
                        .withDivertingStreamId(divertingStreamId)
                        .withDiversionStation(diversionStation)
                        .withDiversionBank(diversionBank)
                        .withReceivingStreamId(receivingStreamId)
                        .withConfluenceStation(confluenceStation)
                        .withConfluenceBank(confluenceBank)
                        .withComment(comment)
                        .withAverageSlope(averageSlope)
                        .withStreamLocations(getStreamLocationsOnStream(streamId, unitSystem, officeId))
                        .withTributaries(getTributaries(streamId, unitSystem, stationUnitIn, stationUnitOut, officeId))
                        .withStreamReaches(getReaches(streamId, officeId))
                        .build();
                retVal.add(stream);
            }
        }

        return retVal;
    }
}
