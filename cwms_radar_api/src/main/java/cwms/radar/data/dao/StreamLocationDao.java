package cwms.radar.data.dao;

import cwms.radar.data.dto.basinconnectivity.StreamLocation;
import cwms.radar.data.dto.basinconnectivity.StreamLocationBuilder;
import cwms.radar.data.util.BasinUnitsConverter;
import org.jooq.DSLContext;
import usace.cwms.db.jooq.dao.CwmsDbStreamJooq;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.Set;

public class StreamLocationDao extends JooqDao<StreamLocation>
{
    public StreamLocationDao(DSLContext dsl)
    {
        super(dsl);
    }

    /**
     *
     * @param streamId - stream containing stream locations that are being retrieved
     * @return list of stream locations on stream
     */
    public Set<StreamLocation> getStreamLocations(String streamId, String unitSystem, String officeId) throws SQLException
    {
        String pStreamIdMaskIn = "*";
        String pLocationIdMaskIn = "*";
        String pStationUnitIn = "km";
        String pStageUnitIn = "ft";
        String pAreaUnitIn = "km2";
        String pStationUnitOut = "km";
        String pStageUnitOut = "ft";
        String pAreaUnitOut = "km2";
        if(unitSystem.equalsIgnoreCase("EN"))
        {
            pStationUnitOut = "mi";
            pStageUnitOut = "m";
            pAreaUnitOut = "mi2";
        }
        if(streamId != null)
        {
            pStreamIdMaskIn = streamId;
        }
        Connection c = dsl.configuration().connectionProvider().acquire();
        CwmsDbStreamJooq streamJooq = new CwmsDbStreamJooq();
        ResultSet rs = streamJooq.catStreamLocations(c, pStreamIdMaskIn, pLocationIdMaskIn, pStationUnitIn, pStageUnitIn, pAreaUnitIn, officeId);
        return buildStreamLocations(rs, pStationUnitOut, pStageUnitOut, pAreaUnitOut);
    }

    private Set<StreamLocation> buildStreamLocations(ResultSet rs, String stationUnitOut, String stageUnitOut, String areaUnitOut) throws SQLException
    {
        Set<StreamLocation> retVal = new HashSet<>();
        String stationUnitIn = "km";
        String stageUnitIn = "ft";
        String areaUnitIn = "km2";
        while(rs.next())
        {
            String locationId = rs.getString(3);
            String officeId = rs.getString(1);
            String streamId = rs.getString(2);
            Double station = toDouble(rs.getBigDecimal(4));
            station = BasinUnitsConverter.convertUnits(station, stationUnitIn, stationUnitOut);
            Double publishedStation = toDouble(rs.getBigDecimal(5));
            publishedStation = BasinUnitsConverter.convertUnits(publishedStation, stationUnitIn, stationUnitOut);
            Double navigationStation = toDouble(rs.getBigDecimal(6));
            navigationStation = BasinUnitsConverter.convertUnits(navigationStation, stationUnitIn, stationUnitOut);
            Double lowestMeasurableStage = toDouble(rs.getBigDecimal(7));
            lowestMeasurableStage = BasinUnitsConverter.convertUnits(lowestMeasurableStage, stageUnitIn, stageUnitOut);
            Double totalDrainageArea = toDouble(rs.getBigDecimal(9));
            totalDrainageArea = BasinUnitsConverter.convertUnits(totalDrainageArea, areaUnitIn, areaUnitOut);
            Double ungagedDrainageArea = toDouble(rs.getBigDecimal(10));
            ungagedDrainageArea = BasinUnitsConverter.convertUnits(ungagedDrainageArea, areaUnitIn, areaUnitOut);
            String bank = rs.getString(7);
            StreamLocation loc = new StreamLocationBuilder(locationId, streamId, station, bank, officeId)
                    .withPublishedStation(publishedStation)
                    .withNavigationStation(navigationStation)
                    .withLowestMeasurableStage(lowestMeasurableStage)
                    .withTotalDrainageArea(totalDrainageArea)
                    .withUngagedDrainageArea(ungagedDrainageArea)
                    .build();
            retVal.add(loc);
        }

        return retVal;
    }

    public Set<StreamLocation> getAllStreamLocations(String unitSystem, String officeId) throws SQLException
    {
        return getStreamLocations(null, unitSystem, officeId);
    }

}
