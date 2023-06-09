package cwms.cda.data.dao;

import cwms.cda.api.enums.Unit;
import cwms.cda.api.enums.UnitSystem;
import cwms.cda.data.dto.basinconnectivity.StreamLocation;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import org.jooq.DSLContext;
import usace.cwms.db.jooq.dao.CwmsDbStreamJooq;

public class StreamLocationDao extends JooqDao<StreamLocation> {
    public StreamLocationDao(DSLContext dsl) {
        super(dsl);
    }

    /**
     * @param streamId - stream containing stream locations that are being retrieved
     * @return list of stream locations on stream
     */
    public Set<StreamLocation> getStreamLocations(String streamId, String unitSystem,
                                                  String officeId) throws SQLException {
        String pStreamIdMaskIn = streamId == null ? "*" : streamId;
        String pLocationIdMaskIn = "*";
        String pStationUnitIn = UnitSystem.EN.value().equalsIgnoreCase(unitSystem)
                ? Unit.MILE.getValue() : Unit.KILOMETER.getValue();
        String pStageUnitIn = UnitSystem.EN.value().equalsIgnoreCase(unitSystem)
                ? Unit.FEET.getValue() : Unit.METER.getValue();
        String pAreaUnitIn = UnitSystem.EN.value().equalsIgnoreCase(unitSystem)
                ? Unit.SQUARE_MILES.getValue() : Unit.SQUARE_KILOMETERS.getValue();
        CwmsDbStreamJooq streamJooq = new CwmsDbStreamJooq();
        AtomicReference<ResultSet> resultSetRef = new AtomicReference<>();
        connection(dsl, c -> resultSetRef.set(streamJooq.catStreamLocations(c, pStreamIdMaskIn,
                pLocationIdMaskIn, pStationUnitIn, pStageUnitIn, pAreaUnitIn, officeId)));
        return buildStreamLocations(resultSetRef.get());
    }

    private Set<StreamLocation> buildStreamLocations(ResultSet rs) throws SQLException {
        Set<StreamLocation> retVal = new HashSet<>();
        while (rs.next()) {
            String locationId = rs.getString("LOCATION_ID");
            String officeId = rs.getString("OFFICE_ID");
            String streamId = rs.getString("STREAM_ID");
            Double station = toDouble(rs.getBigDecimal("STATION"));
            Double publishedStation = toDouble(rs.getBigDecimal("PUBLISHED_STATION"));
            Double navigationStation = toDouble(rs.getBigDecimal("NAVIGATION_STATION"));
            Double lowestMeasurableStage = toDouble(rs.getBigDecimal("LOWEST_MEASURABLE_STAGE"));
            Double totalDrainageArea = toDouble(rs.getBigDecimal("DRAINAGE_AREA"));
            Double ungagedDrainageArea = toDouble(rs.getBigDecimal("UNGAGED_DRAINAGE_AREA"));
            String bank = rs.getString("BANK");
            StreamLocation loc = new StreamLocation.Builder(locationId, streamId, station, bank,
                    officeId)
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

    public Set<StreamLocation> getAllStreamLocations(String unitSystem, String officeId) throws SQLException {
        return getStreamLocations(null, unitSystem, officeId);
    }

}
