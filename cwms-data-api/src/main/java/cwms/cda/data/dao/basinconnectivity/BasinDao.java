package cwms.cda.data.dao.basinconnectivity;

import cwms.cda.api.enums.Unit;
import cwms.cda.api.enums.UnitSystem;
import cwms.cda.data.dao.JooqDao;
import cwms.cda.data.dto.basinconnectivity.Basin;
import cwms.cda.data.dto.basinconnectivity.Stream;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import org.jooq.DSLContext;
import org.jooq.Record;
import org.jooq.Result;
import usace.cwms.db.jooq.codegen.packages.CWMS_BASIN_PACKAGE;
import usace.cwms.db.jooq.codegen.packages.cwms_basin.RETRIEVE_BASIN;

public class BasinDao extends JooqDao<Basin> {
    public BasinDao(DSLContext dsl) {
        super(dsl);
    }

    public List<Basin> getAllBasins(String unitSystem, String officeId) throws SQLException {
        List<Basin> retVal = new ArrayList<>();
        String areaUnitIn = UnitSystem.EN.value().equals(unitSystem)
                ? Unit.SQUARE_MILES.getValue() : Unit.SQUARE_KILOMETERS.getValue();
        try {
            connection(dsl, c -> {
                Result<Record>
                    rs = CWMS_BASIN_PACKAGE.call_CAT_BASINS(getDslContext(c, officeId).configuration(), null, null, null, areaUnitIn, officeId);
                retVal.addAll(buildBasinsFromRecords(rs.intoResultSet(), unitSystem));
            });
        } catch (Exception ex) {
            throw new SQLException(ex);
        }
        return retVal;
    }

    public Basin getBasin(String basinId, String unitSystem, String officeId) {
        String areaUnitIn = UnitSystem.EN.value().equals(unitSystem)
                ? Unit.SQUARE_MILES.getValue() : Unit.SQUARE_KILOMETERS.getValue();

        RETRIEVE_BASIN basin = connectionResult(dsl, c ->
            CWMS_BASIN_PACKAGE.call_RETRIEVE_BASIN(getDslContext(c, officeId).configuration(), basinId, areaUnitIn, officeId));

        Basin retVal = new Basin.Builder(basinId, officeId)
                .withBasinArea(basin.getP_TOTAL_DRAINAGE_AREA())
                .withContributingArea(basin.getP_CONTRIBUTING_DRAINAGE_AREA())
                .withParentBasinId(basin.getP_PARENT_BASIN_ID())
                .withSortOrder(basin.getP_SORT_ORDER())
                .build();
        String primaryStreamId = basin.getP_PRIMARY_STREAM_ID();
        if (primaryStreamId != null) {
            StreamDao streamDao = new StreamDao(dsl);
            Stream primaryStream = streamDao.getStream(primaryStreamId, unitSystem, officeId);
            retVal = new Basin.Builder(retVal).withPrimaryStream(primaryStream).build();
        }
        return retVal;
    }

    private List<Basin> buildBasinsFromRecords(ResultSet rs, String unitSystem) throws SQLException {
        List<Basin> retVal = new ArrayList<>();
        while (rs.next()) {
            Basin basin = buildBasinFromRow(rs, unitSystem);
            retVal.add(basin);
        }

        return retVal;
    }

    private Basin buildBasinFromRow(ResultSet rs, String unitSystem) throws SQLException {
        String officeId = rs.getString("OFFICE_ID");
        String basinId = rs.getString("BASIN_ID");
        String parentBasinId = rs.getString("PARENT_BASIN_ID");
        Double sortOrder = rs.getDouble("SORT_ORDER");
        String primaryStreamId = rs.getString("PRIMARY_STREAM_ID");
        Double basinArea = rs.getDouble("TOTAL_DRAINAGE_AREA");
        Double contributingArea = rs.getDouble("CONTRIBUTING_DRAINAGE_AREA");
        Basin basin = new Basin.Builder(basinId, officeId)
                .withBasinArea(basinArea)
                .withContributingArea(contributingArea)
                .withParentBasinId(parentBasinId)
                .withSortOrder(sortOrder)
                .build();
        if (primaryStreamId != null) {
            StreamDao streamDao = new StreamDao(dsl);
            Stream primaryStream = streamDao.getStream(primaryStreamId, unitSystem, officeId);
            basin = new Basin.Builder(basin).withPrimaryStream(primaryStream).build();
        }
        return basin;
    }

}
