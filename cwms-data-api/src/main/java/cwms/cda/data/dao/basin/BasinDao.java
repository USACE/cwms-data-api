package cwms.cda.data.dao.basin;

import cwms.cda.data.dao.JooqDao;
import cwms.cda.data.dto.LocationIdentifier;
import cwms.cda.data.dto.basin.Basin;
import cwms.cda.api.enums.UnitSystem;
import cwms.cda.api.enums.Unit;
import usace.cwms.db.jooq.dao.CwmsDbBasinJooq;
import java.sql.SQLException;
import java.sql.ResultSet;
import java.util.List;
import java.util.ArrayList;
import org.jooq.DSLContext;

public class BasinDao extends JooqDao<Basin> {
    public BasinDao(DSLContext dsl) {
        super(dsl);
    }

    public List<Basin> getAllBasins(String unitSystem, String officeId) throws SQLException {
        List<Basin> retVal = new ArrayList<>();
        CwmsDbBasinJooq basinJooq = new CwmsDbBasinJooq();
        String areaUnitIn = UnitSystem.EN.value().equals(unitSystem)
                ? Unit.SQUARE_MILES.getValue() : Unit.SQUARE_KILOMETERS.getValue();
        try {
            connection(dsl, c -> {
                try (ResultSet rs = basinJooq.catBasins(c, null, null, null, areaUnitIn, officeId)) {
                    retVal.addAll(buildBasinsFromResultSet(rs, unitSystem));
                }
            });
        } catch (Exception ex) {
            throw new SQLException(ex);
        }
        return retVal;
    }

    public Basin getBasin(LocationIdentifier basinId, String unitSystem) throws SQLException {
        CwmsDbBasinJooq basinJooq = new CwmsDbBasinJooq();

        String[] pParentBasinId = new String[1];
        Double[] pSortOrder = new Double[1];
        String[] pPrimaryStreamId = new String[1];
        Double[] pTotalDrainageArea = new Double[1];
        Double[] pContributingDrainageArea = new Double[1];
        String pBasinId = "";
        String pOfficeId = "";

        String areaUnitIn = UnitSystem.EN.value().equals(unitSystem)
                ? Unit.SQUARE_MILES.getValue() : Unit.SQUARE_KILOMETERS.getValue();

        connection(dsl, c -> basinJooq.retrieveBasin(c, pParentBasinId, pSortOrder,
                pPrimaryStreamId, pTotalDrainageArea, pContributingDrainageArea, pBasinId,
                areaUnitIn, pOfficeId));

        return new Basin.Builder()
                .withBasinId(new LocationIdentifier.Builder()
                        .withLocationId(pBasinId)
                        .withOfficeId(pOfficeId)
                        .build())
                .withBasinArea(pTotalDrainageArea[0])
                .withContributingArea(pContributingDrainageArea[0])
                .withParentBasinId(new LocationIdentifier.Builder()
                        .withLocationId(pParentBasinId[0]).
                        withOfficeId(pOfficeId)
                        .build())
                .withSortOrder(pSortOrder[0])
                .build();
    }

    private List<Basin> buildBasinsFromResultSet(ResultSet rs, String unitSystem) throws SQLException {
        List<Basin> basins = new ArrayList<>();
        while (rs.next()) {
            Basin basin = buildBasinFromRow(rs, unitSystem);
            basins.add(basin);
        }
        return basins;
    }

    private Basin buildBasinFromRow(ResultSet rs, String unitSystem) throws SQLException {
        String basinId = rs.getString("BASIN_ID");
        String officeId = rs.getString("OFFICE_ID");
        String parentBasinId = rs.getString("PARENT_BASIN_ID");
        Double sortOrder = rs.getDouble("SORT_ORDER");
        String primaryStreamId = rs.getString("PRIMARY_STREAM_ID");
        Double basinArea = rs.getDouble("TOTAL_DRAINAGE_AREA");
        Double contributingArea = rs.getDouble("CONTRIBUTING_DRAINAGE_AREA");
        return new Basin.Builder()
                .withBasinId(new LocationIdentifier.Builder()
                        .withLocationId(basinId)
                        .withOfficeId(officeId)
                        .build())
                .withBasinArea(basinArea)
                .withContributingArea(contributingArea)
                .withParentBasinId(new LocationIdentifier.Builder()
                        .withLocationId(parentBasinId)
                        .withOfficeId(officeId)
                        .build())
                .withSortOrder(sortOrder)
                .build();
    }

}
