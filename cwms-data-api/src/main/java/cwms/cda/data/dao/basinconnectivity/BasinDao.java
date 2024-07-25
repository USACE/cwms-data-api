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
import usace.cwms.db.jooq.dao.CwmsDbBasinJooq;

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

    public Basin getBasin(String basinId, String unitSystem, String officeId) throws SQLException {
        CwmsDbBasinJooq basinJooq = new CwmsDbBasinJooq();

        String[] pParentBasinId = new String[1];
        Double[] pSortOrder = new Double[1];
        String[] pPrimaryStreamId = new String[1];
        Double[] pTotalDrainageArea = new Double[1];
        Double[] pContributingDrainageArea = new Double[1];
        String areaUnitIn = UnitSystem.EN.value().equals(unitSystem)
                ? Unit.SQUARE_MILES.getValue() : Unit.SQUARE_KILOMETERS.getValue();

        connection(dsl, c -> basinJooq.retrieveBasin(c, pParentBasinId, pSortOrder,
                pPrimaryStreamId, pTotalDrainageArea, pContributingDrainageArea, basinId,
                areaUnitIn, officeId));

        Basin retVal = new Basin.Builder(basinId, officeId)
                .withBasinArea(pTotalDrainageArea[0])
                .withContributingArea(pContributingDrainageArea[0])
                .withParentBasinId(pParentBasinId[0])
                .withSortOrder(pSortOrder[0])
                .build();
        if (pPrimaryStreamId[0] != null) {
            StreamDao streamDao = new StreamDao(dsl);
            Stream primaryStream = streamDao.getStream(pPrimaryStreamId[0], unitSystem, officeId);
            retVal = new Basin.Builder(retVal).withPrimaryStream(primaryStream).build();
        }
        return retVal;
    }

    private List<Basin> buildBasinsFromResultSet(ResultSet rs, String unitSystem) throws SQLException {
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
