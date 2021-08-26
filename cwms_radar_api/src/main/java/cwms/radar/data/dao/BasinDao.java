package cwms.radar.data.dao;

import cwms.radar.data.dto.basinconnectivity.Basin;
import cwms.radar.data.dto.basinconnectivity.BasinBuilder;
import cwms.radar.data.dto.basinconnectivity.Stream;
import cwms.radar.data.util.BasinUnitsConverter;
import org.jooq.DSLContext;
import usace.cwms.db.jooq.dao.CwmsDbBasinJooq;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public class BasinDao extends JooqDao<Basin>
{
    public BasinDao(DSLContext dsl)
    {
        super(dsl);
    }

    public List<Basin> getAllBasins(String unitSystem, String officeId) throws SQLException
    {
        CwmsDbBasinJooq basinJooq = new CwmsDbBasinJooq();
        String areaUnitIn = "km2";
        String areaUnitOut = "km2";
        if(unitSystem.equals("EN"))
        {
            areaUnitOut = "mi2";
        }
        Connection c = dsl.configuration().connectionProvider().acquire();
        ResultSet rs = basinJooq.catBasins(c, null, null, null, areaUnitIn, officeId);
        return buildBasinsFromResultSet(rs, unitSystem, areaUnitIn, areaUnitOut);
    }

    public Basin getBasin(String basinId, String unitSystem, String officeId) throws SQLException
    {
        CwmsDbBasinJooq basinJooq = new CwmsDbBasinJooq();
        String[] pParentBasinId = new String[1];
        Double[] pSortOrder = new Double[1];
        String[] pPrimaryStreamId = new String[1];
        Double[] pTotalDrainageArea = new Double[1];
        Double[] pContributingDrainageArea = new Double[1];
        String areaUnitIn = "km2";
        String areaUnitOut = "km2";
        if(unitSystem.equals("EN"))
        {
            areaUnitOut = "mi2";
        }
        Connection c = dsl.configuration().connectionProvider().acquire();
        basinJooq.retrieveBasin(c, pParentBasinId, pSortOrder, pPrimaryStreamId, pTotalDrainageArea, pContributingDrainageArea, basinId, areaUnitIn, officeId);
        Basin retval = new BasinBuilder(basinId, officeId)
                .withBasinArea(BasinUnitsConverter.convertUnits(pTotalDrainageArea[0], areaUnitIn, areaUnitOut))
                .withContributingArea(BasinUnitsConverter.convertUnits(pContributingDrainageArea[0], areaUnitIn, areaUnitOut))
                .withParentBasinId(pParentBasinId[0])
                .withSortOrder(pSortOrder[0])
                .build();
        if(pPrimaryStreamId[0] != null)
        {
            StreamDao streamDao = new StreamDao(dsl);
            Stream primaryStream = streamDao.getStream(pPrimaryStreamId[0], unitSystem, officeId);
            retval = new BasinBuilder(retval).withPrimaryStream(primaryStream).build();
        }
        return retval;
    }

    @Override
    public Optional getByUniqueName(String uniqueName, Optional limitToOffice)
    {
        return Optional.empty();
    }

    @Override
    public List getAll(Optional limitToOffice)
    {
        return null;
    }

    private List<Basin> buildBasinsFromResultSet(ResultSet rs, String unitSystem, String areaUnitIn, String areaUnitOut) throws SQLException
    {
        ArrayList retval = new ArrayList();
        while(rs.next())
        {
            String officeId = rs.getString(1);
            String basinId = rs.getString(2);
            String parentBasinId = rs.getString(3);
            Double sortOrder = rs.getDouble(4);
            String primaryStreamId = rs.getString(5);
            Double basinArea = rs.getDouble(6);
            Double contributingArea = rs.getDouble(7);
            Basin basin = new BasinBuilder(basinId, officeId)
                    .withBasinArea(BasinUnitsConverter.convertUnits(basinArea, areaUnitIn, areaUnitOut))
                    .withContributingArea(BasinUnitsConverter.convertUnits(contributingArea, areaUnitIn, areaUnitOut))
                    .withParentBasinId(parentBasinId)
                    .withSortOrder(sortOrder)
                    .build();
            if(primaryStreamId != null)
            {
                StreamDao streamDao = new StreamDao(dsl);
                Stream primaryStream = streamDao.getStream(primaryStreamId, unitSystem, officeId);
                basin = new BasinBuilder(basin).withPrimaryStream(primaryStream).build();
            }
            retval.add(basin);
        }

        return retval;
    }

}
