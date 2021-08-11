package cwms.radar.data.dao;

import cwms.radar.data.dto.Basin;
import cwms.radar.data.dto.Stream;
import org.jooq.DSLContext;
import usace.cwms.db.jooq.codegen.packages.cwms_basin.RETRIEVE_BASIN;

import static usace.cwms.db.jooq.codegen.packages.CWMS_BASIN_PACKAGE.*;

public class BasinDao extends JooqDao<Basin>
{
    public BasinDao(DSLContext dsl)
    {
        super(dsl);
    }

    public Basin getBasin(String basinId, String unit, String officeId)
    {
        RETRIEVE_BASIN basin = call_RETRIEVE_BASIN(dsl.configuration(), basinId, unit, officeId);
        String primaryStreamId = basin.getP_PRIMARY_STREAM_ID();
        StreamDao streamDao = new StreamDao(dsl);
        Stream primaryStream = streamDao.getStream(primaryStreamId, unit, officeId);
        return new Basin(basinId, primaryStream);
    }
}
