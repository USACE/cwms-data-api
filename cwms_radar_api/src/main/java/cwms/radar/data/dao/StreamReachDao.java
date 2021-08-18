package cwms.radar.data.dao;

import cwms.radar.data.dto.basinconnectivity.StreamReach;
import org.jooq.DSLContext;
import usace.cwms.db.jooq.dao.CwmsDbStreamJooq;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.Set;


public class StreamReachDao extends JooqDao<StreamReach>
{
    public StreamReachDao(DSLContext dsl)
    {
        super(dsl);
    }

    public Set<StreamReach> getReachesOnStream(String streamId, String officeId) throws SQLException
    {
        String pStationUnitIn = "km";
        String pOfficeIdMaskIn = null;
        if (officeId != null)
        {
            pOfficeIdMaskIn = officeId;
        }
        Connection c = dsl.configuration().connectionProvider().acquire();
        CwmsDbStreamJooq streamJooq = new CwmsDbStreamJooq();
        ResultSet rs = streamJooq.catStreamReaches(c, streamId, null, null, null, pStationUnitIn, pOfficeIdMaskIn);
        return buildReachesFromResultSet(rs);
    }

    private Set<StreamReach> buildReachesFromResultSet(ResultSet rs) throws SQLException
    {
        Set<StreamReach> retVal = new HashSet<>();

        while(rs.next())
        {
            String reachId = rs.getString(4);
            if (!reachId.isEmpty())
            {
                String streamId = rs.getString(3);
                String upstreamLocationId = rs.getString(5);
                String downstreamLocationId = rs.getString(8);
                StreamReach streamReach = new StreamReach(reachId, streamId, upstreamLocationId, downstreamLocationId);
                retVal.add(streamReach);
            }
        }

        return retVal;
    }

}
