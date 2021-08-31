package cwms.radar.data.dao;

import cwms.radar.api.enums.Unit;
import cwms.radar.data.dto.basinconnectivity.StreamReach;
import org.jooq.DSLContext;
import usace.cwms.db.jooq.dao.CwmsDbStreamJooq;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;


public class StreamReachDao extends JooqDao<StreamReach>
{
    public StreamReachDao(DSLContext dsl)
    {
        super(dsl);
    }

    public Set<StreamReach> getReachesOnStream(String streamId, String officeId) throws SQLException
    {
        String pStationUnitIn = Unit.KILOMETER.getValue();
        CwmsDbStreamJooq streamJooq = new CwmsDbStreamJooq();
        AtomicReference<ResultSet> resultSetRef = new AtomicReference<>();
        dsl.connection(c -> resultSetRef.set(streamJooq.catStreamReaches(c, streamId, null, null, null, pStationUnitIn, officeId)));
        return buildReachesFromResultSet(resultSetRef.get());
    }

    private Set<StreamReach> buildReachesFromResultSet(ResultSet rs) throws SQLException
    {
        Set<StreamReach> retVal = new HashSet<>();

        while(rs.next())
        {
            String reachId = rs.getString("REACH_LOCATION");
            if (!reachId.isEmpty())
            {
                String streamId = rs.getString("STREAM_LOCATION");
                String officeId = rs.getString("OFFICE_ID");
                String upstreamLocationId = rs.getString("UPSTREAM_LOCATION");
                String downstreamLocationId = rs.getString("DOWNSTREAM_LOCATION");
                String configuration = rs.getString("CONFIGURATION");
                String comment = rs.getString("COMMENTS");
                StreamReach streamReach = new StreamReach.Builder(reachId, streamId, upstreamLocationId, downstreamLocationId, officeId)
                        .withComment(comment)
                        .withConfiguration(configuration)
                        .build();
                retVal.add(streamReach);
            }
        }

        return retVal;
    }

}
