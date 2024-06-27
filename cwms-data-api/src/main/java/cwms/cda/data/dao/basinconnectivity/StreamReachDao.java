package cwms.cda.data.dao.basinconnectivity;

import cwms.cda.api.enums.Unit;
import cwms.cda.data.dao.JooqDao;
import cwms.cda.data.dto.basinconnectivity.StreamReach;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.Set;
import org.jooq.DSLContext;
import usace.cwms.db.jooq.dao.CwmsDbStreamJooq;


public class StreamReachDao extends JooqDao<StreamReach> {
    public StreamReachDao(DSLContext dsl) {
        super(dsl);
    }

    public Set<StreamReach> getReachesOnStream(String streamId, String officeId) {
        String pStationUnitIn = Unit.KILOMETER.getValue();
        CwmsDbStreamJooq streamJooq = new CwmsDbStreamJooq();

        return connectionResult(dsl, c -> {
            try (ResultSet resultSet = streamJooq.catStreamReaches(c, streamId, null, null,
                    null, pStationUnitIn, officeId)) {
                return buildReachesFromResultSet(resultSet);
            }
        });
    }

    private Set<StreamReach> buildReachesFromResultSet(ResultSet rs) throws SQLException {
        Set<StreamReach> retVal = new HashSet<>();

        while (rs.next()) {
            String reachId = rs.getString("REACH_LOCATION");
            if (!reachId.isEmpty()) {
                String streamId = rs.getString("STREAM_LOCATION");
                String officeId = rs.getString("OFFICE_ID");
                String upstreamLocationId = rs.getString("UPSTREAM_LOCATION");
                String downstreamLocationId = rs.getString("DOWNSTREAM_LOCATION");
                String configuration = rs.getString("CONFIGURATION");
                String comment = rs.getString("COMMENTS");
                StreamReach streamReach = new StreamReach.Builder(reachId, streamId,
                        upstreamLocationId, downstreamLocationId, officeId)
                        .withComment(comment)
                        .withConfiguration(configuration)
                        .build();
                retVal.add(streamReach);
            }
        }

        return retVal;
    }
}
