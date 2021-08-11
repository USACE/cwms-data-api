package cwms.radar.data.dao;

import cwms.radar.data.dto.StreamReach;
import org.jooq.DSLContext;
import org.jooq.Record;
import org.jooq.Result;

import java.util.HashSet;
import java.util.Set;

import static usace.cwms.db.jooq.codegen.packages.CWMS_STREAM_PACKAGE.call_CAT_STREAM_REACHES;

public class StreamReachDao extends JooqDao<StreamReach>
{
    public StreamReachDao(DSLContext dsl)
    {
        super(dsl);
    }

    public Set<StreamReach> getReachesOnStream(String streamId, String officeId)
    {
        Set<StreamReach> retval = new HashSet<>();
        String pStationUnitIn = "km";
        String pOfficeIdMaskIn = null;
        if (officeId != null) {
            pOfficeIdMaskIn = officeId;
        }
        Result<Record> result = call_CAT_STREAM_REACHES(dsl.configuration(), streamId, null, null, null, pStationUnitIn, pOfficeIdMaskIn);
        for(Record rec : result)
        {
            String upstreamLocationId = rec.get("UPSTREAM_LOCATION_ID").toString();
            String downstreamLocationId = rec.get("DOWNSTREAM_LOCATION_ID").toString();
            String reachId = rec.get("STREAM_REACH_ID").toString();
            StreamReach reach = new StreamReach(reachId, streamId, upstreamLocationId, downstreamLocationId);
            retval.add(reach);
        }
        return retval;
    }

}
