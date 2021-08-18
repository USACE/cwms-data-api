package cwms.radar.data.dto.basinconnectivity.graph;

import javax.json.Json;
import javax.json.JsonObject;

public class ReachEdge extends BasinConnectivityEdge
{
    private static final String LABEL = "Reach";
    private final String _reachId;

    public ReachEdge(String reachId, String streamId, BasinConnectivityNode source, BasinConnectivityNode target)
    {
        super(streamId, source, target);
        _reachId = reachId;
    }

    private String getReachId()
    {
        return _reachId;
    }

    @Override
    public String getLabel()
    {
        return LABEL;
    }

    @Override
    public JsonObject getProperties()
    {
        return Json.createObjectBuilder()
                .add("reach_id", Json.createArrayBuilder().add(getReachId()))
                .add("stream_id", Json.createArrayBuilder().add(getStreamId()))
                .build();
    }
}
