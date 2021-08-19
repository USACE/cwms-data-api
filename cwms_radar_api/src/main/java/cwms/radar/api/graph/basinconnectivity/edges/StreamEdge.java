package cwms.radar.api.graph.basinconnectivity.edges;

import cwms.radar.api.graph.basinconnectivity.edges.BasinConnectivityEdge;
import cwms.radar.api.graph.basinconnectivity.nodes.BasinConnectivityNode;

import javax.json.Json;
import javax.json.JsonObject;

public class StreamEdge extends BasinConnectivityEdge
{
    private static final String LABEL = "Stream";
    public StreamEdge(String streamId, BasinConnectivityNode source, BasinConnectivityNode target)
    {
        super(streamId, source, target);
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
                .add("stream_id", Json.createArrayBuilder().add(getStreamId()))
                .build();
    }
}
