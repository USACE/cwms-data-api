package cwms.radar.api.graph.basinconnectivity.edges;

import cwms.radar.api.graph.basinconnectivity.nodes.BasinConnectivityNode;
import cwms.radar.api.graph.pgjson.PgJsonEdge;

import javax.json.Json;
import javax.json.JsonObject;

public abstract class BasinConnectivityEdge implements PgJsonEdge
{
    private final String _streamId;
    private final BasinConnectivityNode _source;
    private final BasinConnectivityNode _target;

    protected BasinConnectivityEdge(String streamId, BasinConnectivityNode source, BasinConnectivityNode target)
    {
        _streamId = streamId;
        _source = source;
        _target = target;
    }

    @Override
    public String getId()
    {
        return getStreamId();
    }

    public String getStreamId()
    {
        return _streamId;
    }

    public BasinConnectivityNode getSource()
    {
        return _source;
    }

    public BasinConnectivityNode getTarget()
    {
        return _target;
    }

    @Override
    public JsonObject toPGJSON()
    {
        return Json.createObjectBuilder()
                .add("from", getSource().getId())
                .add("to", getTarget().getId())
                .add("undirected", false)
                .add("labels", Json.createArrayBuilder().add(getLabel()))
                .add("properties", getProperties())
                .build();
    }
}
