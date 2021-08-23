package cwms.radar.api.graph.basinconnectivity;

import cwms.radar.api.graph.basinconnectivity.edges.BasinConnectivityEdge;
import cwms.radar.api.graph.basinconnectivity.nodes.BasinConnectivityNode;
import cwms.radar.api.graph.pgjson.PgJsonEdge;
import cwms.radar.api.graph.pgjson.PgJsonGraph;
import cwms.radar.api.graph.pgjson.PgJsonNode;

import java.util.ArrayList;
import java.util.List;

public class BasinConnectivityGraph implements PgJsonGraph
{

    private final List<BasinConnectivityEdge> _edges = new ArrayList<>();
    private final List<BasinConnectivityNode> _nodes = new ArrayList<>();
    private final String _id;

    BasinConnectivityGraph(BasinConnectivityGraphBuilder builder)
    {
        _edges.addAll(builder.getEdges());
        _nodes.addAll(builder.getNodes());
        _id = builder.getBasinId();
    }

    @Override
    public List<PgJsonEdge> getEdges()
    {
        return new ArrayList<>(_edges);
    }

    @Override
    public List<PgJsonNode> getNodes()
    {
        return new ArrayList<>(_nodes);
    }

    @Override
    public String getId()
    {
        return _id;
    }

}
