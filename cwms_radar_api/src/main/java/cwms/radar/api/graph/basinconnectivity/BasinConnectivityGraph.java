package cwms.radar.api.graph.basinconnectivity;

import cwms.radar.api.graph.basinconnectivity.edges.BasinConnectivityEdge;
import cwms.radar.api.graph.basinconnectivity.nodes.BasinConnectivityNode;

import java.util.ArrayList;
import java.util.List;

public class BasinConnectivityGraph
{

    private final List<BasinConnectivityEdge> _edges = new ArrayList<>();
    private final List<BasinConnectivityNode> _nodes = new ArrayList<>();

    BasinConnectivityGraph(BasinConnectivityGraphBuilder builder)
    {
        _edges.clear();
        _nodes.clear();
        _edges.addAll(builder.getEdges());
        _nodes.addAll(builder.getNodes());
    }

    public List<BasinConnectivityEdge> getEdges()
    {
        return new ArrayList<>(_edges);
    }

    public List<BasinConnectivityNode> getNodes()
    {
        return new ArrayList<>(_nodes);
    }

    public boolean isEmpty()
    {
        return getEdges().isEmpty() && getNodes().isEmpty();
    }

}
