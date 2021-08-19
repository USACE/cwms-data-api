package cwms.radar.api.graph.basinconnectivity;

import cwms.radar.api.graph.basinconnectivity.edges.BasinConnectivityEdge;
import cwms.radar.api.graph.basinconnectivity.nodes.BasinConnectivityNode;

import java.util.List;

public class BasinConnectivityGraph
{

    private final List<BasinConnectivityEdge> _edges;
    private final List<BasinConnectivityNode> _nodes;

    public BasinConnectivityGraph(List<BasinConnectivityEdge> edges, List<BasinConnectivityNode> nodes)
    {
        _edges = edges;
        _nodes = nodes;
    }

    public List<BasinConnectivityEdge> getEdges()
    {
        return _edges;
    }

    public List<BasinConnectivityNode> getNodes()
    {
        return _nodes;
    }

}
