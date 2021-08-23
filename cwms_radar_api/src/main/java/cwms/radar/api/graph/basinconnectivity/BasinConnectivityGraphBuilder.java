package cwms.radar.api.graph.basinconnectivity;

import cwms.radar.api.graph.basinconnectivity.edges.BasinConnectivityEdge;
import cwms.radar.api.graph.basinconnectivity.nodes.BasinConnectivityNode;
import cwms.radar.data.dto.basinconnectivity.Basin;
import cwms.radar.data.dto.basinconnectivity.Stream;

import java.util.ArrayList;
import java.util.List;

public class BasinConnectivityGraphBuilder
{

    private final List<BasinConnectivityEdge> _edges = new ArrayList<>();
    private final List<BasinConnectivityNode> _nodes = new ArrayList<>();
    private final Basin _basin;

    public BasinConnectivityGraphBuilder(Basin basin)
    {
        _basin = basin;
    }

    List<BasinConnectivityEdge> getEdges()
    {
        return new ArrayList<>(_edges);
    }

    List<BasinConnectivityNode> getNodes()
    {
        return new ArrayList<>(_nodes);
    }

    public BasinConnectivityGraph build()
    {
        _edges.clear();
        _nodes.clear();
        BasinConnectivityGraph retVal = new BasinConnectivityGraph(this);
        Stream primaryStream = _basin.getPrimaryStream();
        if(primaryStream != null)
        {
            BasinConnectivityStream primaryBasinConnStream = new BasinConnectivityStream(primaryStream, null);
            _edges.addAll(primaryBasinConnStream.getEdges());
            _nodes.addAll(primaryBasinConnStream.getNodes());
            retVal = new BasinConnectivityGraph(this);
        }
        return retVal;
    }

    public String getBasinId()
    {
        return _basin.getBasinId();
    }
}
