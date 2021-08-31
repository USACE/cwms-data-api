package cwms.radar.api.graph.basinconnectivity;

import cwms.radar.api.graph.Edge;
import cwms.radar.api.graph.Node;
import cwms.radar.api.graph.basinconnectivity.edges.BasinConnectivityEdge;
import cwms.radar.api.graph.basinconnectivity.nodes.BasinConnectivityNode;
import cwms.radar.api.graph.Graph;
import cwms.radar.data.dto.basinconnectivity.Basin;
import cwms.radar.data.dto.basinconnectivity.Stream;

import java.util.ArrayList;
import java.util.List;

public class BasinConnectivityGraph implements Graph
{

    private final List<BasinConnectivityEdge> edges = new ArrayList<>();
    private final List<BasinConnectivityNode> nodes = new ArrayList<>();
    private final String id;

    private BasinConnectivityGraph(Builder builder)
    {
        edges.addAll(builder.edges);
        nodes.addAll(builder.nodes);
        id = builder.basin.getBasinName();
    }

    @Override
    public List<Edge> getEdges()
    {
        return new ArrayList<>(edges);
    }

    @Override
    public List<Node> getNodes()
    {
        return new ArrayList<>(nodes);
    }



    @Override
    public String getId()
    {
        return id;
    }

    public static class Builder
    {

        private final List<BasinConnectivityEdge> edges = new ArrayList<>();
        private final List<BasinConnectivityNode> nodes = new ArrayList<>();
        private final Basin basin;

        public Builder(Basin basin)
        {
            this.basin = basin;
        }

        public BasinConnectivityGraph build()
        {
            edges.clear();
            nodes.clear();
            BasinConnectivityGraph retVal = new BasinConnectivityGraph(this);
            Stream primaryStream = basin.getPrimaryStream();
            if(primaryStream != null)
            {
                BasinConnectivityStream primaryBasinConnStream = new BasinConnectivityStream(primaryStream, null);
                edges.addAll(primaryBasinConnStream.getEdges());
                nodes.addAll(primaryBasinConnStream.getNodes());
                retVal = new BasinConnectivityGraph(this);
            }
            return retVal;
        }
    }

}
