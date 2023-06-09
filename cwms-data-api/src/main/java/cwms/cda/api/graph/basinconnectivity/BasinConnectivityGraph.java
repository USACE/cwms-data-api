package cwms.cda.api.graph.basinconnectivity;

import cwms.cda.api.graph.Edge;
import cwms.cda.api.graph.Graph;
import cwms.cda.api.graph.Node;
import cwms.cda.api.graph.basinconnectivity.edges.BasinConnectivityEdge;
import cwms.cda.api.graph.basinconnectivity.nodes.BasinConnectivityNode;
import cwms.cda.data.dto.basinconnectivity.Basin;
import cwms.cda.data.dto.basinconnectivity.Stream;
import java.util.ArrayList;
import java.util.List;

public class BasinConnectivityGraph implements Graph {

    private final List<BasinConnectivityEdge> edges = new ArrayList<>();
    private final List<BasinConnectivityNode> nodes = new ArrayList<>();
    private final String name;

    private BasinConnectivityGraph(Builder builder) {
        edges.addAll(builder.edges);
        nodes.addAll(builder.nodes);
        name = builder.basin.getBasinName();
    }

    @Override
    public List<Edge> getEdges() {
        return new ArrayList<>(edges);
    }

    @Override
    public List<Node> getNodes() {
        return new ArrayList<>(nodes);
    }


    @Override
    public String getName() {
        return name;
    }

    public static class Builder {

        private final List<BasinConnectivityEdge> edges = new ArrayList<>();
        private final List<BasinConnectivityNode> nodes = new ArrayList<>();
        private final Basin basin;

        public Builder(Basin basin) {
            this.basin = basin;
        }

        public BasinConnectivityGraph build() {
            edges.clear();
            nodes.clear();
            BasinConnectivityGraph retVal = new BasinConnectivityGraph(this);
            Stream primaryStream = basin.getPrimaryStream();
            if (primaryStream != null) {
                BasinConnectivityStream primaryBasinConnStream =
                        new BasinConnectivityStream(primaryStream, null);
                edges.addAll(primaryBasinConnStream.getEdges());
                nodes.addAll(primaryBasinConnStream.getNodes());
                retVal = new BasinConnectivityGraph(this);
            }
            return retVal;
        }
    }

}
