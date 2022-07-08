package cwms.radar.api.graph.basinconnectivity.edges;

import cwms.radar.api.graph.basinconnectivity.nodes.BasinConnectivityNode;

public class StreamEdge extends BasinConnectivityEdge {
    private static final String LABEL = "Stream";

    public StreamEdge(String streamId, BasinConnectivityNode source, BasinConnectivityNode target) {
        super(streamId, source, target);
    }

    @Override
    public String getLabel() {
        return LABEL;
    }

}
