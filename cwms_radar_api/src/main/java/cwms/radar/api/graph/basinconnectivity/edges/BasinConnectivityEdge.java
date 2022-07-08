package cwms.radar.api.graph.basinconnectivity.edges;

import cwms.radar.api.graph.Edge;
import cwms.radar.api.graph.basinconnectivity.nodes.BasinConnectivityNode;

public abstract class BasinConnectivityEdge implements Edge {
    private final String streamId;
    private final BasinConnectivityNode source;
    private final BasinConnectivityNode target;

    protected BasinConnectivityEdge(String streamId, BasinConnectivityNode source,
                                    BasinConnectivityNode target) {
        this.streamId = streamId;
        this.source = source;
        this.target = target;
    }

    @Override
    public String getId() {
        return getStreamId();
    }

    public String getStreamId() {
        return streamId;
    }

    public BasinConnectivityNode getSource() {
        return source;
    }

    public BasinConnectivityNode getTarget() {
        return target;
    }

    public abstract String getLabel();

}
