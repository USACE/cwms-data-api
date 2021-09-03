package cwms.radar.api.graph.basinconnectivity.edges;

import cwms.radar.api.graph.basinconnectivity.nodes.BasinConnectivityNode;

public class ReachEdge extends BasinConnectivityEdge
{
    private static final String LABEL = "Reach";
    private final String reachId;

    public ReachEdge(String reachId, String streamId, BasinConnectivityNode source, BasinConnectivityNode target)
    {
        super(streamId, source, target);
        this.reachId = reachId;
    }

    @Override
    public String getId()
    {
        return reachId;
    }

    @Override
    public String getLabel()
    {
        return LABEL;
    }

}
