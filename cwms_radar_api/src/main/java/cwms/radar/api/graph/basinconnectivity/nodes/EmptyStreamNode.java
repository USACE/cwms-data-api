package cwms.radar.api.graph.basinconnectivity.nodes;

import cwms.radar.api.graph.basinconnectivity.nodes.BasinConnectivityNode;

public class EmptyStreamNode extends BasinConnectivityNode//probably needs a better name
{
    private static final String LABEL = "EMPTY";

    public EmptyStreamNode(String streamId, Double station, String bank)
    {
        super(streamId, station, bank);
    }

    @Override
    public String getLabel()
    {
        return LABEL;
    }

    @Override
    public String getId()
    {
        return getStreamId() + "-Node-" + getStation();
    }
}
