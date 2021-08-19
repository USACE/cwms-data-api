package cwms.radar.api.graph.basinconnectivity.nodes;

import cwms.radar.data.dto.basinconnectivity.StreamLocation;

public class BasinConnectivityStreamLocation extends BasinConnectivityNode
{

    private static final String LABEL = "Stream Location";
    private final StreamLocation _streamLocation;

    public BasinConnectivityStreamLocation(StreamLocation streamLocation)
    {
        super(streamLocation.getStreamId(), streamLocation.getStation(), streamLocation.getBank());
        _streamLocation = streamLocation;
    }

    @Override
    public String getId()
    {
        return _streamLocation.getLocationId();
    }

    @Override
    public String getLabel()
    {
        return LABEL;
    }
}
