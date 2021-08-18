package cwms.radar.data.dto.basinconnectivity;

import cwms.radar.data.dto.CwmsDTO;
import cwms.radar.data.dto.basinconnectivity.graph.BasinConnectivityNode;

public class StreamLocation extends BasinConnectivityNode implements CwmsDTO
{
    private final String _locationId;
    private static final String LABEL = "Stream Location";

    public StreamLocation(String locationId, String streamId, Double station, String bank)
    {
        super(streamId, station, bank);
        _locationId = locationId;
    }

    public String getLocationId()
    {
        return _locationId;
    }

    @Override
    public String getName()
    {
        return getLocationId();
    }

    @Override
    public String getLabel()
    {
        return LABEL;
    }

}

