package cwms.radar.data.dto.basinconnectivity;

import cwms.radar.data.dto.CwmsDTO;

public class StreamLocation implements CwmsDTO
{
    private final String _locationId;
    private final String _streamId;
    private final Double _station;
    private final String _bank;

    public StreamLocation(String locationId, String streamId, Double station, String bank)
    {
        _locationId = locationId;
        _streamId = streamId;
        _station = station;
        _bank = bank;
    }

    public String getStreamId()
    {
        return _streamId;
    }

    public Double getStation()
    {
        return _station;
    }

    public String getBank()
    {
        return _bank;
    }
    public String getLocationId()
    {
        return _locationId;
    }
}

