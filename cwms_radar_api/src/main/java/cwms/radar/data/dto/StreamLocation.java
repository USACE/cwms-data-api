package cwms.radar.data.dto;

import java.sql.ResultSet;
import java.sql.SQLException;

public class StreamLocation implements CwmsDTO
{
    private final String _bank;
    private final String _streamId;
    private final Double _station;
    private final String _locationId;

    public StreamLocation(String locationId, String streamId, Double station, String bank)
    {
        _locationId = locationId;
        _bank = bank;
        _streamId = streamId;
        _station = station;
    }

    public String getLocationId()
    {
        return _locationId;
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
}
