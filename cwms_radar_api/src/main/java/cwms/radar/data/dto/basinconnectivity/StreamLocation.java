package cwms.radar.data.dto.basinconnectivity;

import cwms.radar.data.dto.CwmsDTO;

public class StreamLocation implements CwmsDTO
{
    private final String _locationId;
    private final String _streamId;
    private final Double _station;
    private final String _bank;
    private final String _officeId;
    private final Double _publishedStation;
    private final Double _navigationStation;
    private final Double _lowestMeasurableStage;
    private final Double _totalDrainageArea;
    private final Double _ungagedDrainageArea;

    StreamLocation(StreamLocationBuilder builder)
    {
        _locationId = builder.getLocationId();
        _streamId = builder.getStreamId();
        _station = builder.getStation();
        _bank = builder.getBank();
        _officeId = builder.getOfficeId();
        _publishedStation = builder.getPublishedStation();
        _navigationStation = builder.getNagivationStation();
        _lowestMeasurableStage = builder.getLowestMeasurableStage();
        _totalDrainageArea = builder.getTotalDrainageArea();
        _ungagedDrainageArea = builder.getUngagedDrainageArea();
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

    public Double getPublishedStation()
    {
        return _publishedStation;
    }

    public Double getNagivationStation()
    {
        return _navigationStation;
    }

    public Double getLowestMeasurableStage()
    {
        return _lowestMeasurableStage;
    }

    public Double getTotalDrainageArea()
    {
        return _totalDrainageArea;
    }

    public Double getUngagedDrainageArea()
    {
        return _ungagedDrainageArea;
    }

    public String getOfficeId()
    {
        return _officeId;
    }
}

