package cwms.radar.data.dto.basinconnectivity;

import cwms.radar.data.dto.basinconnectivity.buildercontracts.Build;

public class StreamLocationBuilder implements Build<StreamLocation>
{
    private final String _officeId;
    private String _locationId;
    private String _streamId;
    private Double _station;
    private String _bank;
    private Double _publishedStation;
    private Double _navigationStation;
    private Double _lowestMeasurableStage;
    private Double _totalDrainageArea;
    private Double _ungagedDrainageArea;

    public StreamLocationBuilder(String locationId, String streamId, Double station, String bank, String officeId)
    {
        _locationId = locationId;
        _streamId = streamId;
        _station = station;
        _bank = bank;
        _officeId = officeId;
    }

    public StreamLocationBuilder(StreamLocation streamLocation)
    {
        _locationId = streamLocation.getLocationId();
        _streamId = streamLocation.getStreamId();
        _station = streamLocation.getStation();
        _bank = streamLocation.getBank();
        _officeId = streamLocation.getOfficeId();
        _publishedStation = streamLocation.getPublishedStation();
        _navigationStation = streamLocation.getNagivationStation();
        _lowestMeasurableStage = streamLocation.getLowestMeasurableStage();
        _totalDrainageArea = streamLocation.getTotalDrainageArea();
        _ungagedDrainageArea = streamLocation.getUngagedDrainageArea();
    }

    public StreamLocationBuilder withPublishedStation(Double publishedStation)
    {
        _publishedStation = publishedStation;
        return this;
    }

    public StreamLocationBuilder withNavigationStation(Double navigationStation)
    {
        _navigationStation = navigationStation;
        return this;
    }

    public StreamLocationBuilder withLowestMeasurableStage(Double lowestMeasurableStage)
    {
        _lowestMeasurableStage = lowestMeasurableStage;
        return this;
    }

    public StreamLocationBuilder withTotalDrainageArea(Double totalDrainageArea)
    {
        _totalDrainageArea = totalDrainageArea;
        return this;
    }

    public StreamLocationBuilder withUngagedDrainageArea(Double ungagedDrainageArea)
    {
        _ungagedDrainageArea = ungagedDrainageArea;
        return this;
    }

    String getLocationId()
    {
        return _locationId;
    }

    String getStreamId()
    {
        return _streamId;
    }

    Double getStation()
    {
        return _station;
    }

    String getBank()
    {
        return _bank;
    }

    Double getPublishedStation()
    {
        return _publishedStation;
    }

    Double getNagivationStation()
    {
        return _navigationStation;
    }

    Double getLowestMeasurableStage()
    {
        return _lowestMeasurableStage;
    }

    Double getTotalDrainageArea()
    {
        return _totalDrainageArea;
    }

    Double getUngagedDrainageArea()
    {
        return _ungagedDrainageArea;
    }

    String getOfficeId()
    {
        return _officeId;
    }

    @Override
    public StreamLocation build()
    {
        return new StreamLocation(this);
    }
}
