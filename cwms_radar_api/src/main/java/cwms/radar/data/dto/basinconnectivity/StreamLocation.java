package cwms.radar.data.dto.basinconnectivity;

import cwms.radar.api.errors.FieldException;
import cwms.radar.data.dto.CwmsDTO;

public class StreamLocation implements CwmsDTO
{
    private final String locationName;
    private final String streamName;
    private final Double station;
    private final String bank;
    private final String officeId;
    private final Double publishedStation;
    private final Double navigationStation;
    private final Double lowestMeasurableStage;
    private final Double totalDrainageArea;
    private final Double ungagedDrainageArea;

    private StreamLocation(Builder builder)
    {
        locationName = builder.locationName;
        streamName = builder.streamName;
        station = builder.station;
        bank = builder.bank;
        officeId = builder.officeId;
        publishedStation = builder.publishedStation;
        navigationStation = builder.navigationStation;
        lowestMeasurableStage = builder.lowestMeasurableStage;
        totalDrainageArea = builder.totalDrainageArea;
        ungagedDrainageArea = builder.ungagedDrainageArea;
    }

    public String getStreamName()
    {
        return streamName;
    }

    public Double getStation()
    {
        return station;
    }

    public String getBank()
    {
        return bank;
    }

    public String getLocationName()
    {
        return locationName;
    }

    public Double getPublishedStation()
    {
        return publishedStation;
    }

    public Double getNagivationStation()
    {
        return navigationStation;
    }

    public Double getLowestMeasurableStage()
    {
        return lowestMeasurableStage;
    }

    public Double getTotalDrainageArea()
    {
        return totalDrainageArea;
    }

    public Double getUngagedDrainageArea()
    {
        return ungagedDrainageArea;
    }

    public String getOfficeId()
    {
        return officeId;
    }

    public static class Builder
    {
        private final String officeId;
        private final String locationName;
        private final String streamName;
        private final Double station;
        private final String bank;
        private Double publishedStation;
        private Double navigationStation;
        private Double lowestMeasurableStage;
        private Double totalDrainageArea;
        private Double ungagedDrainageArea;

        public Builder(String locationName, String streamName, Double station, String bank, String officeId)
        {
            this.locationName = locationName;
            this.streamName = streamName;
            this.station = station;
            this.bank = bank;
            this.officeId = officeId;
        }

        public Builder(StreamLocation streamLocation)
        {
            this.locationName = streamLocation.getLocationName();
            this.streamName = streamLocation.getStreamName();
            this.station = streamLocation.getStation();
            this.bank = streamLocation.getBank();
            this.officeId = streamLocation.getOfficeId();
            this.publishedStation = streamLocation.getPublishedStation();
            this.navigationStation = streamLocation.getNagivationStation();
            this.lowestMeasurableStage = streamLocation.getLowestMeasurableStage();
            this.totalDrainageArea = streamLocation.getTotalDrainageArea();
            this.ungagedDrainageArea = streamLocation.getUngagedDrainageArea();
        }

        public Builder withPublishedStation(Double publishedStation)
        {
            this.publishedStation = publishedStation;
            return this;
        }

        public Builder withNavigationStation(Double navigationStation)
        {
            this.navigationStation = navigationStation;
            return this;
        }

        public Builder withLowestMeasurableStage(Double lowestMeasurableStage)
        {
            this.lowestMeasurableStage = lowestMeasurableStage;
            return this;
        }

        public Builder withTotalDrainageArea(Double totalDrainageArea)
        {
            this.totalDrainageArea = totalDrainageArea;
            return this;
        }

        public Builder withUngagedDrainageArea(Double ungagedDrainageArea)
        {
            this.ungagedDrainageArea = ungagedDrainageArea;
            return this;
        }

        public StreamLocation build()
        {
            return new StreamLocation(this);
        }
    }

    @Override
    public void validate() throws FieldException {
        // TODO Auto-generated method stub

    }

}
