package cwms.radar.data.dto;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import com.fasterxml.jackson.databind.annotation.JsonPOJOBuilder;
import java.time.ZoneId;
import java.util.Optional;

@JsonDeserialize(builder = LocationID.Builder.class)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonNaming(PropertyNamingStrategies.KebabCaseStrategy.class)
public class LocationID {


    private final String _baseLocation;
    private final String _subLocation;
    private final String _location;
    private final ZoneId _zoneId;

    public LocationID(Builder builder) {
        this._baseLocation = builder._baseLocation;
        this._subLocation = builder._subLocation;
        this._location = builder._location;
        this._zoneId = builder._zoneId;
    }

    public String getBaseLocation() {
        return _baseLocation;
    }

    public String getSubLocation() {
        return _subLocation;
    }

    public String getLocation() {
        return _location;
    }

    public ZoneId getZoneId() {
        return _zoneId;
    }

    @Override
    public String toString() {
        return getLocation();
    }

    @JsonPOJOBuilder
    @JsonNaming(PropertyNamingStrategies.KebabCaseStrategy.class)
    public static class Builder {
        private String _baseLocation;
        private String _subLocation;
        private String _location;
        private ZoneId _zoneId;

        public Builder() {

        }

        public Builder withBaseLocation(String baseLocation) {

            this._baseLocation = baseLocation.trim();
            if (this._subLocation != null && !this._subLocation.isEmpty()) {
                this._location = this._baseLocation + "-" + this._subLocation;
            } else {
                this._subLocation = "";
                this._location = this._baseLocation;
            }

            return this;
        }

        public Builder withSubLocation(String subLocation) {

            if (subLocation == null) {
                subLocation = "";
            }

            this._subLocation = subLocation.trim();
            if (!this._subLocation.isEmpty()) {
                this._location = this._baseLocation + "-" + this._subLocation;
            } else {
                this._location = this._baseLocation;
            }

            return this;
        }

        public Builder withLocation(String location) {
            this._location = location;
            int index = location.indexOf("-");
            if (index != -1) {
                this._baseLocation = location.substring(0, index).trim();
                this._subLocation = location.substring(index + 1).trim();
            } else {
                this._baseLocation = location;
                this._subLocation = "";
            }

            return this;
        }

        public Builder withZoneId(ZoneId zoneId) {
            this._zoneId = zoneId;
            return this;
        }

        public Builder withLocationID(mil.army.usace.hec.metadata.LocationID locationId) {
            Optional<ZoneId> zoneIdOpt = locationId.getZoneId();
            ZoneId zoneId = null;
            if (zoneIdOpt.isPresent()) {
                zoneId = zoneIdOpt.get();
            }
            withZoneId(zoneId);

            withBaseLocation(locationId.getBaseLocation());
            withSubLocation(locationId.getSubLocation());
            withLocation(locationId.getLocation());

            return this;
        }


        public LocationID build() {
            return new LocationID(this);
        }

    }

}
