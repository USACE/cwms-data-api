package cwms.radar.data.dto;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import com.fasterxml.jackson.databind.annotation.JsonPOJOBuilder;
import cwms.radar.api.enums.Nation;

import java.time.ZoneId;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.function.Consumer;

@JsonDeserialize(builder = Location.Builder.class)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonNaming(PropertyNamingStrategies.KebabCaseStrategy.class)
public final class  Location implements CwmsDTO
{
    private final String name;
    private final Double latitude;
    private final Double longitude;
    private final Boolean active;
    private final String publicName;
    private final String longName;
    private final String description;
    private final String timezoneName;
    private final String locationType;
    private final String locationKind;
    private final Nation nation;
    private final String stateInitial;
    private final String countyName;
    private final String nearestCity;
    private final String horizontalDatum;
    private final Double publishedLongitude;
    private final Double publishedLatitude;
    private final String verticalDatum;
    private final Double elevation;
    private final String mapLabel;
    private final String boundingOfficeId;
    private final String officeId;

    private Location(Builder builder)
    {
        this.name = builder.name;
        this.latitude = builder.latitude;
        this.longitude = builder.longitude;
        this.active = builder.active;
        this.publicName = builder.publicName;
        this.longName = builder.longName;
        this.description = builder.description;
        this.timezoneName = builder.timezoneName;
        this.locationType = builder.locationType;
        this.locationKind = builder.locationKind;
        this.nation = builder.nation;
        this.stateInitial = builder.stateInitial;
        this.countyName = builder.countyName;
        this.nearestCity = builder.nearestCity;
        this.horizontalDatum = builder.horizontalDatum;
        this.publishedLatitude = builder.publishedLatitude;
        this.publishedLongitude = builder.publishedLongitude;
        this.verticalDatum = builder.verticalDatum;
        this.elevation = builder.elevation;
        this.mapLabel = builder.mapLabel;
        this.boundingOfficeId = builder.boundingOfficeId;
        this.officeId = builder.officeId;
    }

    public String getName()
    {
        return name;
    }

    public Double getLatitude()
    {
        return latitude;
    }

    public Double getLongitude()
    {
        return longitude;
    }

    public Boolean active()
    {
        return active;
    }

    public String getPublicName()
    {
        return publicName;
    }

    public String getLongName()
    {
        return longName;
    }

    public String getDescription()
    {
        return description;
    }

    public String getTimezoneName()
    {
        return timezoneName;
    }

    public String getLocationType()
    {
        return locationType;
    }

    public String getLocationKind()
    {
        return locationKind;
    }

    public Nation getNation()
    {
        return nation;
    }

    public String getStateInitial()
    {
        return stateInitial;
    }

    public String getCountyName()
    {
        return countyName;
    }

    public String getNearestCity()
    {
        return nearestCity;
    }

    public String getHorizontalDatum()
    {
        return horizontalDatum;
    }

    public Double getPublishedLongitude()
    {
        return publishedLongitude;
    }

    public Double getPublishedLatitude()
    {
        return publishedLatitude;
    }

    public String getVerticalDatum()
    {
        return verticalDatum;
    }

    public Double getElevation()
    {
        return elevation;
    }

    public String getMapLabel()
    {
        return mapLabel;
    }

    public String getBoundingOfficeId()
    {
        return boundingOfficeId;
    }

    public String getOfficeId()
    {
        return officeId;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o)
        {
            return true;
        }
        if (o == null || getClass() != o.getClass())
        {
            return false;
        }
        Location location = (Location) o;
        return Double.compare(location.getLatitude(), getLatitude()) == 0 && Double.compare(location.getLongitude(), getLongitude()) == 0 && active() == location.active() && getName().equals(location.getName()) && getPublicName().equals(location.getPublicName()) && Objects.equals(getLongName(), location.getLongName()) && Objects.equals(getDescription(), location.getDescription()) && getTimezoneName().equals(location.getTimezoneName()) && Objects.equals(getLocationType(), location.getLocationType()) && getLocationKind().equals(location.getLocationKind()) && Objects.equals(getNation(), location.getNation()) && Objects.equals(getStateInitial(), location.getStateInitial()) && Objects.equals(getCountyName(), location.getCountyName()) && getHorizontalDatum().equals(location.getHorizontalDatum()) && Objects.equals(getPublishedLongitude(), location.getPublishedLongitude()) && Objects.equals(getPublishedLatitude(), location.getPublishedLatitude()) && Objects.equals(getVerticalDatum(), location.getVerticalDatum()) && Objects.equals(getElevation(), location.getElevation()) && Objects.equals(getMapLabel(), location.getMapLabel()) && Objects.equals(getBoundingOfficeId(), location.getBoundingOfficeId()) && getOfficeId().equals(location.getOfficeId());
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(getName(), getLatitude(), getLongitude(), active(), getPublicName(), getLongName(), getDescription(), getTimezoneName(), getLocationType(), getLocationKind(), getNation(), getStateInitial(), getCountyName(), getHorizontalDatum(), getPublishedLongitude(), getPublishedLatitude(), getVerticalDatum(), getElevation(), getMapLabel(), getBoundingOfficeId(), getOfficeId());
    }

    @Override
    public String toString() {
        return "Location{" +
                "name='" + name + '\'' +
                ", latitude=" + latitude +
                ", longitude=" + longitude +
                ", active=" + active +
                ", publicName='" + publicName + '\'' +
                ", longName='" + longName + '\'' +
                ", description='" + description + '\'' +
                ", timezoneName='" + timezoneName + '\'' +
                ", locationType='" + locationType + '\'' +
                ", locationKind='" + locationKind + '\'' +
                ", nation=" + nation +
                ", stateInitial='" + stateInitial + '\'' +
                ", countyName='" + countyName + '\'' +
                ", nearestCity='" + nearestCity + '\'' +
                ", horizontalDatum='" + horizontalDatum + '\'' +
                ", publishedLongitude=" + publishedLongitude +
                ", publishedLatitude=" + publishedLatitude +
                ", verticalDatum='" + verticalDatum + '\'' +
                ", elevation=" + elevation +
                ", mapLabel='" + mapLabel + '\'' +
                ", boundingOfficeId='" + boundingOfficeId + '\'' +
                ", officeId='" + officeId + '\'' +
                '}';
    }

    @JsonPOJOBuilder
    public static class Builder
    {
        private String name;
        private double latitude;
        private double longitude;
        private String officeId;
        private boolean active = true;
        private String publicName;
        private String longName;
        private String description;
        private String timezoneName;
        private String locationType;
        private String locationKind;
        private Nation nation;
        private String stateInitial;
        private String countyName;
        private String nearestCity;
        private String horizontalDatum;
        private Double publishedLongitude;
        private Double publishedLatitude;
        private String verticalDatum;
        private Double elevation;
        private String mapLabel;
        private String boundingOfficeId;
        private static final String MISSING_NAME_ERROR_MSG = "Location name is a required field";
        private final Map<String, Consumer<Object>> propertyFunctionMap = new HashMap<>();

        @JsonCreator
        public Builder(@JsonProperty(value = "name") String name, @JsonProperty(value = "locationKind") String locationKind,
                       @JsonProperty (value = "timezoneName") ZoneId timezoneName, @JsonProperty(value = "latitude") double latitude,
                       @JsonProperty(value = "longitude") double longitude, @JsonProperty(value = "horizontalDatum") String horizontalDatum,
                       @JsonProperty(value = "officeId") String officeId)
        {
            this.name = name;
            this.publicName = name;
            this.locationKind = locationKind;
            this.timezoneName = timezoneName == null ? null : timezoneName.getId();
            this.latitude = latitude;
            this.longitude = longitude;
            this.horizontalDatum = horizontalDatum;
            this.officeId = officeId;
            buildPropertyFunctions();
        }

        public Builder(Location location)
        {
            this.name = location.getName();
            this.latitude = location.getLatitude();
            this.longitude = location.getLongitude();
            this.horizontalDatum = location.getHorizontalDatum();
            this.locationKind = location.getLocationKind();
            this.officeId = location.getOfficeId();
            this.timezoneName = location.getTimezoneName();
            this.active = location.active();
            this.publicName = location.getPublicName();
            this.longName = location.getLongName();
            this.description = location.getDescription();
            this.locationType = location.getLocationType();
            this.nation = location.getNation();
            this.stateInitial = location.getStateInitial();
            this.countyName = location.getCountyName();
            this.nearestCity = location.getNearestCity();
            this.publishedLatitude = location.getPublishedLatitude();
            this.publishedLongitude = location.getPublishedLongitude();
            this.verticalDatum = location.getVerticalDatum();
            this.elevation = location.getElevation();
            this.mapLabel = location.getMapLabel();
            this.boundingOfficeId = location.getBoundingOfficeId();
            buildPropertyFunctions();
        }

        @JsonIgnore
        private void buildPropertyFunctions()
        {
            propertyFunctionMap.clear();
            propertyFunctionMap.put("name", nameVal -> withName((String)nameVal));
            propertyFunctionMap.put("latitude", latitudeVal -> withLatitude((Double)latitudeVal));
            propertyFunctionMap.put("longitude", longitudeVal -> withLongitude((Double)longitudeVal));
            propertyFunctionMap.put("officeId", officeIdVal -> withOfficeId((String)officeIdVal));
            propertyFunctionMap.put("active", activeVal -> withActive((Boolean)activeVal));
            propertyFunctionMap.put("publicName", publicNameVal -> withPublicName((String)publicNameVal));
            propertyFunctionMap.put("longName", longNameVal -> withLongName((String)longNameVal));
            propertyFunctionMap.put("description", descriptionVal -> withDescription((String)descriptionVal));
            propertyFunctionMap.put("timezoneName", timezoneNameVal -> withTimeZoneName(ZoneId.of(Objects.requireNonNull((String)timezoneNameVal, "Timezone is a required field"))));
            propertyFunctionMap.put("locationType", locationTypeVal -> withLocationType((String)locationTypeVal));
            propertyFunctionMap.put("locationKind", locationKindVal -> withLocationKind((String)locationKindVal));
            propertyFunctionMap.put("nation", nationVal -> withNation(convertStringToNation((String)nationVal)));
            propertyFunctionMap.put("stateInitial", stateInitialVal -> withStateInitial((String)stateInitialVal));
            propertyFunctionMap.put("countyName", countyNameVal -> withCountyName((String)countyNameVal));
            propertyFunctionMap.put("nearestCity", nearestCityVal -> withNearestCity((String)nearestCityVal));
            propertyFunctionMap.put("horizontalDatum", horizontalDatumVal -> withHorizontalDatum((String)horizontalDatumVal));
            propertyFunctionMap.put("publishedLongitude", publishedLongitudeVal -> withPublishedLongitude((Double)publishedLongitudeVal));
            propertyFunctionMap.put("publishedLatitude", publishedLatitudeVal -> withPublishedLatitude((Double)publishedLatitudeVal));
            propertyFunctionMap.put("verticalDatum", verticalDatumVal -> withVerticalDatum((String)verticalDatumVal));
            propertyFunctionMap.put("elevation", elevationVal -> withElevation((Double)elevationVal));
            propertyFunctionMap.put("mapLabel", mapLabelVal -> withMapLabel((String)mapLabelVal));
            propertyFunctionMap.put("boundingOfficeId", boundingOfficeIdVal -> withBoundingOfficeId((String)boundingOfficeIdVal));
        }

        @JsonIgnore
        public Builder withProperty(String propertyName, Object value)
        {
            Consumer<Object> function = propertyFunctionMap.get(propertyName);
            if(function == null)
            {
                throw new IllegalArgumentException("Property Name does not exist for Location");
            }
            function.accept(value);
            return this;
        }

        public Builder withName(String name)
        {
            this.name = Objects.requireNonNull(name, MISSING_NAME_ERROR_MSG);
            return this;
        }

        public Builder withLocationKind(String kind)
        {
            this.locationKind = Objects.requireNonNull(kind, "Location kind is a required field");
            return this;
        }

        public Builder withTimeZoneName(ZoneId zoneId)
        {
            this.timezoneName = zoneId.getId();
            return this;
        }

        public Builder withLatitude(Double latitude)
        {
            this.latitude = Objects.requireNonNull(latitude, "Latitude is a required field");
            return this;
        }

        public Builder withLongitude(Double longitude)
        {
            this.longitude = Objects.requireNonNull(longitude, "Longitude is a required field");
            return this;
        }

        public Builder withHorizontalDatum(String horizontalDatum)
        {
            this.horizontalDatum = Objects.requireNonNull(horizontalDatum, "Horizontal datum is a required field");
            return this;
        }

        public Builder withOfficeId(String officeId)
        {
            this.officeId = Objects.requireNonNull(officeId, "Office Id is a required field");
            return this;
        }

        public Builder withLongName(String longName)
        {
            this.longName = longName;
            return this;
        }

        public Builder withActive(boolean active)
        {
            this.active = active;
            return this;
        }

        public Builder withPublicName(String publicName)
        {
            this.publicName = publicName;
            return this;
        }

        public Builder withDescription(String description)
        {
            this.description = description;
            return this;
        }

        public Builder withLocationType(String locationType)
        {
            this.locationType = locationType;
            return this;
        }

        public Builder withNation(Nation nation)
        {
            this.nation = nation;
            return this;
        }

        public Builder withStateInitial(String stateInitial)
        {
            if(stateInitial != null)
            {
                stateInitial = stateInitial.toUpperCase();
            }
            this.stateInitial = stateInitial;
            return this;
        }

        public Builder withCountyName(String countyName)
        {
            this.countyName = countyName;
            return this;
        }

        public Builder withNearestCity(String nearestCity)
        {
            this.nearestCity = nearestCity;
            return this;
        }

        public Builder withPublishedLatitude(Double publishedLatitude)
        {
            this.publishedLatitude = publishedLatitude;
            return this;
        }

        public Builder withPublishedLongitude(Double publishedLongitude)
        {
            this.publishedLongitude = publishedLongitude;
            return this;
        }

        public Builder withVerticalDatum(String verticalDatum)
        {
            this.verticalDatum = verticalDatum;
            return this;
        }

        public Builder withElevation(Double elevation)
        {
            this.elevation = elevation;
            return this;
        }

        public Builder withMapLabel(String mapLabel)
        {
            this.mapLabel = mapLabel;
            return this;
        }

        public Builder withBoundingOfficeId(String boundingOfficeId)
        {
            this.boundingOfficeId = boundingOfficeId;
            return this;
        }

        public Location build()
        {
            return new Location(this);
        }

        @JsonIgnore
        private Nation convertStringToNation(String nation)
        {
            Nation nationConverted = Nation.NationForCode(nation);
            if(nationConverted == null)
            {
                nationConverted = Nation.NationForName(nation);
            }
            return nationConverted;
        }

    }
 }