package cwms.cda.data.dto.catalog;

import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlElementWrapper;
import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlProperty;

import java.util.Collection;
import java.util.Objects;


@JsonNaming(PropertyNamingStrategies.KebabCaseStrategy.class)
public class LocationCatalogEntry extends CatalogEntry {
    private String name;
    private String nearestCity;
    private String publicName;
    private String longName;
    private String description;
    private String kind;
    private String type;
    private String timeZone;
    private Double latitude;
    private Double longitude;
    private Double publishedLatitude;
    private Double publishedLongitude;
    private String horizontalDatum;
    private Double elevation;
    private String unit;
    private String verticalDatum;
    private String nation;
    private String state;
    private String county;
    private String boundingOffice;
    private String mapLabel;
    private boolean active;
    @JacksonXmlElementWrapper(localName = "aliases")
    @JacksonXmlProperty(localName = "alias")
    private Collection<LocationAlias> aliases;

    private LocationCatalogEntry() {
        super(null);
    }

    private LocationCatalogEntry(String office,
                                String name,
                                String nearestCity,
                                String publicName,
                                String longName,
                                String description,
                                String kind,
                                String type,
                                String timeZone,
                                Double latitude,
                                Double longitude,
                                Double publishedLatitude,
                                Double publishedLongitude,
                                String horizontalDatum,
                                Double elevation,
                                String unit,
                                String verticalDatum,
                                String nation,
                                String state,
                                String county,
                                String boundingOffice,
                                String mapLabel,
                                boolean active,
                                Collection<LocationAlias> aliases) {
        super(office);
        Objects.requireNonNull(aliases, "aliases provided must be an actual list, empty list is "
                + "okay");
        this.name = name;
        this.nearestCity = nearestCity;
        this.publicName = publicName;
        this.longName = longName;
        this.description = description;
        this.kind = kind;
        this.type = type;
        this.timeZone = timeZone;
        this.latitude = latitude;
        this.longitude = longitude;
        this.publishedLatitude = publishedLatitude;
        this.publishedLongitude = publishedLongitude;
        this.horizontalDatum = horizontalDatum;
        this.elevation = elevation;
        this.unit = unit;
        this.verticalDatum = verticalDatum;
        this.nation = nation;
        this.state = state;
        this.county = county;
        this.boundingOffice = boundingOffice;
        this.mapLabel = mapLabel;
        this.active = active;
        this.aliases = aliases;
    }

    public String getPublicName() {
        return this.publicName;
    }

    public String getLongName() {
        return this.longName;
    }

    public String getDescription() {
        return this.description;
    }

    public String getKind() {
        return this.kind;
    }

    public String getType() {
        return this.type;
    }

    public String getTimeZone() {
        return this.timeZone;
    }

    public Double getLatitude() {
        return this.latitude;
    }

    public Double getLongitude() {
        return this.longitude;
    }

    public Double getPublishedLatitude() {
        return this.publishedLatitude;
    }

    public Double getPublishedLongitude() {
        return this.publishedLongitude;
    }

    public String getHorizontalDatum() {
        return this.horizontalDatum;
    }

    public Double getElevation() {
        return this.elevation;
    }

    public String getUnit() {
        return this.unit;
    }

    public String getVerticalDatum() {
        return this.verticalDatum;
    }

    public String getNation() {
        return this.nation;
    }

    public String getState() {
        return this.state;
    }

    public String getCounty() {
        return this.county;
    }

    public String getBoundingOffice() {
        return this.boundingOffice;
    }

    public String getMapLabel() {
        return this.mapLabel;
    }

    public boolean getActive() {
        return this.active;
    }

    public boolean isActive() {
        return this.active;
    }

    public Collection<LocationAlias> getAliases() {
        return this.aliases;
    }


    public String getName() {
        return name;
    }

    public String getNearestCity() {
        return nearestCity;
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append(getOffice()).append("/").append(name)
                .append(";nearestCity=").append(nearestCity);
        for (LocationAlias alias : aliases) {
            builder.append(";alias=").append(alias.toString());
        }
        return builder.toString();
    }

    @Override
    public String getCursor() {
        return (getOffice() + "/" + name).toUpperCase();
    }


    public static class Builder {
        private String office;
        private String name;
        private String nearestCity;
        private String publicName;
        private String longName;
        private String description;
        private String kind;
        private String type;
        private String timeZone;
        private Double latitude;
        private Double longitude;
        private Double publishedLatitude;
        private Double publishedLongitude;
        private String horizontalDatum;
        private Double elevation;
        private String unit;
        private String verticalDatum;
        private String nation;
        private String state;
        private String county;
        private String boundingOffice;
        private String mapLabel;
        private boolean active;
        private Collection<LocationAlias> aliases;

        public Builder officeId(final String office) {
            this.office = office;
            return this;
        }

        public Builder name(String name) {
            this.name = name;
            return this;
        }

        public Builder nearestCity(String nearestCity) {
            this.nearestCity = nearestCity;
            return this;
        }

        public Builder publicName(String publicName) {
            this.publicName = publicName;
            return this;
        }

        public Builder longName(String longName) {
            this.longName = longName;
            return this;
        }

        public Builder description(String description) {
            this.description = description;
            return this;
        }

        public Builder kind(String kind) {
            this.kind = kind;
            return this;
        }

        public Builder type(String type) {
            this.type = type;
            return this;
        }

        public Builder timeZone(String timeZone) {
            this.timeZone = timeZone;
            return this;
        }

        public Builder latitude(Double latitude) {
            this.latitude = latitude;
            return this;
        }

        public Builder longitude(Double longitude) {
            this.longitude = longitude;
            return this;
        }

        public Builder publishedLatitude(Double publishedLatitude) {
            this.publishedLatitude = publishedLatitude;
            return this;
        }

        public Builder publishedLongitude(Double publishedLongitude) {
            this.publishedLongitude = publishedLongitude;
            return this;
        }

        public Builder horizontalDatum(String horizontalDatum) {
            this.horizontalDatum = horizontalDatum;
            return this;
        }

        public Builder elevation(Double elevation) {
            this.elevation = elevation;
            return this;
        }

        public Builder unit(String unit) {
            this.unit = unit;
            return this;
        }

        public Builder verticalDatum(String verticalDatum) {
            this.verticalDatum = verticalDatum;
            return this;
        }

        public Builder nation(String nation) {
            this.nation = nation;
            return this;
        }

        public Builder state(String state) {
            this.state = state;
            return this;
        }

        public Builder county(String county) {
            this.county = county;
            return this;
        }

        public Builder boundingOffice(String boundingOffice) {
            this.boundingOffice = boundingOffice;
            return this;
        }

        public Builder mapLabel(String mapLabel) {
            this.mapLabel = mapLabel;
            return this;
        }

        public Builder active(boolean active) {
            this.active = active;
            return this;
        }

        public Builder aliases(Collection<LocationAlias> aliases) {
            this.aliases = aliases;
            return this;
        }

        public LocationCatalogEntry build() {
            return new LocationCatalogEntry(office, name, nearestCity, publicName,
                    longName, description, kind, type, timeZone, latitude, longitude,
                    publishedLatitude, publishedLongitude, horizontalDatum, elevation, unit,
                    verticalDatum, nation, state, county, boundingOffice, mapLabel, active,
                    aliases);
        }


    }


}
