/*
 *
 * MIT License
 *
 * Copyright (c) 2024 Hydrologic Engineering Center
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 *  to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER
 * DEALINGS IN THE
 * SOFTWARE.
 */

package cwms.cda.data.dto.watersupply;

public class LocationType {
    private final String nearestCity;
    private final String nationId;
    private final String boundingOfficeName;
    private final String boundingOfficeId;
    private final Double publishedLongitude;
    private final Double publishedLatitude;
    private final String mapLabel;
    private final String locationKindId;
    private final boolean activeFlag;
    private final String description;
    private final String longName;
    private final String publicName;
    private final String verticalDatum;
    private final String elevUnitId;
    private final Double elevation;
    private final String horizontalDatum;
    private final Double longitude;
    private final Double latitude;
    private final String typeOfLocation;
    private final String timeZoneName;
    private final String countyName;
    private final String stateInitial;
    private final LocationRefType locationRefType;

    private LocationType() {
        this.nearestCity = null;
        this.nationId = null;
        this.boundingOfficeName = null;
        this.boundingOfficeId = null;
        this.publishedLongitude = null;
        this.publishedLatitude = null;
        this.mapLabel = null;
        this.locationKindId = null;
        this.activeFlag = false;
        this.description = null;
        this.longName = null;
        this.publicName = null;
        this.verticalDatum = null;
        this.elevUnitId = null;
        this.elevation = null;
        this.horizontalDatum = null;
        this.longitude = null;
        this.latitude = null;
        this.typeOfLocation = null;
        this.timeZoneName = null;
        this.countyName = null;
        this.stateInitial = null;
        this.locationRefType = null;
    }


    public LocationType(Builder builder) {
        this.locationRefType = builder.locationRefType;
        this.stateInitial = builder.stateInitial;
        this.countyName = builder.countyName;
        this.timeZoneName = builder.timeZoneName;
        this.typeOfLocation = builder.typeOfLocation;
        this.latitude = builder.latitude;
        this.longitude = builder.longitude;
        this.horizontalDatum = builder.horizontalDatum;
        this.elevation = builder.elevation;
        this.elevUnitId = builder.elevUnitId;
        this.verticalDatum = builder.verticalDatum;
        this.publicName = builder.publicName;
        this.longName = builder.longName;
        this.description = builder.description;
        this.activeFlag = builder.activeFlag;
        this.locationKindId = builder.locationKindId;
        this.mapLabel = builder.mapLabel;
        this.publishedLatitude = builder.publishedLatitude;
        this.publishedLongitude = builder.publishedLongitude;
        this.boundingOfficeId = builder.boundingOfficeId;
        this.boundingOfficeName = builder.boundingOfficeName;
        this.nationId = builder.nationId;
        this.nearestCity = builder.nearestCity;
    }

    public String getNearestCity() {
        return this.nearestCity;
    }

    public String getNationId() {
        return this.nationId;
    }

    public String getBoundingOfficeName() {
        return this.boundingOfficeName;
    }

    public String getBoundingOfficeId() {
        return this.boundingOfficeId;
    }

    public Double getPublishedLongitude() {
        return this.publishedLongitude;
    }

    public Double getPublishedLatitude() {
        return this.publishedLatitude;
    }

    public String getMapLabel() {
        return this.mapLabel;
    }

    public String getLocationKindId() {
        return this.locationKindId;
    }

    public boolean getActiveFlag() {
        return this.activeFlag;
    }

    public String getDescription() {
        return this.description;
    }

    public String getLongName() {
        return this.longName;
    }

    public String getPublicName() {
        return this.publicName;
    }

    public String getVerticalDatum() {
        return this.verticalDatum;
    }

    public String getElevUnitId() {
        return this.elevUnitId;
    }

    public Double getElevation() {
        return this.elevation;
    }

    public String getHorizontalDatum() {
        return this.horizontalDatum;
    }

    public Double getLongitude() {
        return this.longitude;
    }

    public Double getLatitude() {
        return this.latitude;
    }

    public String getTypeOfLocation() {
        return this.typeOfLocation;
    }

    public String getTimeZoneName() {
        return this.timeZoneName;
    }

    public String getCountyName() {
        return this.countyName;
    }

    public String getStateInitial() {
        return this.stateInitial;
    }

    public LocationRefType getLocationRefType() {
        return this.locationRefType;
    }

    public static class Builder {
        private String nearestCity;
        private String nationId;
        private String boundingOfficeName;
        private String boundingOfficeId;
        private Double publishedLongitude;
        private Double publishedLatitude;
        private String mapLabel;
        private String locationKindId;
        private boolean activeFlag;
        private String description;
        private String longName;
        private String publicName;
        private String verticalDatum;
        private String elevUnitId;
        private Double elevation;
        private String horizontalDatum;
        private Double longitude;
        private Double latitude;
        private String typeOfLocation;
        private String timeZoneName;
        private String countyName;
        private String stateInitial;
        private LocationRefType locationRefType;

        public Builder withNearestCity(String nearestCity) {
            this.nearestCity = nearestCity;
            return this;
        }

        public Builder withNationId(String nationId) {
            this.nationId = nationId;
            return this;
        }

        public Builder withBoundingOfficeName(String boundingOfficeName) {
            this.boundingOfficeName = boundingOfficeName;
            return this;
        }

        public Builder withBoundingOfficeId(String boundingOfficeId) {
            this.boundingOfficeId = boundingOfficeId;
            return this;
        }

        public Builder withPublishedLongitude(Double publishedLongitude) {
            this.publishedLongitude = publishedLongitude;
            return this;
        }

        public Builder withPublishedLatitude(Double publishedLatitude) {
            this.publishedLatitude = publishedLatitude;
            return this;
        }

        public Builder withMapLabel(String mapLabel) {
            this.mapLabel = mapLabel;
            return this;
        }

        public Builder withLocationKindId(String locationKindId) {
            this.locationKindId = locationKindId;
            return this;
        }

        public Builder withActiveFlag(boolean activeFlag) {
            this.activeFlag = activeFlag;
            return this;
        }

        public Builder withDescription(String description) {
            this.description = description;
            return this;
        }

        public Builder withLongName(String longName) {
            this.longName = longName;
            return this;
        }

        public Builder withPublicName(String publicName) {
            this.publicName = publicName;
            return this;
        }

        public Builder withVerticalDatum(String verticalDatum) {
            this.verticalDatum = verticalDatum;
            return this;
        }

        public Builder withElevUnitId(String elevUnitId) {
            this.elevUnitId = elevUnitId;
            return this;
        }

        public Builder withElevation(Double elevation) {
            this.elevation = elevation;
            return this;
        }

        public Builder withHorizontalDatum(String horizontalDatum) {
            this.horizontalDatum = horizontalDatum;
            return this;
        }

        public Builder withLongitude(Double longitude) {
            this.longitude = longitude;
            return this;
        }

        public Builder withLatitude(Double latitude) {
            this.latitude = latitude;
            return this;
        }

        public Builder withTypeOfLocation(String typeOfLocation) {
            this.typeOfLocation = typeOfLocation;
            return this;
        }

        public Builder withTimeZoneName(String timeZoneName) {
            this.timeZoneName = timeZoneName;
            return this;
        }

        public Builder withCountyName(String countyName) {
            this.countyName = countyName;
            return this;
        }

        public Builder withStateInitial(String stateInitial) {
            this.stateInitial = stateInitial;
            return this;
        }

        public Builder withLocationRefType(LocationRefType locationRefType) {
            this.locationRefType = locationRefType;
            return this;
        }

        public LocationType build() {
            return new LocationType(this);
        }
    }
}
