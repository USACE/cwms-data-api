package cwms.cda.data.dao;

import java.time.ZonedDateTime;

public class TimeSeriesRequestParameters {
    private final String names;
    private final String office;
    private final String units;
    private final ZonedDateTime beginTime;
    private final ZonedDateTime endTime;
    private final ZonedDateTime versionDate;
    private final boolean shouldTrim;
    private final boolean includeEntryDate;

    private TimeSeriesRequestParameters(Builder builder) {
        this.names = builder.names;
        this.office = builder.office;
        this.units = builder.units;
        this.beginTime = builder.beginTime;
        this.endTime = builder.endTime;
        this.versionDate = builder.versionDate;
        this.shouldTrim = builder.shouldTrim;
        this.includeEntryDate = builder.includeEntryDate;
    }

    public String getNames() {
        return names;
    }

    public String getOffice() {
        return office;
    }

    public String getUnits() {
        return units;
    }

    public ZonedDateTime getBeginTime() {
        return beginTime;
    }

    public ZonedDateTime getEndTime() {
        return endTime;
    }

    public ZonedDateTime getVersionDate() {
        return versionDate;
    }

    public boolean isShouldTrim() {
        return shouldTrim;
    }

    public boolean isIncludeEntryDate() {
        return includeEntryDate;
    }

    public static class Builder {
        private String names;
        private String office;
        private String units;
        private ZonedDateTime beginTime;
        private ZonedDateTime endTime;
        private ZonedDateTime versionDate;
        private boolean shouldTrim = true;
        private boolean includeEntryDate = false;

        public Builder() {
        }

        public Builder withNames(String names) {
            this.names = names;
            return this;
        }

        public Builder withOffice(String office) {
            this.office = office;
            return this;
        }

        public Builder withUnits(String units) {
            this.units = units;
            return this;
        }

        public Builder withBeginTime(ZonedDateTime beginTime) {
            this.beginTime = beginTime;
            return this;
        }

        public Builder withEndTime(ZonedDateTime endTime) {
            this.endTime = endTime;
            return this;
        }

        public Builder withVersionDate(ZonedDateTime versionDate) {
            this.versionDate = versionDate;
            return this;
        }

        public Builder withShouldTrim(boolean shouldTrim) {
            this.shouldTrim = shouldTrim;
            return this;
        }

        public Builder withIncludeEntryDate(boolean includeEntryDate) {
            this.includeEntryDate = includeEntryDate;
            return this;
        }

        public static Builder from(TimeSeriesRequestParameters params) {
            // This NEEDS to include every field in the TimeSeriesRequestParameters
            return new Builder()
                    .withNames(params.names)
                    .withOffice(params.office)
                    .withUnits(params.units)
                    .withBeginTime(params.beginTime)
                    .withEndTime(params.endTime)
                    .withVersionDate(params.versionDate)
                    .withShouldTrim(params.shouldTrim)
                    .withIncludeEntryDate(params.includeEntryDate);
        }

        public TimeSeriesRequestParameters build() {
            return new TimeSeriesRequestParameters(this);
        }
    }
}