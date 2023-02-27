package cwms.radar.data.dto;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import com.fasterxml.jackson.databind.annotation.JsonPOJOBuilder;
import cwms.radar.api.errors.FieldException;
import java.time.ZoneId;

// DTO version of usace.cwms.db.dao.ifc.ts.TimeSeriesIdentifierDescriptor
@JsonDeserialize(builder = cwms.radar.data.dto.TimeSeriesIdentifierDescriptor.Builder.class)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonNaming(PropertyNamingStrategies.KebabCaseStrategy.class)
public class TimeSeriesIdentifierDescriptor implements CwmsDTO {

    private final String officeId;
    private final String timeSeriesId;
    private final ZoneId zoneId;
    private final Long intervalOffsetMinutes;
    private final boolean active;

    private TimeSeriesIdentifierDescriptor(Builder builder) {
        this.officeId = builder.officeId;
        this.timeSeriesId = builder.timeSeriesId;
        this.zoneId = builder.zoneId;
        this.intervalOffsetMinutes = builder.intervalOffsetMinutes;
        this.active = builder.active;
    }

    public String getOfficeId() {
        return officeId;
    }

    public String getTimeSeriesId() {
        return timeSeriesId;
    }

    public ZoneId getZoneId() {
        return zoneId;
    }

    public Long getIntervalOffsetMinutes() {
        return intervalOffsetMinutes;
    }

    public boolean isActive() {
        return active;
    }

    @Override
    public void validate() throws FieldException {
        // Nothing to validate
    }

    @JsonPOJOBuilder
    @JsonNaming(PropertyNamingStrategies.KebabCaseStrategy.class)
    public static class Builder {
        private String officeId;
        private String timeSeriesId;
        private ZoneId zoneId;
        private Long intervalOffsetMinutes;
        private boolean active;

        public Builder withOfficeId(String officeId) {
            this.officeId = officeId;
            return this;
        }

        public Builder withTimeSeriesId(String timeSeriesId) {
            this.timeSeriesId = timeSeriesId;
            return this;
        }

        public Builder withZoneId(ZoneId zoneId) {
            this.zoneId = zoneId;
            return this;
        }

        public Builder withIntervalOffsetMinutes(Long intervalOffsetMinutes) {
            this.intervalOffsetMinutes = intervalOffsetMinutes;
            return this;
        }

        public Builder withActive(boolean active) {
            this.active = active;
            return this;
        }

        public Builder withTimeSeriesIdentifierDescriptor(TimeSeriesIdentifierDescriptor tsid) {
            this.officeId = tsid.getOfficeId();
            this.timeSeriesId = tsid.getTimeSeriesId();
            this.zoneId = tsid.getZoneId();
            this.intervalOffsetMinutes = tsid.getIntervalOffsetMinutes();
            this.active = tsid.isActive();
            return this;
        }

        public TimeSeriesIdentifierDescriptor build() {
            return new TimeSeriesIdentifierDescriptor(this);
        }
    }


}
