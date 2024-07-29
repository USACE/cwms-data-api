package cwms.cda.data.dto;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import com.fasterxml.jackson.databind.annotation.JsonPOJOBuilder;
import cwms.cda.api.errors.FieldException;
import cwms.cda.formatters.Formats;
import cwms.cda.formatters.annotations.FormattableWith;
import cwms.cda.formatters.json.JsonV2;

import java.time.ZoneId;

// DTO version of usace.cwms.db.dao.ifc.ts.TimeSeriesIdentifierDescriptor
@JsonDeserialize(builder = cwms.cda.data.dto.TimeSeriesIdentifierDescriptor.Builder.class)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonNaming(PropertyNamingStrategies.KebabCaseStrategy.class)
@FormattableWith(contentType = Formats.JSONV2, formatter = JsonV2.class)
public class TimeSeriesIdentifierDescriptor extends CwmsDTO {
    private final String timeSeriesId;
    private final String timezoneName;
    private final Long intervalOffsetMinutes;
    private final boolean active;

    private TimeSeriesIdentifierDescriptor(Builder builder) {
        super(builder.officeId);
        this.timeSeriesId = builder.timeSeriesId;
        this.timezoneName = builder.timezoneName;
        this.intervalOffsetMinutes = builder.intervalOffsetMinutes;
        this.active = builder.active;
    }

    public String getTimeSeriesId() {
        return timeSeriesId;
    }

    public String getTimezoneName() {
        return timezoneName;
    }

    public Long getIntervalOffsetMinutes() {
        return intervalOffsetMinutes;
    }

    public boolean isActive() {
        return active;
    }

    @JsonPOJOBuilder
    @JsonNaming(PropertyNamingStrategies.KebabCaseStrategy.class)
    public static class Builder {
        private String officeId;
        private String timeSeriesId;
        private String timezoneName;
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
            String tzName = null;

            if (zoneId != null) {
                tzName = zoneId.getId();
            }
            return withTimezoneName(tzName);
        }

        public Builder withTimezoneName(String timezoneName) {
            this.timezoneName = timezoneName;
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
            this.timezoneName = tsid.getTimezoneName();
            this.intervalOffsetMinutes = tsid.getIntervalOffsetMinutes();
            this.active = tsid.isActive();
            return this;
        }

        public TimeSeriesIdentifierDescriptor build() {
            return new TimeSeriesIdentifierDescriptor(this);
        }
    }

}
