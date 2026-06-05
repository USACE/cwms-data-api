package cwms.cda.data.dto;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import cwms.cda.formatters.Formats;
import cwms.cda.formatters.annotations.FormattableWith;
import cwms.cda.formatters.json.JsonV1;
import java.time.Instant;

@FormattableWith(contentType = Formats.JSONV1, formatter = JsonV1.class, aliases = {Formats.DEFAULT, Formats.JSON})
@JsonDeserialize(builder = PublishedTimeSeriesData.Builder.class)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonNaming(PropertyNamingStrategies.KebabCaseStrategy.class)
public final class PublishedTimeSeriesData {
    private final CwmsId timeSeriesId;
    private final String timezoneName;
    private final Integer intervalOffsetMinutes;
    private final Boolean active;
    private final Instant dateRefreshed;
    private final String notes;

    private PublishedTimeSeriesData(Builder builder) {
        this.timeSeriesId = builder.timeSeriesId;
        this.timezoneName = builder.timezoneName;
        this.intervalOffsetMinutes = builder.intervalOffsetMinutes;
        this.active = builder.active;
        this.dateRefreshed = builder.dateRefreshed;
        this.notes = builder.notes;
    }

    public CwmsId getTimeSeriesId() {
        return timeSeriesId;
    }

    public String getTimezoneName() {
        return timezoneName;
    }

    public Integer getIntervalOffsetMinutes() {
        return intervalOffsetMinutes;
    }

    public Boolean getActive() {
        return active;
    }

    public Instant getDateRefreshed() {
        return dateRefreshed;
    }

    public String getNotes() {
        return notes;
    }

    public static class Builder {
        private CwmsId timeSeriesId;
        private String timezoneName;
        private Integer intervalOffsetMinutes;
        private Boolean active;
        private Instant dateRefreshed;
        private String notes;

        public Builder withTimeSeriesId(CwmsId timeSeriesId) {
            this.timeSeriesId = timeSeriesId;
            return this;
        }

        public Builder withTimezoneName(String timezoneName) {
            this.timezoneName = timezoneName;
            return this;
        }

        public Builder withIntervalOffsetMinutes(Integer intervalOffsetMinutes) {
            this.intervalOffsetMinutes = intervalOffsetMinutes;
            return this;
        }

        public Builder withActive(Boolean active) {
            this.active = active;
            return this;
        }

        public Builder withDateRefreshed(Instant dateRefreshed) {
            this.dateRefreshed = dateRefreshed;
            return this;
        }

        public Builder withNotes(String notes) {
            this.notes = notes;
            return this;
        }

        public PublishedTimeSeriesData build() {
            return new PublishedTimeSeriesData(this);
        }
    }

}
