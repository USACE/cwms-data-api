package cwms.cda.data.dto;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import cwms.cda.formatters.Formats;
import cwms.cda.formatters.annotations.FormattableWith;
import cwms.cda.formatters.json.JsonV1;
import java.time.Instant;

@FormattableWith(contentType = Formats.JSONV1, formatter = JsonV1.class, aliases = {Formats.DEFAULT, Formats.JSON})
@JsonDeserialize(builder = TimeSeriesMetaData.Builder.class)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonNaming(PropertyNamingStrategies.KebabCaseStrategy.class)
public final class TimeSeriesMetaData {
    @JsonProperty(required = true)
    private final TimeSeriesIdentifierDescriptor tsId;
    private final Instant dateRefreshed;
    private final String notes;

    private TimeSeriesMetaData(Builder builder) {
        this.tsId = builder.tsId;
        this.dateRefreshed = builder.dateRefreshed;
        this.notes = builder.notes;
    }

    public TimeSeriesIdentifierDescriptor getTsId() {
        return tsId;
    }

    public Instant getDateRefreshed() {
        return dateRefreshed;
    }

    public String getNotes() {
        return notes;
    }

    public static class Builder {
        private TimeSeriesIdentifierDescriptor tsId;
        private Instant dateRefreshed;
        private String notes;

        public Builder withTsId(TimeSeriesIdentifierDescriptor tsId) {
            this.tsId = tsId;
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

        public TimeSeriesMetaData build() {
            return new TimeSeriesMetaData(this);
        }
    }

}
