package cwms.cda.data.dto.timeseriesprofile;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import com.fasterxml.jackson.databind.annotation.JsonPOJOBuilder;
import cwms.cda.data.dto.CwmsDTOBase;
import cwms.cda.formatters.Formats;
import cwms.cda.formatters.annotations.FormattableWith;
import cwms.cda.formatters.json.JsonV1;
import java.time.Instant;

@FormattableWith(contentType = Formats.JSONV1, formatter = JsonV1.class)
@JsonDeserialize(builder = TimeValuePair.Builder.class)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonNaming(PropertyNamingStrategies.KebabCaseStrategy.class)
public final class TimeValuePair extends CwmsDTOBase {
    private final Instant dateTime;
    private final double value;
    private final int quality;

    private TimeValuePair(Builder builder) {
        dateTime = builder.dateTime;
        value = builder.value;
        quality = builder.quality;
    }

    public Instant getDateTime() {
        return dateTime;
    }

    public double getValue() {
        return value;
    }

    public int getQuality() {
        return quality;
    }

    @JsonPOJOBuilder
    @JsonNaming(PropertyNamingStrategies.KebabCaseStrategy.class)
    public static final class Builder {
        private Instant dateTime;
        private double value;
        private int quality;

        public Builder withDateTime(Instant dateTime) {
            this.dateTime = dateTime;
            return this;
        }

        public Builder withValue(double value) {
            this.value = value;
            return this;
        }

        public Builder withQuality(int quality) {
            this.quality = quality;
            return this;
        }

        public TimeValuePair build() {
            return new TimeValuePair(this);
        }
    }
}
