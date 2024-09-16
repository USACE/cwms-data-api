package cwms.cda.data.dto.measurement;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import cwms.cda.data.dto.CwmsDTOBase;
import cwms.cda.formatters.Formats;
import cwms.cda.formatters.annotations.FormattableWith;
import cwms.cda.formatters.json.JsonV1;

@FormattableWith(contentType = Formats.JSONV1, formatter = JsonV1.class, aliases = {Formats.DEFAULT, Formats.JSON})
@JsonDeserialize(builder = StreamflowMeasurement.Builder.class)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonNaming(PropertyNamingStrategies.KebabCaseStrategy.class)
public final class StreamflowMeasurement extends CwmsDTOBase {
    private final Double gageHeight;
    private final Double flow;
    private final String quality;

    private StreamflowMeasurement(Builder builder) {
        this.gageHeight = builder.gageHeight;
        this.flow = builder.flow;
        this.quality = builder.quality;
    }

    public Double getGageHeight() {
        return gageHeight;
    }

    public Double getFlow() {
        return flow;
    }

    public String getQuality() {
        return quality;
    }

    public static final class Builder {
        private Double gageHeight;
        private Double flow;
        private String quality;

        public Builder withGageHeight(Double gageHeight) {
            this.gageHeight = gageHeight;
            return this;
        }

        public Builder withFlow(Double flow) {
            this.flow = flow;
            return this;
        }

        public Builder withQuality(String quality) {
            this.quality = quality;
            return this;
        }

        public StreamflowMeasurement build() {
            return new StreamflowMeasurement(this);
        }
    }
}
