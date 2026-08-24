package cwms.cda.data.dto.measurement;

import com.fasterxml.jackson.annotation.JsonAlias;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import cwms.cda.formatters.Formats;
import cwms.cda.formatters.annotations.FormattableWith;
import cwms.cda.formatters.json.JsonV1;

@FormattableWith(contentType = Formats.JSONV1, formatter = JsonV1.class, aliases = {Formats.DEFAULT, Formats.JSON})
@JsonDeserialize(builder = MeasurementLegacy.Builder.class)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonNaming(PropertyNamingStrategies.KebabCaseStrategy.class)
public class MeasurementLegacy extends Measurement {

    public MeasurementLegacy(Builder builder) {
        super(builder);
    }

    @JsonProperty("number")
    @Override
    public String getMeasurementId() {
        return super.getMeasurementId();
    }

    public static class Builder extends Measurement.Builder {
        @Override
        public MeasurementLegacy build() {
            return new MeasurementLegacy(this);
        }

        @JsonProperty("number")
        @JsonAlias("measurement-id")
        @Override
        public MeasurementLegacy.Builder withMeasurementId(String measurementId) {
            return (MeasurementLegacy.Builder) super.withMeasurementId(measurementId);
        }
    }
}
