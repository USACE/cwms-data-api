package cwms.cda.data.dto.timeseriesprofile;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import com.fasterxml.jackson.databind.annotation.JsonPOJOBuilder;
import cwms.cda.formatters.Formats;
import cwms.cda.formatters.annotations.FormattableWith;
import cwms.cda.formatters.json.JsonV2;

import java.util.List;

@FormattableWith(contentType = Formats.JSONV2, formatter = JsonV2.class)
@JsonDeserialize(builder = ProfileTimeSeries.Builder.class)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonNaming(PropertyNamingStrategies.KebabCaseStrategy.class)
public final class ProfileTimeSeries {
    private final String parameter;
    private final String unit;
    private final String timeZone;
    private final List<TimeValuePair> values;

    protected ProfileTimeSeries(Builder builder)
    {
        parameter = builder.parameter;
        unit = builder.unit;
        values = builder.values;
        timeZone = builder.timeZone;
    }
    public String getTimeZone()
    {
        return timeZone;
    }
    public String getParameter()
    {
        return parameter;
    }
    public String getUnit()
    {
        return unit;
    }
    public List<TimeValuePair> getValues()
    {
        return values;
    }
    @JsonPOJOBuilder
    @JsonNaming(PropertyNamingStrategies.KebabCaseStrategy.class)
    public static final class Builder {
        private List<TimeValuePair> values;
        private String unit;
        private String parameter;
        private String timeZone;
        public Builder withValues(List<TimeValuePair> values) {
            this.values = values;
            return this;
        }

        public Builder withParameter(String parameter) {
            this.parameter = parameter;
            return this;
        }

        public Builder withUnit(String unit) {
            this.unit = unit;
            return this;
        }

        public Builder withTimeZone(String timeZone){
            this.timeZone = timeZone;
            return this;
        }
        public ProfileTimeSeries build() {
            return new ProfileTimeSeries(this);
        }
    }

}
