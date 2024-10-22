package cwms.cda.data.dto.timeseriesprofile;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import com.fasterxml.jackson.databind.annotation.JsonPOJOBuilder;
import cwms.cda.data.dto.CwmsDTOBase;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
@JsonSubTypes({@JsonSubTypes.Type(value = ParameterInfoIndexed.class, name = "indexed-parameter-info"),
            @JsonSubTypes.Type(value = ParameterInfoColumnar.class, name = "columnar-parameter-info")
})

public abstract class ParameterInfo extends CwmsDTOBase {
    private final String parameter;
    private final String unit;

    ParameterInfo(ParameterInfo.Builder builder) {
        parameter = builder.parameter;
        unit = builder.unit;
    }
    public abstract String getParameterInfoString();

    public String getParameter() {
        return parameter;
    }

    public String getUnit() {
        return unit;
    }

    @JsonPOJOBuilder
    @JsonNaming(PropertyNamingStrategies.KebabCaseStrategy.class)
    public abstract static class Builder {
        private String parameter;
        private String unit;

        public Builder withParameter(String parameter) {
            this.parameter = parameter;
            return this;
        }

        public Builder withUnit(String unit) {
            this.unit = unit;
            return this;
        }
        public abstract ParameterInfo build();
    }
}