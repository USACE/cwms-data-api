package cwms.radar.data.dto;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import com.fasterxml.jackson.databind.annotation.JsonPOJOBuilder;

@JsonDeserialize(builder = Parameter.Builder.class)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonNaming(PropertyNamingStrategies.KebabCaseStrategy.class)
public class Parameter {


    private final int parameterId;
    private final String parameter;
    private final String baseParameter;
    private final String subParameter;
    private final String unitsString;

    public Parameter(Builder builder) {
        this.parameterId = builder.parameterId;
        this.parameter = builder.parameter;
        this.baseParameter = builder.baseParameter;
        this.subParameter = builder.subParameter;
        this.unitsString = builder.unitsString;

    }

    public int getParameterId() {
        return parameterId;
    }

    public String getParameter() {
        return parameter;
    }

    public String getBaseParameter() {
        return baseParameter;
    }

    public String getSubParameter() {
        return subParameter;
    }

    public String getUnitsString() {
        return unitsString;
    }

    @Override
    public String toString() {
        return getParameter();
    }

    @JsonPOJOBuilder
    @JsonNaming(PropertyNamingStrategies.KebabCaseStrategy.class)
    public static class Builder {

        private int parameterId;
        private String parameter;
        private String baseParameter;
        private String subParameter;
        private String unitsString;

        public Builder() {

        }

        public Builder withParameter(mil.army.usace.hec.metadata.Parameter parameter) {

            withParameterId(parameter.getParameterId());
            withParameter(parameter.getParameter());
            withBaseParameter(parameter.getBaseParameter());
            withSubParameter(parameter.getSubParameter());
            withUnitsString(parameter.getUnitsString());

            return this;
        }

        public Builder withUnitsString(String unitsString1) {
            this.unitsString = unitsString1;
            return this;
        }

        public Builder withSubParameter(String subParameter1) {
            this.subParameter = subParameter1;
            return this;
        }

        public Builder withBaseParameter(String baseParameter1) {
            this.baseParameter = baseParameter1;
            return this;
        }

        public Builder withParameter(String paramName) {
            this.parameter = paramName;
            return this;
        }

        public Builder withParameterId(int id) {
            this.parameterId = id;
            return this;
        }

        public Parameter build() {
            return new Parameter(this);
        }
    }
}
