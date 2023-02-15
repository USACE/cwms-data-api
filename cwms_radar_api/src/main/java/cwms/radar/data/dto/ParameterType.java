package cwms.radar.data.dto;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import com.fasterxml.jackson.databind.annotation.JsonPOJOBuilder;

@JsonDeserialize(builder = ParameterType.Builder.class)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonNaming(PropertyNamingStrategies.KebabCaseStrategy.class)
public class ParameterType {

    private final String type;

    public ParameterType(Builder builder) {
        this.type = builder.type;
    }

    public String getType() {
        return type;
    }

    @Override
    public String toString() {
        return getType();
    }

    @JsonPOJOBuilder
    @JsonNaming(PropertyNamingStrategies.KebabCaseStrategy.class)
    public static class Builder {
        private String type;

        public Builder() {
        }

        public ParameterType build() {
            return new ParameterType(this);
        }

        public Builder withType(String inst) {
            this.type = inst;

            return this;
        }

        public Builder withType(mil.army.usace.hec.metadata.ParameterType pt) {
            return withType(pt.getParameterType());
        }
    }
}
