package cwms.cda.data.dto.timeseriesprofile;


import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import com.fasterxml.jackson.databind.annotation.JsonPOJOBuilder;
import cwms.cda.data.dto.CwmsDTOValidator;
import cwms.cda.formatters.Formats;
import cwms.cda.formatters.annotations.FormattableWith;
import cwms.cda.formatters.json.JsonV2;

@FormattableWith(contentType = Formats.JSONV2, formatter = JsonV2.class)
@JsonDeserialize(builder = ParameterInfoIndexed.Builder.class)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonNaming(PropertyNamingStrategies.KebabCaseStrategy.class)
public final class ParameterInfoIndexed extends ParameterInfo {
    private final Integer index;

    ParameterInfoIndexed(ParameterInfoIndexed.Builder builder) {
        super(builder);
        index = builder.index;
    }

    public Integer getIndex() {
        return index;
    }

    @Override
    protected void validateInternal(CwmsDTOValidator validator) {
        validator.required(getIndex(), "index");
    }

    @JsonIgnore
    @Override
    public String getParameterInfoString() {
        return getParameter() +
                "," +
                getUnit() +
                "," +
                getIndex() +
                "," +
                ",";
    }


    @JsonPOJOBuilder
    @JsonNaming(PropertyNamingStrategies.KebabCaseStrategy.class)
    public static final class Builder extends ParameterInfo.Builder {
        private Integer index;

        public ParameterInfoIndexed.Builder withIndex(int index) {
            this.index = index;
            return this;
        }

        public ParameterInfo build() {
            return new ParameterInfoIndexed(this);
        }

    }
}