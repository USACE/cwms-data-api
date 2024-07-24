package cwms.cda.data.dto.timeseriesprofile;

import java.util.Objects;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import com.fasterxml.jackson.databind.annotation.JsonPOJOBuilder;
import cwms.cda.api.errors.FieldException;
import cwms.cda.data.dto.CwmsDTOBase;

@JsonDeserialize(builder = ParameterInfo.Builder.class)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonNaming(PropertyNamingStrategies.KebabCaseStrategy.class)
public final class ParameterInfo implements CwmsDTOBase {
    private final String parameter;
    private final String unit;
    private final Integer index;
    private final Integer startColumn;
    private final Integer endColumn;

    ParameterInfo(ParameterInfo.Builder builder) {
        parameter = builder.parameter;
        unit = builder.unit;
        index = builder.index;
        startColumn = builder.startColumn;
        endColumn = builder.endColumn;
    }

    public String getParameter() {
        return parameter;
    }

    public String getUnit() {
        return unit;
    }

    public Integer getIndex() {
        return index;
    }

    public Integer getStartColumn(){
        return startColumn;
    }
    public Integer getEndColumn(){
        return endColumn;
    }
    @Override
    public void validate() throws FieldException {
        if(index==null && !(startColumn!=null && endColumn!=null))
        {
            throw new FieldException("if index is null, startColumn and endColumn must be defined!");
        }
        if(index!=null && (startColumn!=null || endColumn!=null))
        {
            throw new FieldException("if index is defined, startColumn and endColumn must not be defined!");
        }
    }

    @Override
    public int hashCode() {
        int result = Objects.hashCode(getParameter());
        result = 31 * result + Objects.hashCode(getUnit());
        result = 31 * result + Objects.hashCode(getIndex());
        return result;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        ParameterInfo that = (ParameterInfo) o;
        return Objects.equals(getParameter(), that.getParameter())
                && Objects.equals(getUnit(), that.getUnit())
                && Objects.equals(getIndex(), that.getIndex());
    }

    @JsonPOJOBuilder
    @JsonNaming(PropertyNamingStrategies.KebabCaseStrategy.class)
    public static final class Builder {
        private String parameter;
        private String unit;
        private Integer index;
        private Integer startColumn;
        private Integer endColumn;

        public ParameterInfo.Builder withParameter(String parameter) {
            this.parameter = parameter;
            return this;
        }

        public ParameterInfo.Builder withUnit(String unit) {
            this.unit = unit;
            return this;
        }

        public ParameterInfo.Builder withIndex(int index) {
            this.index = index;
            return this;
        }

        public ParameterInfo.Builder withStartColumn(int startColumn){
            this.startColumn = startColumn;
            return this;
        }
        public ParameterInfo.Builder withEndColumn(int endColumn){
            this.endColumn = endColumn;
            return this;
        }
        public ParameterInfo build() {
            return new ParameterInfo(this);
        }

    }
}