package cwms.cda.data.dto.timeseriesprofile;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import com.fasterxml.jackson.databind.annotation.JsonPOJOBuilder;
import cwms.cda.data.dto.CwmsDTOValidator;
import cwms.cda.formatters.Formats;
import cwms.cda.formatters.annotations.FormattableWith;
import cwms.cda.formatters.json.JsonV1;
import java.math.BigInteger;

@FormattableWith(contentType = Formats.JSONV1, formatter = JsonV1.class)
@JsonDeserialize(builder = TimeSeriesProfileParserColumnar.Builder.class)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonNaming(PropertyNamingStrategies.KebabCaseStrategy.class)
@JsonTypeName("columnar-timeseries-profile-parser")
public final class TimeSeriesProfileParserColumnar extends TimeSeriesProfileParser {
    private final Integer timeStartColumn;
    private final Integer timeEndColumn;

    TimeSeriesProfileParserColumnar(Builder builder) {
        super(builder);
        timeStartColumn = builder.timeStartColumn;
        timeEndColumn = builder.timeEndColumn;
    }

    @Override
    protected void validateInternal(CwmsDTOValidator validator) {
        validator.required(getTimeStartColumn(),"timeStartColumn");
        validator.required(getTimeEndColumn(),"timeEndColumn");
    }

    public BigInteger getTimeStartColumn() {
        if (timeStartColumn != null) {
            return BigInteger.valueOf(timeStartColumn);
        }
        return null;
    }

    public BigInteger getTimeEndColumn() {
        if (timeEndColumn != null) {
            return BigInteger.valueOf(timeEndColumn);
        }
        return null;
    }

    @JsonPOJOBuilder
    @JsonNaming(PropertyNamingStrategies.KebabCaseStrategy.class)
    public static final class Builder extends TimeSeriesProfileParser.Builder {
        private Integer timeStartColumn = null;
        private Integer timeEndColumn = null;

        public TimeSeriesProfileParserColumnar.Builder withTimeStartColumn(int timeStartColumn) {
            this.timeStartColumn = timeStartColumn;
            return this;
        }

        public TimeSeriesProfileParserColumnar.Builder withTimeEndColumn(int timeEndColumn) {
            this.timeEndColumn = timeEndColumn;
            return this;
        }

        @Override
        public TimeSeriesProfileParserColumnar build() {
            return new TimeSeriesProfileParserColumnar(this);
        }
    }

}
