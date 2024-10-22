package cwms.cda.data.dto.timeseriesprofile;

import static cwms.cda.api.timeseriesprofile.TimeSeriesProfileParserBase.COLUMNAR_TYPE;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import com.fasterxml.jackson.databind.annotation.JsonPOJOBuilder;
import cwms.cda.formatters.Formats;
import cwms.cda.formatters.annotations.FormattableWith;
import cwms.cda.formatters.json.JsonV1;


@FormattableWith(contentType = Formats.JSONV1, formatter = JsonV1.class)
@JsonDeserialize(builder = TimeSeriesProfileParserColumnar.Builder.class)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonNaming(PropertyNamingStrategies.KebabCaseStrategy.class)
@JsonTypeName(COLUMNAR_TYPE)
@JsonIgnoreProperties("type")
public final class TimeSeriesProfileParserColumnar extends TimeSeriesProfileParser {
    private final Integer timeStartColumn;
    private final Integer timeEndColumn;

    TimeSeriesProfileParserColumnar(Builder builder) {
        super(builder);
        timeStartColumn = builder.timeStartColumn;
        timeEndColumn = builder.timeEndColumn;
    }

    public Integer getTimeStartColumn() {
        return timeStartColumn;
    }

    public Integer getTimeEndColumn() {
        return timeEndColumn;
    }

    @Override
    public String getType() {
        return COLUMNAR_TYPE;
    }

    @JsonPOJOBuilder
    @JsonNaming(PropertyNamingStrategies.KebabCaseStrategy.class)
    public static final class Builder extends TimeSeriesProfileParser.Builder<TimeSeriesProfileParserColumnar> {
        private Integer timeStartColumn = null;
        private Integer timeEndColumn = null;

        public TimeSeriesProfileParserColumnar.Builder withTimeStartColumn(Integer timeStartColumn) {
            this.timeStartColumn = timeStartColumn;
            return this;
        }

        public TimeSeriesProfileParserColumnar.Builder withTimeEndColumn(Integer timeEndColumn) {
            this.timeEndColumn = timeEndColumn;
            return this;
        }

        @Override
        public TimeSeriesProfileParserColumnar build() {
            return new TimeSeriesProfileParserColumnar(this);
        }
    }

}
