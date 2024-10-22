package cwms.cda.data.dto.timeseriesprofile;

import static cwms.cda.api.timeseriesprofile.TimeSeriesProfileParserBase.INDEXED_TYPE;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import com.fasterxml.jackson.databind.annotation.JsonPOJOBuilder;
import cwms.cda.formatters.Formats;
import cwms.cda.formatters.annotations.FormattableWith;
import cwms.cda.formatters.json.JsonV1;


@FormattableWith(contentType = Formats.JSONV1, formatter = JsonV1.class)
@JsonDeserialize(builder = TimeSeriesProfileParserIndexed.Builder.class)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonNaming(PropertyNamingStrategies.KebabCaseStrategy.class)
@JsonTypeName(INDEXED_TYPE)
@JsonIgnoreProperties("type")
public class TimeSeriesProfileParserIndexed extends TimeSeriesProfileParser {
    @JsonProperty(required = true)
    private final Character fieldDelimiter;
    @JsonProperty(required = true)
    private final Long timeField;

    TimeSeriesProfileParserIndexed(Builder builder) {
        super(builder);
        fieldDelimiter = builder.fieldDelimiter;
        timeField = builder.timeField;
    }


    public Character getFieldDelimiter() {
        return fieldDelimiter;
    }


    public Long getTimeField() {
        return timeField;
    }

    @Override
    public String getType() {
        return INDEXED_TYPE;
    }

    @JsonPOJOBuilder
    @JsonNaming(PropertyNamingStrategies.KebabCaseStrategy.class)
    public static class Builder extends TimeSeriesProfileParser.Builder<TimeSeriesProfileParserIndexed> {
        private Character fieldDelimiter = null;
        private Long timeField;

        public TimeSeriesProfileParserIndexed.Builder withFieldDelimiter(char delimiter) {
            this.fieldDelimiter = delimiter;
            return this;
        }


        public TimeSeriesProfileParserIndexed.Builder withTimeField(Long field) {
            this.timeField = field;
            return this;
        }

        @Override
        public TimeSeriesProfileParserIndexed build() {
            return new TimeSeriesProfileParserIndexed(this);
        }
    }

}
