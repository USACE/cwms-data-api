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
@JsonDeserialize(builder = TimeSeriesProfileParserIndexed.Builder.class)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonNaming(PropertyNamingStrategies.KebabCaseStrategy.class)
@JsonTypeName("indexed-timeseries-profile-parser")
public class TimeSeriesProfileParserIndexed extends TimeSeriesProfileParser {
    private final Character fieldDelimiter;
    private final Integer timeField;
    private final String type;

    TimeSeriesProfileParserIndexed(Builder builder) {
        super(builder);
        fieldDelimiter = builder.fieldDelimiter;
        timeField = builder.timeField;
        type = builder.type;
    }

    @Override
    protected void validateInternal(CwmsDTOValidator validator) {
        validator.required(getFieldDelimiter(),"fieldDelimiter");
        validator.required(getTimeField(),"timeField");
    }


    public Character getFieldDelimiter() {
        return fieldDelimiter;
    }


    public BigInteger getTimeField() {
        return BigInteger.valueOf(timeField);
    }

    public String getType() {
        return type;
    }

    @JsonPOJOBuilder
    @JsonNaming(PropertyNamingStrategies.KebabCaseStrategy.class)
    public static class Builder extends TimeSeriesProfileParser.Builder {
        private Character fieldDelimiter = null;
        private Integer timeField = null;
        private String type = "indexed-timeseries-profile-parser";


        public TimeSeriesProfileParserIndexed.Builder withFieldDelimiter(char delimiter) {
            this.fieldDelimiter = delimiter;
            return this;
        }


        public TimeSeriesProfileParserIndexed.Builder withTimeField(int field) {
            this.timeField = field;
            return this;
        }

        public TimeSeriesProfileParserIndexed.Builder withType(String type) {
            this.type = type;
            return this;
        }

        @Override
        public TimeSeriesProfileParserIndexed build() {
            return new TimeSeriesProfileParserIndexed(this);
        }
    }

}
