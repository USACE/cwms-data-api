package cwms.cda.data.dto.timeseriesprofile;

import static cwms.cda.api.timeseriesprofile.TimeSeriesProfileParserBase.COLUMNAR_TYPE;
import static cwms.cda.api.timeseriesprofile.TimeSeriesProfileParserBase.INDEXED_TYPE;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import com.fasterxml.jackson.databind.annotation.JsonPOJOBuilder;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import cwms.cda.data.dto.CwmsDTOBase;
import cwms.cda.data.dto.CwmsDTOValidator;
import cwms.cda.data.dto.CwmsId;
import cwms.cda.formatters.Formats;
import cwms.cda.formatters.annotations.FormattableWith;
import cwms.cda.formatters.json.JsonV1;
import java.util.List;


@FormattableWith(contentType = Formats.JSONV1, formatter = JsonV1.class, aliases = {Formats.DEFAULT, Formats.JSON})
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
@JsonTypeName("time-series-profile-parser")
@JsonNaming(PropertyNamingStrategies.KebabCaseStrategy.class)
@JsonSerialize(as = TimeSeriesProfileParser.class)
@JsonDeserialize(builder = TimeSeriesProfileParser.Builder.class)
@JsonSubTypes({@JsonSubTypes.Type(value = TimeSeriesProfileParserIndexed.class,
        name = INDEXED_TYPE),
    @JsonSubTypes.Type(value = TimeSeriesProfileParserColumnar.class,
        name = COLUMNAR_TYPE)
})

public  class TimeSeriesProfileParser extends CwmsDTOBase {
    private final CwmsId locationId;
    private final String keyParameter;
    private final char recordDelimiter;
    private final String timeFormat;
    private final String timeZone;
    private final List<ParameterInfo> parameterInfoList;
    private final boolean timeInTwoFields;

    TimeSeriesProfileParser(Builder builder) {
        locationId = builder.locationId;
        keyParameter = builder.keyParameter;
        recordDelimiter = builder.recordDelimiter;
        timeFormat = builder.timeFormat;
        timeZone = builder.timeZone;
        parameterInfoList = builder.parameterInfoList;
        timeInTwoFields = builder.timeInTwoFields;
    }

    @Override
    protected void validateInternal(CwmsDTOValidator validator) {
        // there must be a key parameter
        validator.required(getKeyParameter(),"keyParameter");
    }

    public CwmsId getLocationId() {
        return locationId;
    }

    public String getKeyParameter() {
        return keyParameter;
    }

    public char getRecordDelimiter() {
        return recordDelimiter;
    }

    public List<ParameterInfo> getParameterInfoList() {
        return parameterInfoList;
    }

    public String getTimeFormat() {
        return timeFormat;
    }

    public String getTimeZone() {
        return timeZone;
    }

    public boolean getTimeInTwoFields() {
        return timeInTwoFields;
    }


    @JsonPOJOBuilder
    @JsonNaming(PropertyNamingStrategies.KebabCaseStrategy.class)
    public static class Builder {
        private List<ParameterInfo> parameterInfoList;
        private String keyParameter;
        private char recordDelimiter;
        private String timeFormat;
        private String timeZone;
        private boolean timeInTwoFields;
        private CwmsId locationId;

        public TimeSeriesProfileParser.Builder withLocationId(CwmsId locationId) {
            this.locationId = locationId;
            return this;
        }

        public TimeSeriesProfileParser.Builder withKeyParameter(String keyParameter) {
            this.keyParameter = keyParameter;
            return this;
        }

        public TimeSeriesProfileParser.Builder withRecordDelimiter(char delimiter) {
            this.recordDelimiter = delimiter;
            return this;
        }


        public TimeSeriesProfileParser.Builder withTimeFormat(String timeFormat) {
            this.timeFormat = timeFormat;
            return this;
        }

        public TimeSeriesProfileParser.Builder withTimeZone(String timeZone) {
            this.timeZone = timeZone;
            return this;
        }


        public TimeSeriesProfileParser.Builder withTimeInTwoFields(boolean timeInTwoFields) {
            this.timeInTwoFields = timeInTwoFields;
            return this;
        }

        public TimeSeriesProfileParser.Builder withParameterInfoList(List<ParameterInfo> parameterInfoList) {
            this.parameterInfoList = parameterInfoList;
            return this;
        }

        public TimeSeriesProfileParser build() {
            return new TimeSeriesProfileParser(this);
        }
    }

}
