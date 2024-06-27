package cwms.cda.data.dto.timeseriesprofile;

import java.math.BigInteger;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import com.fasterxml.jackson.databind.annotation.JsonPOJOBuilder;
import cwms.cda.api.errors.FieldException;
import cwms.cda.data.dto.CwmsDTOBase;
import cwms.cda.data.dto.CwmsId;
import cwms.cda.formatters.Formats;
import cwms.cda.formatters.annotations.FormattableWith;
import cwms.cda.formatters.json.JsonV2;

@FormattableWith(contentType = Formats.JSONV2, formatter = JsonV2.class)
@JsonDeserialize(builder = TimeSeriesProfileParser.Builder.class)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonNaming(PropertyNamingStrategies.KebabCaseStrategy.class)
public class TimeSeriesProfileParser implements CwmsDTOBase {
    private final CwmsId locationId;
    private final String keyParameter;
    private final char recordDelimiter;
    private final char fieldDelimiter;
    private final String timeFormat;
    private final String timeZone;
    private final int timeField;
    private final List<ParameterInfo> parameterInfoList;
    private final boolean timeInTwoFields;

    protected TimeSeriesProfileParser(Builder builder) {
        locationId = builder.locationId;
        keyParameter = builder.keyParameter;
        recordDelimiter = builder.recordDelimiter;
        fieldDelimiter = builder.fieldDelimiter;
        timeFormat = builder.timeFormat;
        timeZone = builder.timeZone;
        timeField = builder.timeField;
        parameterInfoList = builder.parameterInfoList;
        timeInTwoFields = builder.timeInTwoFields;
    }

    @Override
    public void validate() throws FieldException {
        if (this.keyParameter == null) {
            throw new FieldException("Key Parameter field can't be null");
        }
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

    public char getFieldDelimiter() {
        return fieldDelimiter;
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

    public BigInteger getTimeField() {
        return BigInteger.valueOf(timeField);
    }

    public boolean getTimeInTwoFields() {
        return timeInTwoFields;
    }


    @JsonPOJOBuilder
    @JsonNaming(PropertyNamingStrategies.KebabCaseStrategy.class)
    public static final class Builder {
        private List<ParameterInfo> parameterInfoList;
        private String keyParameter;
        private char recordDelimiter;
        private char fieldDelimiter;
        private String timeFormat;
        private String timeZone;
        private int timeField;
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

        public TimeSeriesProfileParser.Builder withFieldDelimiter(char delimiter) {
            this.fieldDelimiter = delimiter;
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

        public TimeSeriesProfileParser.Builder withTimeField(int field) {
            this.timeField = field;
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
