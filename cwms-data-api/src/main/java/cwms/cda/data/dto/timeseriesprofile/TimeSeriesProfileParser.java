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
public final class TimeSeriesProfileParser implements CwmsDTOBase {
    private final CwmsId locationId;
    private final String keyParameter;
    private final char recordDelimiter;
    private final Character fieldDelimiter;
    private final String timeFormat;
    private final String timeZone;
    private final Integer timeField;
    private final List<ParameterInfo> parameterInfoList;
    private final boolean timeInTwoFields;
    private final Integer timeStartColumn;
    private final Integer timeEndColumn;

    private TimeSeriesProfileParser(Builder builder) {
        locationId = builder.locationId;
        keyParameter = builder.keyParameter;
        recordDelimiter = builder.recordDelimiter;
        fieldDelimiter = builder.fieldDelimiter;
        timeFormat = builder.timeFormat;
        timeZone = builder.timeZone;
        timeField = builder.timeField;
        parameterInfoList = builder.parameterInfoList;
        timeInTwoFields = builder.timeInTwoFields;
        timeStartColumn = builder.timeStartColumn;
        timeEndColumn = builder.timeEndColumn;
    }

    @Override
    public void validate() throws FieldException {
        // there must be a key parameter
        if (this.keyParameter == null) {
            throw new FieldException("Key Parameter field can't be null");
        }
        // if there is a field delimiter, and it is part of the time format, time in two fields must be true
        // if we have a field delimiter and it is not contained in the timeformat, then timeInTwoField must be false
        if(fieldDelimiter!=null && (-1==timeFormat.indexOf(fieldDelimiter) && timeInTwoFields))
        {
            throw new FieldException("Field delimiter '"+fieldDelimiter+"' is not part of time format "
                        +timeFormat+". timeInTwoFields must be false.");
        }
        // if there is no field delimiter, time start column and time end column must be defined
        if(fieldDelimiter==null && (timeStartColumn==null || timeEndColumn ==null))
        {
            throw new FieldException("If Field delimiter is undefined, then timeStartColumn and timeEndColumn must be defined.");
        }
        // if there is no field delimiter, parameter info must provide a start and end column
        if(fieldDelimiter==null)
        {
            for(ParameterInfo parameterInfo : parameterInfoList)
            {
                if(parameterInfo.getStartColumn()==null || parameterInfo.getEndColumn()==null)
                {
                    throw new FieldException("If Field delimiter is undefined, then startColumn and endColumn must be defined for ParameterInfo.");
                }
            }
        }
        // if time field is undefined, it is columnar format and time start column and time end column must be defined
        if(timeStartColumn!=null&&timeEndColumn!=null&&timeField!=null)
        {
            throw new FieldException("If time field is defined, then startColumn and endColumn must be undefined.");
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

    public Character getFieldDelimiter() {
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
        return timeField==null ? null : BigInteger.valueOf(timeField);
    }

    public boolean getTimeInTwoFields() {
        return timeInTwoFields;
    }

    public BigInteger getTimeStartColumn() {
        if (timeStartColumn != null) {
            return BigInteger.valueOf(timeStartColumn);
        }
        return null;
    }
    public BigInteger getTimeEndColumn(){
        if (timeEndColumn != null) {
            return BigInteger.valueOf(timeEndColumn);
        }
        return null;
    }

    @JsonPOJOBuilder
    @JsonNaming(PropertyNamingStrategies.KebabCaseStrategy.class)
    public static final class Builder {
        private List<ParameterInfo> parameterInfoList;
        private String keyParameter;
        private char recordDelimiter;
        private Character fieldDelimiter = null;
        private String timeFormat;
        private String timeZone;
        private Integer timeField = null;
        private boolean timeInTwoFields;
        private CwmsId locationId;
        private Integer timeStartColumn = null;
        private Integer timeEndColumn = null;

        public TimeSeriesProfileParser.Builder withLocationId(CwmsId locationId) {
            this.locationId = locationId;
            return this;
        }

        public TimeSeriesProfileParser.Builder withTimeStartColumn(int timeStartColumn)
        {
            this.timeStartColumn = timeStartColumn;
            return this;
        }
        public TimeSeriesProfileParser.Builder withTimeEndColumn(int timeEndColumn)
        {
            this.timeEndColumn = timeEndColumn;
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
