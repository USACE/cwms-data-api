package cwms.cda.data.dto;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import com.fasterxml.jackson.databind.annotation.JsonPOJOBuilder;
import cwms.cda.api.errors.ExclusiveFieldsException;
import cwms.cda.api.errors.FieldException;
import cwms.cda.api.errors.RequiredFieldException;
import cwms.cda.formatters.Formats;
import cwms.cda.formatters.annotations.FormattableWith;
import cwms.cda.formatters.json.JsonV2;
import hec.data.level.ILocationLevelRef;
import hec.data.level.IParameterTypedValue;
import hec.data.level.ISeasonalInterval;
import hec.data.level.ISeasonalValue;
import hec.data.level.ISeasonalValues;
import hec.data.level.JDomLocationLevelImpl;
import io.swagger.v3.oas.annotations.media.Schema;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import rma.util.RMAConst;

@JsonDeserialize(builder = LocationLevel.Builder.class)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonNaming(PropertyNamingStrategies.KebabCaseStrategy.class)
@FormattableWith(contentType = Formats.JSONV2, formatter = JsonV2.class)
public final class LocationLevel extends CwmsDTO {
    @JsonProperty(required = true)
    @Schema(description = "Name of the location level")
    private final String locationLevelId;
    @Schema(description = "Timeseries ID (e.g. from the times series catalog) to use as the "
            + "location level. Mutually exclusive with seasonalValues and "
            + "siParameterUnitsConstantValue")
    private final String seasonalTimeSeriesId;
    @Schema(description = "List of Repeating seasonal values. The values repeater after the "
            + "specified interval."
            + " A yearly interval seasonable could have 12 different values, one for each month for"
            + " example. Mutually exclusive with seasonalTimeSeriesId and "
            + "siParameterUnitsConstantValue")
    private final List<SeasonalValueBean> seasonalValues;
    @Schema(description = "Generic name of this location level. Common names are 'Top of Dam', "
            + "'Streambed', 'Bottom of Dam'.")
    private final String specifiedLevelId;
    @Schema(description = "To indicate if single or aggregate value", allowableValues = {"Inst",
            "Ave", "Min", "Max", "Total"})
    private final String parameterTypeId;
    @Schema(description = "Data Type such as Stage, Elevation, or others.")
    private final String parameterId;
    @Schema(description = "Single value for this location level. Mutually exclusive with "
            + "seasonableTimeSeriesId and seasonValues.")
    private final Double constantValue;
    @Schema(description = "Units the provided levels are in")
    private final String levelUnitsId;
    @Schema(description = "The date/time at which this location level configuration takes effect.")
    @JsonFormat(shape = JsonFormat.Shape.STRING)
    private final ZonedDateTime levelDate;
    private final String levelComment;
    @Schema(description = "The start point of provided seasonal values")
    @JsonFormat(shape = JsonFormat.Shape.STRING)
    private final ZonedDateTime intervalOrigin;
    private final Integer intervalMonths;
    private final Integer intervalMinutes;
    @Schema(description = "Indicating whether or not to interpolate between seasonal values.",
            allowableValues = {"T", "F"})
    private final String interpolateString;
    @Schema(description = "0 if parameterTypeId is Inst. Otherwise duration indicating the time "
            + "window of the aggregate value.")
    private final String durationId;
    private final BigDecimal attributeValue;
    private final String attributeUnitsId;
    private final String attributeParameterTypeId;
    private final String attributeParameterId;
    private final String attributeDurationId;
    private final String attributeComment;

    private LocationLevel(Builder builder) {
        super(builder.officeId);
        seasonalTimeSeriesId = builder.seasonalTimeSeriesId;
        seasonalValues = builder.seasonalValues;
        specifiedLevelId = builder.specifiedLevelId;
        parameterTypeId = builder.parameterTypeId;
        parameterId = builder.parameterId;
        constantValue = builder.constantValue;
        levelUnitsId = builder.levelUnitsId;
        levelDate = builder.levelDate;
        levelComment = builder.levelComment;
        intervalOrigin = builder.intervalOrigin;
        intervalMonths = builder.intervalMonths;
        intervalMinutes = builder.intervalMinutes;
        interpolateString = builder.interpolateString;
        durationId = builder.durationId;
        attributeValue = builder.attributeValue;
        attributeUnitsId = builder.attributeUnitsId;
        attributeParameterTypeId = builder.attributeParameterTypeId;
        attributeParameterId = builder.attributeParameterId;
        attributeDurationId = builder.attributeDurationId;
        attributeComment = builder.attributeComment;
        locationLevelId = builder.locationId;
    }

    public String getSeasonalTimeSeriesId() {
        return seasonalTimeSeriesId;
    }

    public List<SeasonalValueBean> getSeasonalValues() {
        return seasonalValues;
    }

    public String getSpecifiedLevelId() {
        return specifiedLevelId;
    }

    public String getParameterTypeId() {
        return parameterTypeId;
    }

    public String getParameterId() {
        return parameterId;
    }

    public Double getConstantValue() {
        return constantValue;
    }

    public String getLevelUnitsId() {
        return levelUnitsId;
    }

    public ZonedDateTime getLevelDate() {
        return levelDate;
    }

    public String getLevelComment() {
        return levelComment;
    }

    public ZonedDateTime getIntervalOrigin() {
        return intervalOrigin;
    }

    public Integer getIntervalMonths() {
        return intervalMonths;
    }

    public Integer getIntervalMinutes() {
        return intervalMinutes;
    }

    public String getInterpolateString() {
        return interpolateString;
    }

    public String getDurationId() {
        return durationId;
    }

    public BigDecimal getAttributeValue() {
        return attributeValue;
    }

    public String getAttributeUnitsId() {
        return attributeUnitsId;
    }

    public String getAttributeParameterTypeId() {
        return attributeParameterTypeId;
    }

    public String getAttributeParameterId() {
        return attributeParameterId;
    }

    public String getAttributeDurationId() {
        return attributeDurationId;
    }

    public String getAttributeComment() {
        return attributeComment;
    }

    public String getLocationLevelId() {
        return locationLevelId;
    }

    @JsonPOJOBuilder
    @JsonNaming(PropertyNamingStrategies.KebabCaseStrategy.class)
    public static class Builder {
        private String seasonalTimeSeriesId;
        private List<SeasonalValueBean> seasonalValues;
        private String specifiedLevelId;
        private String parameterTypeId;
        private String parameterId;
        private Double constantValue;
        private String levelUnitsId;
        private ZonedDateTime levelDate;
        private String levelComment;
        private ZonedDateTime intervalOrigin;
        private Integer intervalMonths;
        private Integer intervalMinutes;
        private String interpolateString;
        private String durationId;
        private BigDecimal attributeValue;
        private String attributeUnitsId;
        private String attributeParameterTypeId;
        private String attributeParameterId;
        private String attributeDurationId;
        private String attributeComment;
        private String locationId;
        private String officeId;
        private final Map<String, Consumer<Object>> propertyFunctionMap = new HashMap<>();

        @JsonCreator
        public Builder(@JsonProperty(value = "location-level-id", required = true) String name,
                   @JsonProperty(value = "level-date", required = true) ZonedDateTime lvlDate) {
            locationId = name;
            levelDate = lvlDate;
            buildPropertyFunctions();
        }

        public Builder(LocationLevel copyFrom) {
            withAttributeComment(copyFrom.getAttributeComment());
            withAttributeDurationId(copyFrom.getAttributeDurationId());
            withAttributeParameterId(copyFrom.getAttributeParameterId());
            withLocationLevelId(copyFrom.getLocationLevelId());
            withAttributeValue(copyFrom.getAttributeValue());
            withAttributeParameterTypeId(copyFrom.getAttributeParameterTypeId());
            withAttributeUnitsId(copyFrom.getAttributeUnitsId());
            withDurationId(copyFrom.getDurationId());
            withInterpolateString(copyFrom.getInterpolateString());
            withIntervalMinutes(copyFrom.getIntervalMinutes());
            withIntervalMonths(copyFrom.getIntervalMonths());
            withIntervalOrigin(copyFrom.getIntervalOrigin());
            withLevelComment(copyFrom.getLevelComment());
            withLevelDate(copyFrom.getLevelDate());
            withLevelUnitsId(copyFrom.getLevelUnitsId());
            withOfficeId(copyFrom.getOfficeId());
            withParameterId(copyFrom.getParameterId());
            withParameterTypeId(copyFrom.getParameterTypeId());
            withSeasonalTimeSeriesId(copyFrom.getSeasonalTimeSeriesId());
            withSeasonalValues(copyFrom.getSeasonalValues());
            withConstantValue(copyFrom.getConstantValue());
            withSpecifiedLevelId(copyFrom.getSpecifiedLevelId());
            buildPropertyFunctions();
        }

        public Builder(JDomLocationLevelImpl copyFrom) {
            withAttributeComment(copyFrom.getAttributeComment());
            withAttributeDurationId(copyFrom.getAttributeDurationId());
            withAttributeParameterId(copyFrom.getAttributeParameterId());
            ILocationLevelRef locationLevelRef = copyFrom.getLocationLevelRef();
            if (locationLevelRef != null) {
                withLocationLevelId(locationLevelRef.getLocationLevelId());
            }
            withAttributeValue(copyFrom.getAttributeValue());
            withAttributeParameterTypeId(copyFrom.getAttributeParameterTypeId());
            withAttributeUnitsId(copyFrom.getAttributeUnitsId());
            withDurationId(copyFrom.getDurationId());
            withInterpolateString(copyFrom.getInterpolateString());
            withIntervalMinutes(copyFrom.getIntervalMinutes());
            withIntervalMonths(copyFrom.getIntervalMonths());
            Date intervalOriginDate = copyFrom.getIntervalOrigin();
            if (intervalOriginDate != null) {
                withIntervalOrigin(ZonedDateTime.ofInstant(intervalOriginDate.toInstant(),
                        ZoneId.of("UTC")));
            }

            withLevelComment(copyFrom.getLevelComment());
            Date copyLevelDate = copyFrom.getLevelDate();
            if (copyLevelDate != null) {
                withLevelDate(ZonedDateTime.ofInstant(copyLevelDate.toInstant(), ZoneId.of("UTC")));
            }

            withLevelUnitsId(copyFrom.getLevelUnitsId());
            withOfficeId(copyFrom.getOfficeId());
            withParameterId(copyFrom.getParameterId());
            withParameterTypeId(copyFrom.getParameterTypeId());
            withSeasonalTimeSeriesId(copyFrom.getSeasonalTimeSeriesId());
            ISeasonalValues values = copyFrom.getSeasonalValues();
            if (values != null) {
                withSeasonalValues(buildSeasonalValues(values));
            }
            IParameterTypedValue constantLevel = copyFrom.getConstantLevel();
            if (constantLevel != null) {
                withConstantValue(constantLevel.getSiParameterUnitsValue());
            }
            withSpecifiedLevelId(copyFrom.getSpecifiedLevelId());
            buildPropertyFunctions();
        }

        @JsonIgnore
        private void buildPropertyFunctions() {
            propertyFunctionMap.clear();
            propertyFunctionMap.put("location-level-id",
                nameVal -> withLocationLevelId((String) nameVal));
            propertyFunctionMap.put("seasonal-time-series-id",
                tsIdVal -> withSeasonalTimeSeriesId((String) tsIdVal));
            propertyFunctionMap.put("seasonal-values",
                seasonalVals -> withSeasonalValues((List<SeasonalValueBean>) seasonalVals));
            propertyFunctionMap.put("office-id", officeIdVal -> withOfficeId((String) officeIdVal));
            propertyFunctionMap.put("specified-level-id",
                specifiedLevelIdVal -> withSpecifiedLevelId((String) specifiedLevelIdVal));
            propertyFunctionMap.put("parameter-type-id",
                parameterTypeIdVal -> withParameterTypeId((String) parameterTypeIdVal));
            propertyFunctionMap.put("parameter-id",
                parameterIdVal -> withParameterId((String) parameterIdVal));
            propertyFunctionMap.put("si-parameter-units-constant-value",
                paramUnitsConstVal -> withConstantValue((Double) paramUnitsConstVal));
            propertyFunctionMap.put("level-units-id",
                levelUnitsIdVal -> withLevelUnitsId((String) levelUnitsIdVal));
            propertyFunctionMap.put("level-date",
                levelDateVal -> withLevelDate((ZonedDateTime) levelDateVal));
            propertyFunctionMap.put("level-comment",
                levelCommentVal -> withLevelComment((String) levelCommentVal));
            propertyFunctionMap.put("interval-origin",
                intervalOriginVal -> withIntervalOrigin((ZonedDateTime) intervalOriginVal));
            propertyFunctionMap.put("interval-months",
                months -> withIntervalMonths((Integer) months));
            propertyFunctionMap.put("interval-minutes",
                mins -> withIntervalMinutes((Integer) mins));
            propertyFunctionMap.put("interpolate-string",
                interpolateStr -> withInterpolateString((String) interpolateStr));
            propertyFunctionMap.put("duration-id",
                durationIdVal -> withDurationId((String) durationIdVal));
            propertyFunctionMap.put("attribute-value",
                attributeVal -> withAttributeValue(BigDecimal.valueOf((Double) attributeVal)));
            propertyFunctionMap.put("attribute-units-id",
                attributeUnitsIdVal -> withAttributeUnitsId((String) attributeUnitsIdVal));
            propertyFunctionMap.put("attribute-parameter-type-id",
                attributeParameterTypeIdVal ->
                        withAttributeParameterTypeId((String) attributeParameterTypeIdVal));
            propertyFunctionMap.put("attribute-parameter-id",
                attributeParameterIdVal ->
                        withAttributeParameterId((String) attributeParameterIdVal));
            propertyFunctionMap.put("attribute-duration-id",
                attributeDurationIdVal -> withAttributeDurationId((String) attributeDurationIdVal));
            propertyFunctionMap.put("attribute-comment",
                attributeCommentVal -> withAttributeComment((String) attributeCommentVal));
        }

        @JsonIgnore
        public Builder withProperty(String propertyName, Object value) {
            Consumer<Object> function = propertyFunctionMap.get(propertyName);
            if (function == null) {
                throw new IllegalArgumentException("Property Name does not exist for Location "
                        + "Level");
            }
            function.accept(value);
            return this;
        }

        public Builder withSeasonalTimeSeriesId(String seasonalTimeSeriesId) {
            this.seasonalTimeSeriesId = seasonalTimeSeriesId;
            return this;
        }

        public Builder withSeasonalValues(List<SeasonalValueBean> seasonalValues) {
            this.seasonalValues = seasonalValues;
            return this;
        }

        public static SeasonalValueBean buildSeasonalValueBean(ISeasonalValue seasonalValue) {
            ISeasonalInterval offset = seasonalValue.getOffset();
            IParameterTypedValue value = seasonalValue.getValue();
            return new SeasonalValueBean.Builder(value.getSiParameterUnitsValue())
                    .withOffsetMinutes(BigInteger.valueOf(offset.getTotalMinutes()))
                    .withOffsetMonths(offset.getTotalMonths())
                    .build();
        }

        public static List<SeasonalValueBean> buildSeasonalValues(ISeasonalValues seasonalValues) {
            List<SeasonalValueBean> retval = null;
            if (seasonalValues != null) {
                retval = new ArrayList<>();
                for (ISeasonalValue seasonalValue : seasonalValues.getSeasonalValues()) {
                    retval.add(buildSeasonalValueBean(seasonalValue));
                }
            }
            return retval;
        }

        public Builder withSpecifiedLevelId(String specifiedLevelId) {
            this.specifiedLevelId = specifiedLevelId;
            return this;
        }

        public Builder withParameterTypeId(String parameterTypeId) {
            this.parameterTypeId = parameterTypeId;
            return this;
        }

        public Builder withParameterId(String parameterId) {
            this.parameterId = parameterId;
            return this;
        }

        public Builder withConstantValue(Double value) {
            if (value != null && RMAConst.isUndefinedValue(value)) {
                value = null;
            }
            this.constantValue = value;
            return this;
        }

        public Builder withLevelUnitsId(String levelUnitsId) {
            this.levelUnitsId = levelUnitsId;
            return this;
        }

        public Builder withLevelDate(ZonedDateTime levelDate) {
            this.levelDate = levelDate;
            return this;
        }

        public Builder withLevelComment(String levelComment) {
            this.levelComment = levelComment;
            return this;
        }

        public Builder withIntervalOrigin(ZonedDateTime intervalOrigin) {
            this.intervalOrigin = intervalOrigin;
            return this;
        }

        public Builder withIntervalOrigin(Date intervalOriginDate, ZonedDateTime effectiveDate) {
            if (intervalOriginDate != null && effectiveDate != null) {
                return withIntervalOrigin(ZonedDateTime.ofInstant(intervalOriginDate.toInstant(),
                        effectiveDate.getZone()));
            } else {
                this.intervalOrigin = null;
                return this;
            }
        }

        public Builder withIntervalMonths(Integer months) {
            if (months != null && RMAConst.isUndefinedValue(months)) {
                months = null;
            }
            this.intervalMonths = months;
            return this;
        }

        public Builder withIntervalMinutes(Integer minutes) {
            if (minutes != null && RMAConst.isUndefinedValue(minutes)) {
                minutes = null;
            }
            this.intervalMinutes = minutes;
            return this;
        }

        public Builder withInterpolateString(String interpolateString) {
            this.interpolateString = interpolateString;
            return this;
        }

        public Builder withDurationId(String durationId) {
            this.durationId = durationId;
            return this;
        }

        public Builder withAttributeValue(BigDecimal attributeValue) {
            this.attributeValue = attributeValue;
            return this;
        }

        public Builder withAttributeUnitsId(String attributeUnitsId) {
            this.attributeUnitsId = attributeUnitsId;
            return this;
        }

        public Builder withAttributeParameterTypeId(String attributeParameterTypeId) {
            this.attributeParameterTypeId = attributeParameterTypeId;
            return this;
        }

        public Builder withAttributeParameterId(String attributeParameterId) {
            this.attributeParameterId = attributeParameterId;
            return this;
        }

        public Builder withAttributeDurationId(String attributeDurationId) {
            this.attributeDurationId = attributeDurationId;
            return this;
        }

        public Builder withAttributeComment(String attributeComment) {
            this.attributeComment = attributeComment;
            return this;
        }

        public Builder withLocationLevelId(String locationId) {
            this.locationId = locationId;
            return this;
        }

        public Builder withOfficeId(String officeId) {
            this.officeId = officeId;
            return this;
        }

        public LocationLevel build() {
            return new LocationLevel(this);
        }
    }

    @Override
    public void validate() throws FieldException {
        ArrayList<String> fields = new ArrayList<>();
        if (seasonalValues != null) {
            fields.add("seasonal-values");
        }

        if (constantValue != null) {
            fields.add("constant-value");
        }

        if (seasonalTimeSeriesId != null) {
            fields.add("seasonable-time-series-id");
        }
        if (fields.isEmpty()) {
            throw new RequiredFieldException(Arrays.asList("seasonal-values", "constant-value",
                    "season-time-series-id"));
        }
        if (fields.size() != 1) {
            throw new ExclusiveFieldsException(fields);
        }
    }
}
