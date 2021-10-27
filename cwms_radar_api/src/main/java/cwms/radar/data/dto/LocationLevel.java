package cwms.radar.data.dto;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import com.fasterxml.jackson.databind.annotation.JsonPOJOBuilder;
import usace.cwms.db.dao.ifc.level.SeasonalValueBean;

import java.math.BigDecimal;
import java.time.ZonedDateTime;
import java.util.*;
import java.util.function.Consumer;

@JsonDeserialize(builder = LocationLevel.Builder.class)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonNaming(PropertyNamingStrategies.KebabCaseStrategy.class)
public final class LocationLevel implements CwmsDTO
{
    private final String seasonalTimeSeriesId;
    private final List<SeasonalValueBean> seasonalValues;
    private final String specifiedLevelId;
    private final String parameterTypeId;
    private final String parameterId;
    private final Double siParameterUnitsConstantValue;
    private final String levelUnitsId;
    private final ZonedDateTime levelDate;
    private final String levelComment;
    private final ZonedDateTime intervalOrigin;
    private final Integer intervalMonths;
    private final Integer intervalMinutes;
    private final String interpolateString;
    private final String durationId;
    private final BigDecimal attributeValue;
    private final String attributeUnitsId;
    private final String attributeParameterTypeId;
    private final String attributeParameterId;
    private final String attributeDurationId;
    private final String attributeComment;
    private final String locationId;
    private final String officeId;

    @JsonCreator
    public LocationLevel(Builder builder)
    {
        seasonalTimeSeriesId = builder.seasonalTimeSeriesId;
        seasonalValues = builder.seasonalValues;
        specifiedLevelId = builder.specifiedLevelId;
        parameterTypeId = builder.parameterTypeId;
        parameterId = builder.parameterId;
        siParameterUnitsConstantValue = builder.siParameterUnitsConstantValue;
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
        locationId = builder.locationId;
        officeId = builder.officeId;
    }

    public String getSeasonalTimeSeriesId()
    {
        return seasonalTimeSeriesId;
    }

    public List<SeasonalValueBean> getSeasonalValues()
    {
        return seasonalValues;
    }

    public String getSpecifiedLevelId()
    {
        return specifiedLevelId;
    }

    public String getParameterTypeId()
    {
        return parameterTypeId;
    }

    public String getParameterId()
    {
        return parameterId;
    }

    public Double getSiParameterUnitsConstantValue()
    {
        return siParameterUnitsConstantValue;
    }

    public String getLevelUnitsId()
    {
        return levelUnitsId;
    }

    public ZonedDateTime getLevelDate()
    {
        return levelDate;
    }

    public String getLevelComment()
    {
        return levelComment;
    }

    public ZonedDateTime getIntervalOrigin()
    {
        return intervalOrigin;
    }

    public Integer getIntervalMonths()
    {
        return intervalMonths;
    }

    public Integer getIntervalMinutes()
    {
        return intervalMinutes;
    }

    public String getInterpolateString()
    {
        return interpolateString;
    }

    public String getDurationId()
    {
        return durationId;
    }

    public BigDecimal getAttributeValue()
    {
        return attributeValue;
    }

    public String getAttributeUnitsId()
    {
        return attributeUnitsId;
    }

    public String getAttributeParameterTypeId()
    {
        return attributeParameterTypeId;
    }

    public String getAttributeParameterId()
    {
        return attributeParameterId;
    }

    public String getAttributeDurationId()
    {
        return attributeDurationId;
    }

    public String getAttributeComment()
    {
        return attributeComment;
    }

    public String getLocationId()
    {
        return locationId;
    }

    public String getOfficeId()
    {
        return officeId;
    }

    @JsonPOJOBuilder
    @JsonNaming(PropertyNamingStrategies.KebabCaseStrategy.class)
    public static class Builder
    {
        private String seasonalTimeSeriesId;
        private List<SeasonalValueBean> seasonalValues;
        private String specifiedLevelId;
        private String parameterTypeId;
        private String parameterId;
        private Double siParameterUnitsConstantValue;
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
        public Builder(@JsonProperty(value = "location-id") String name, @JsonProperty(value = "level-date") ZonedDateTime effectiveDate)
        {
            locationId = name;
            levelDate = effectiveDate;
            buildPropertyFunctions();
        }

        public Builder(LocationLevel copyFrom)
        {
            withAttributeComment(copyFrom.getAttributeComment());
            withAttributeDurationId(copyFrom.getAttributeDurationId());
            withAttributeParameterId(copyFrom.getAttributeParameterId());
            withLocationId(copyFrom.getLocationId());
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
            withSiParameterUnitsConstantValue(copyFrom.getSiParameterUnitsConstantValue());
            withSpecifiedLevelId(copyFrom.getSpecifiedLevelId());
            buildPropertyFunctions();
        }

        @JsonIgnore
        private void buildPropertyFunctions()
        {
            propertyFunctionMap.clear();
            propertyFunctionMap.put("location-id", nameVal -> withLocationId((String)nameVal));
            propertyFunctionMap.put("seasonal-time-series-id", tsIdVal -> withSeasonalTimeSeriesId((String)tsIdVal));
            propertyFunctionMap.put("seasonal-values", seasonalVals -> withSeasonalValues((List<SeasonalValueBean>)seasonalVals));
            propertyFunctionMap.put("office-id", officeIdVal -> withOfficeId((String)officeIdVal));
            propertyFunctionMap.put("specified-level-id", specifiedLevelIdVal -> withSpecifiedLevelId((String)specifiedLevelIdVal));
            propertyFunctionMap.put("parameter-type-id", parameterTypeIdVal -> withParameterTypeId((String)parameterTypeIdVal));
            propertyFunctionMap.put("parameter-id", parameterIdVal -> withParameterId((String)parameterIdVal));
            propertyFunctionMap.put("si-parameter-units-constant-value", paramUnitsConstVal -> withSiParameterUnitsConstantValue((Double)paramUnitsConstVal));
            propertyFunctionMap.put("level-units-id", levelUnitsIdVal -> withLevelUnitsId((String)levelUnitsIdVal));
            propertyFunctionMap.put("level-date", levelDateVal -> withLevelDate((ZonedDateTime)levelDateVal));
            propertyFunctionMap.put("level-comment", levelCommentVal -> withLevelComment((String)levelCommentVal));
            propertyFunctionMap.put("interval-origin", intervalOriginVal -> withIntervalOrigin((ZonedDateTime) intervalOriginVal));
            propertyFunctionMap.put("interval-months", months -> withIntervalMonths((Integer)months));
            propertyFunctionMap.put("interval-minutes", mins -> withIntervalMinutes((Integer)mins));
            propertyFunctionMap.put("interpolate-string", interpolateStr -> withInterpolateString((String)interpolateStr));
            propertyFunctionMap.put("duration-id", durationIdVal -> withDurationId((String)durationIdVal));
            propertyFunctionMap.put("attribute-value", attributeVal -> withAttributeValue(BigDecimal.valueOf((Double)attributeVal)));
            propertyFunctionMap.put("attribute-units-id", attributeUnitsIdVal -> withAttributeUnitsId((String)attributeUnitsIdVal));
            propertyFunctionMap.put("attribute-parameter-type-id", attributeParameterTypeIdVal -> withAttributeParameterTypeId((String)attributeParameterTypeIdVal));
            propertyFunctionMap.put("attribute-parameter-id", attributeParameterIdVal -> withAttributeParameterId((String)attributeParameterIdVal));
            propertyFunctionMap.put("attribute-duration-id", attributeDurationIdVal -> withAttributeDurationId((String)attributeDurationIdVal));
            propertyFunctionMap.put("attribute-comment", attributeCommentVal -> withAttributeComment((String)attributeCommentVal));
        }

        @JsonIgnore
        public Builder withProperty(String propertyName, Object value)
        {
            Consumer<Object> function = propertyFunctionMap.get(propertyName);
            if(function == null)
            {
                throw new IllegalArgumentException("Property Name does not exist for Location Level");
            }
            function.accept(value);
            return this;
        }

        public Builder withSeasonalTimeSeriesId(String seasonalTimeSeriesId)
        {
            this.seasonalTimeSeriesId = seasonalTimeSeriesId;
            return this;
        }

        public Builder withSeasonalValues(List<SeasonalValueBean> seasonalValues)
        {
            this.seasonalValues = seasonalValues;
            return this;
        }

        public Builder withSpecifiedLevelId(String specifiedLevelId)
        {
            this.specifiedLevelId = specifiedLevelId;
            return this;
        }

        public Builder withParameterTypeId(String parameterTypeId)
        {
            this.parameterTypeId = parameterTypeId;
            return this;
        }

        public Builder withParameterId(String parameterId)
        {
            this.parameterId = parameterId;
            return this;
        }

        public Builder withSiParameterUnitsConstantValue(Double siParameterUnitsConstantValue)
        {
            this.siParameterUnitsConstantValue = siParameterUnitsConstantValue;
            return this;
        }

        public Builder withLevelUnitsId(String levelUnitsId)
        {
            this.levelUnitsId = levelUnitsId;
            return this;
        }

        public Builder withLevelDate(ZonedDateTime levelDate)
        {
            this.levelDate = levelDate;
            return this;
        }

        public Builder withLevelComment(String levelComment)
        {
            this.levelComment = levelComment;
            return this;
        }

        public Builder withIntervalOrigin(ZonedDateTime intervalOrigin)
        {
            this.intervalOrigin = intervalOrigin;
            return this;
        }

        public Builder withIntervalMonths(Integer intervalMonths)
        {
            this.intervalMonths = intervalMonths;
            return this;
        }

        public Builder withIntervalMinutes(Integer intervalMinutes)
        {
            this.intervalMinutes = intervalMinutes;
            return this;
        }

        public Builder withInterpolateString(String interpolateString)
        {
            this.interpolateString = interpolateString;
            return this;
        }

        public Builder withDurationId(String durationId)
        {
            this.durationId = durationId;
            return this;
        }

        public Builder withAttributeValue(BigDecimal attributeValue)
        {
            this.attributeValue = attributeValue;
            return this;
        }

        public Builder withAttributeUnitsId(String attributeUnitsId)
        {
            this.attributeUnitsId = attributeUnitsId;
            return this;
        }

        public Builder withAttributeParameterTypeId(String attributeParameterTypeId)
        {
            this.attributeParameterTypeId = attributeParameterTypeId;
            return this;
        }

        public Builder withAttributeParameterId(String attributeParameterId)
        {
            this.attributeParameterId = attributeParameterId;
            return this;
        }

        public Builder withAttributeDurationId(String attributeDurationId)
        {
            this.attributeDurationId = attributeDurationId;
            return this;
        }

        public Builder withAttributeComment(String attributeComment)
        {
            this.attributeComment = attributeComment;
            return this;
        }

        public Builder withLocationId(String locationId)
        {
            this.locationId = locationId;
            return this;
        }

        public Builder withOfficeId(String officeId)
        {
            this.officeId = officeId;
            return this;
        }

        public LocationLevel build()
        {
            return new LocationLevel(this);
        }
    }
}
