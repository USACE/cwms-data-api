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
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.*;
import java.util.function.Consumer;

@JsonDeserialize
@JsonPOJOBuilder
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonNaming(PropertyNamingStrategies.KebabCaseStrategy.class)
public class LocationLevel
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
    public LocationLevel(@JsonProperty(value = "location-id") String name, @JsonProperty(value = "level-date") ZonedDateTime effectiveDate)
    {
        super();
        locationId = name;
        levelDate = effectiveDate;
        buildPropertyFunctions();
    }

    public LocationLevel(LocationLevel copyFrom)
    {
        setAttributeComment(copyFrom.getAttributeComment());
        setAttributeDurationId(copyFrom.getAttributeDurationId());
        setAttributeParameterId(copyFrom.getAttributeParameterId());
        setLocationId(copyFrom.getLocationId());
        setAttributeValue(copyFrom.getAttributeValue());
        setAttributeParameterTypeId(copyFrom.getAttributeParameterTypeId());
        setAttributeUnitsId(copyFrom.getAttributeUnitsId());
        setDurationId(copyFrom.getDurationId());
        setInterpolateString(copyFrom.getInterpolateString());
        setIntervalMinutes(copyFrom.getIntervalMinutes());
        setIntervalMonths(copyFrom.getIntervalMonths());
        setIntervalOrigin(ZonedDateTime.ofInstant(copyFrom.getIntervalOrigin().toInstant(), ZoneId.systemDefault()));
        setLevelComment(copyFrom.getLevelComment());
        setLevelDate(ZonedDateTime.ofInstant(copyFrom.getLevelDate().toInstant(), ZoneId.systemDefault()));
        setLevelUnitsId(copyFrom.getLevelUnitsId());
        setOfficeId(copyFrom.getOfficeId());
        setParameterId(copyFrom.getParameterId());
        setParameterTypeId(copyFrom.getParameterTypeId());
        setSeasonalTimeSeriesId(copyFrom.getSeasonalTimeSeriesId());
        setSeasonalValues(copyFrom.getSeasonalValues());
        setSiParameterUnitsConstantValue(copyFrom.getSiParameterUnitsConstantValue());
        setSpecifiedLevelId(copyFrom.getSpecifiedLevelId());
        buildPropertyFunctions();
    }

    @JsonIgnore
    private void buildPropertyFunctions()
    {
        propertyFunctionMap.clear();
        propertyFunctionMap.put("location-id", nameVal -> setLocationId((String)nameVal));
        propertyFunctionMap.put("seasonal-time-series-id", tsIdVal -> setSeasonalTimeSeriesId((String)tsIdVal));
        propertyFunctionMap.put("seasonal-values", seasonalVals -> setSeasonalValues((List<SeasonalValueBean>)seasonalVals));
        propertyFunctionMap.put("office-id", officeIdVal -> setOfficeId((String)officeIdVal));
        propertyFunctionMap.put("specified-level-id", specifiedLevelIdVal -> setSpecifiedLevelId((String)specifiedLevelIdVal));
        propertyFunctionMap.put("parameter-type-id", parameterTypeIdVal -> setParameterTypeId((String)parameterTypeIdVal));
        propertyFunctionMap.put("parameter-id", parameterIdVal -> setParameterId((String)parameterIdVal));
        propertyFunctionMap.put("si-parameter-units-constant-value", paramUnitsConstVal -> setSiParameterUnitsConstantValue((Double)paramUnitsConstVal));
        propertyFunctionMap.put("level-units-id", levelUnitsIdVal -> setLevelUnitsId((String)levelUnitsIdVal));
        propertyFunctionMap.put("level-date", levelDateVal -> setLevelDate((ZonedDateTime)levelDateVal));
        propertyFunctionMap.put("level-comment", levelCommentVal -> setLevelComment((String)levelCommentVal));
        propertyFunctionMap.put("interval-origin", intervalOriginVal -> setIntervalOrigin((ZonedDateTime) intervalOriginVal));
        propertyFunctionMap.put("interval-months", months -> setIntervalMonths((Integer)months));
        propertyFunctionMap.put("interval-minutes", mins -> setIntervalMinutes((Integer)mins));
        propertyFunctionMap.put("interpolate-string", interpolateStr -> setInterpolateString((String)interpolateStr));
        propertyFunctionMap.put("duration-id", durationIdVal -> setDurationId((String)durationIdVal));
        propertyFunctionMap.put("attribute-value", attributeVal -> setAttributeValue(BigDecimal.valueOf((Double)attributeVal)));
        propertyFunctionMap.put("attribute-units-id", attributeUnitsIdVal -> setAttributeUnitsId((String)attributeUnitsIdVal));
        propertyFunctionMap.put("attribute-parameter-type-id", attributeParameterTypeIdVal -> setAttributeParameterTypeId((String)attributeParameterTypeIdVal));
        propertyFunctionMap.put("attribute-parameter-id", attributeParameterIdVal -> setAttributeParameterId((String)attributeParameterIdVal));
        propertyFunctionMap.put("attribute-duration-id", attributeDurationIdVal -> setAttributeDurationId((String)attributeDurationIdVal));
        propertyFunctionMap.put("attribute-comment", attributeCommentVal -> setAttributeComment((String)attributeCommentVal));
    }

    public void setProperty(String propertyName, Object value)
    {
        Consumer<Object> function = propertyFunctionMap.get(propertyName);
        if(function == null)
        {
            throw new IllegalArgumentException("Property Name does not exist for Location Level");
        }
        function.accept(value);
    }

    public String getSeasonalTimeSeriesId()
    {
        return seasonalTimeSeriesId;
    }

    public void setSeasonalTimeSeriesId(String seasonalTimeSeriesId)
    {
        this.seasonalTimeSeriesId = seasonalTimeSeriesId;
    }

    public List<SeasonalValueBean> getSeasonalValues()
    {
        return seasonalValues;
    }

    public void setSeasonalValues(List<SeasonalValueBean> seasonalValues)
    {
        this.seasonalValues = seasonalValues;
    }

    public String getSpecifiedLevelId()
    {
        return specifiedLevelId;
    }

    public void setSpecifiedLevelId(String specifiedLevelId)
    {
        this.specifiedLevelId = specifiedLevelId;
    }

    public String getParameterTypeId()
    {
        return parameterTypeId;
    }

    public void setParameterTypeId(String parameterTypeId)
    {
        this.parameterTypeId = parameterTypeId;
    }

    public String getParameterId()
    {
        return parameterId;
    }

    public void setParameterId(String parameterId)
    {
        this.parameterId = parameterId;
    }

    public Double getSiParameterUnitsConstantValue()
    {
        return siParameterUnitsConstantValue;
    }

    public void setSiParameterUnitsConstantValue(Double siParameterUnitsConstantValue)
    {
        this.siParameterUnitsConstantValue = siParameterUnitsConstantValue;
    }

    public String getLevelUnitsId()
    {
        return levelUnitsId;
    }

    public void setLevelUnitsId(String levelUnitsId)
    {
        this.levelUnitsId = levelUnitsId;
    }

    public ZonedDateTime getLevelDate()
    {
        return levelDate;
    }

    public void setLevelDate(ZonedDateTime levelDate)
    {
        this.levelDate = levelDate;
    }

    public String getLevelComment()
    {
        return levelComment;
    }

    public void setLevelComment(String levelComment)
    {
        this.levelComment = levelComment;
    }

    public ZonedDateTime getIntervalOrigin()
    {
        return intervalOrigin;
    }

    public void setIntervalOrigin(ZonedDateTime intervalOrigin)
    {
        this.intervalOrigin = intervalOrigin;
    }

    public Integer getIntervalMonths()
    {
        return intervalMonths;
    }

    public void setIntervalMonths(Integer intervalMonths)
    {
        this.intervalMonths = intervalMonths;
    }

    public Integer getIntervalMinutes()
    {
        return intervalMinutes;
    }

    public void setIntervalMinutes(Integer intervalMinutes)
    {
        this.intervalMinutes = intervalMinutes;
    }

    public String getInterpolateString()
    {
        return interpolateString;
    }

    public void setInterpolateString(String interpolateString)
    {
        this.interpolateString = interpolateString;
    }

    public String getDurationId()
    {
        return durationId;
    }

    public void setDurationId(String durationId)
    {
        this.durationId = durationId;
    }

    public BigDecimal getAttributeValue()
    {
        return attributeValue;
    }

    public void setAttributeValue(BigDecimal attributeValue)
    {
        this.attributeValue = attributeValue;
    }

    public String getAttributeUnitsId()
    {
        return attributeUnitsId;
    }

    public void setAttributeUnitsId(String attributeUnitsId)
    {
        this.attributeUnitsId = attributeUnitsId;
    }

    public String getAttributeParameterTypeId()
    {
        return attributeParameterTypeId;
    }

    public void setAttributeParameterTypeId(String attributeParameterTypeId)
    {
        this.attributeParameterTypeId = attributeParameterTypeId;
    }

    public String getAttributeParameterId()
    {
        return attributeParameterId;
    }

    public void setAttributeParameterId(String attributeParameterId)
    {
        this.attributeParameterId = attributeParameterId;
    }

    public String getAttributeDurationId()
    {
        return attributeDurationId;
    }

    public void setAttributeDurationId(String attributeDurationId)
    {
        this.attributeDurationId = attributeDurationId;
    }

    public String getAttributeComment()
    {
        return attributeComment;
    }

    public void setAttributeComment(String attributeComment)
    {
        this.attributeComment = attributeComment;
    }

    public String getLocationId()
    {
        return locationId;
    }

    public void setLocationId(String locationId)
    {
        this.locationId = locationId;
    }

    public String getOfficeId()
    {
        return officeId;
    }

    public void setOfficeId(String officeId)
    {
        this.officeId = officeId;
    }
}
