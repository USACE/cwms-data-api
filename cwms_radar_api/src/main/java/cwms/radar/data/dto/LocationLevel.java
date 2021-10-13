package cwms.radar.data.dto;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import usace.cwms.db.dao.ifc.level.LocationLevelPojo;
import usace.cwms.db.dao.ifc.level.SeasonalValueBean;

import java.math.BigDecimal;
import java.time.ZoneId;
import java.util.*;
import java.util.function.Consumer;

@JsonDeserialize
@JsonInclude(JsonInclude.Include.NON_NULL)
public class LocationLevel
{
    private String seasonalTimeSeriesId;
    private List<SeasonalValueBean> seasonalValues;
    private String specifiedLevelId;
    private String parameterTypeId;
    private String parameterId;
    private Double siParameterUnitsConstantValue;
    private String levelUnitsId;
    private Date levelDate;
    private String levelComment;
    private Date intervalOrigin;
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
    private ZoneId timeZoneId;
    private final Map<String, Consumer<Object>> propertyFunctionMap = new HashMap<>();

    public LocationLevel()
    {
        super();
        buildPropertyFunctions();
    }

    public LocationLevel(LocationLevelPojo copyFrom)
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
        setIntervalOrigin(copyFrom.getIntervalOrigin());
        setLevelComment(copyFrom.getLevelComment());
        setLevelDate(copyFrom.getLevelDate());
        setLevelUnitsId(copyFrom.getLevelUnitsId());
        setOfficeId(copyFrom.getOfficeId());
        setParameterId(copyFrom.getParameterId());
        setParameterTypeId(copyFrom.getParameterTypeId());
        setSeasonalTimeSeriesId(copyFrom.getSeasonalTimeSeriesId());
        setSeasonalValues(copyFrom.getSeasonalValues());
        setSiParameterUnitsConstantValue(copyFrom.getSiParameterUnitsConstantValue());
        setSpecifiedLevelId(copyFrom.getSpecifiedLevelId());
    }
    @JsonIgnore
    private void buildPropertyFunctions()
    {
        propertyFunctionMap.clear();
        propertyFunctionMap.put("locationId", nameVal -> setLocationId((String)nameVal));
        propertyFunctionMap.put("seasonalTimeSeriesId", tsIdVal -> setSeasonalTimeSeriesId((String)tsIdVal));
        propertyFunctionMap.put("seasonalValues", seasonalVals -> setSeasonalValues((List)seasonalVals));
        propertyFunctionMap.put("officeId", officeIdVal -> setOfficeId((String)officeIdVal));
        propertyFunctionMap.put("specifiedLevelId", specifiedLevelIdVal -> setSpecifiedLevelId((String)specifiedLevelIdVal));
        propertyFunctionMap.put("parameterTypeId", parameterTypeIdVal -> setParameterTypeId((String)parameterTypeIdVal));
        propertyFunctionMap.put("parameterId", parameterIdVal -> setParameterId((String)parameterIdVal));
        propertyFunctionMap.put("siParameterUnitsConstantValue", paramUnitsConstVal -> setSiParameterUnitsConstantValue((Double)paramUnitsConstVal));
        propertyFunctionMap.put("levelUnitsId", levelUnitsIdVal -> setLevelUnitsId((String)levelUnitsIdVal));
        propertyFunctionMap.put("levelDate", levelDateVal -> setLevelDate((Date)levelDateVal));
        propertyFunctionMap.put("levelComment", levelCommentVal -> setLevelComment((String)levelCommentVal));
        propertyFunctionMap.put("intervalOrigin", intervalOriginVal -> setIntervalOrigin((Date)intervalOriginVal));
        propertyFunctionMap.put("intervalMonths", months -> setIntervalMonths((Integer)months));
        propertyFunctionMap.put("intervalMinutes", mins -> setIntervalMinutes((Integer)mins));
        propertyFunctionMap.put("interpolateString", interpolateStr -> setInterpolateString((String)interpolateStr));
        propertyFunctionMap.put("durationId", durationIdVal -> setDurationId((String)durationIdVal));
        propertyFunctionMap.put("attributeValue", attributeVal -> setAttributeValue(BigDecimal.valueOf((Double)attributeVal)));
        propertyFunctionMap.put("attributeUnitsId", attributeUnitsIdVal -> setAttributeUnitsId((String)attributeUnitsIdVal));
        propertyFunctionMap.put("attributeParameterTypeId", attributeParameterTypeIdVal -> setAttributeParameterTypeId((String)attributeParameterTypeIdVal));
        propertyFunctionMap.put("attributeParameterId", attributeParameterIdVal -> setAttributeParameterId((String)attributeParameterIdVal));
        propertyFunctionMap.put("attributeDurationId", attributeDurationIdVal -> setAttributeDurationId((String)attributeDurationIdVal));
        propertyFunctionMap.put("attributeComment", attributeCommentVal -> setAttributeComment((String)attributeCommentVal));
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

    public ZoneId getTimeZoneId() {
        return timeZoneId;
    }

    public void setTimeZoneId(ZoneId timeZoneId) {
        this.timeZoneId = timeZoneId;
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

    public Date getLevelDate()
    {
        return levelDate;
    }

    public void setLevelDate(Date levelDate)
    {
        // java.sql.Timestamp fails on equals against a Date.
        this.levelDate = new Date(levelDate.getTime());
    }

    public String getLevelComment()
    {
        return levelComment;
    }

    public void setLevelComment(String levelComment)
    {
        this.levelComment = levelComment;
    }

    public Date getIntervalOrigin()
    {
        return intervalOrigin;
    }

    public void setIntervalOrigin(Date intervalOrigin)
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
