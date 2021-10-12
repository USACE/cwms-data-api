package cwms.radar.data.dto;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import io.swagger.models.auth.In;
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
    private String _seasonalTimeSeriesId;
    private List<SeasonalValueBean> _seasonalValues;
    private String _specifiedLevelId;
    private String _parameterTypeId;
    private String _parameterId;
    private Double _siParameterUnitsConstantValue;
    private String _levelUnitsId;
    private Date _levelDate;
    private String _levelComment;
    private Date _intervalOrigin;
    private Integer _intervalMonths;
    private Integer _intervalMinutes;
    private String _interpolateString;
    private String _durationId;
    private BigDecimal _attributeValue;
    private String _attributeUnitsId;
    private String _attributeParameterTypeId;
    private String _attributeParameterId;
    private String _attributeDurationId;
    private String _attributeComment;
    private String _locationId;
    private String _officeId;
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
        propertyFunctionMap.put("seasonalTimeSeriesId", tsId -> setSeasonalTimeSeriesId((String)tsId));
        propertyFunctionMap.put("seasonalValues", seasonalVals -> setSeasonalValues((List)seasonalVals));
        propertyFunctionMap.put("officeId", officeIdVal -> setOfficeId((String)officeIdVal));
        propertyFunctionMap.put("specifiedLevelId", specifiedLevelId -> setSpecifiedLevelId((String)specifiedLevelId));
        propertyFunctionMap.put("parameterTypeId", parameterTypeId -> setParameterTypeId((String)parameterTypeId));
        propertyFunctionMap.put("parameterId", parameterId -> setParameterId((String)parameterId));
        propertyFunctionMap.put("siParameterUnitsConstantValue", paramUnitsConstVal -> setSiParameterUnitsConstantValue((Double)paramUnitsConstVal));
        propertyFunctionMap.put("levelUnitsId", levelUnitsId -> setLevelUnitsId((String)levelUnitsId));
        propertyFunctionMap.put("levelDate", levelDate -> setLevelDate((Date)levelDate));
        propertyFunctionMap.put("levelComment", levelComment -> setLevelComment((String)levelComment));
        propertyFunctionMap.put("intervalOrigin", intervalOrigin -> setIntervalOrigin((Date)intervalOrigin));
        propertyFunctionMap.put("intervalMonths", months -> setIntervalMonths((Integer)months));
        propertyFunctionMap.put("intervalMinutes", mins -> setIntervalMinutes((Integer)mins));
        propertyFunctionMap.put("interpolateString", interpolateStr -> setInterpolateString((String)interpolateStr));
        propertyFunctionMap.put("durationId", durationId -> setDurationId((String)durationId));
        propertyFunctionMap.put("attributeValue", attributeVal -> setAttributeValue(BigDecimal.valueOf((Double)attributeVal)));
        propertyFunctionMap.put("attributeUnitsId", attributeUnitsId -> setAttributeUnitsId((String)attributeUnitsId));
        propertyFunctionMap.put("attributeParameterTypeId", attributeParameterTypeId -> setAttributeParameterTypeId((String)attributeParameterTypeId));
        propertyFunctionMap.put("attributeParameterId", attributeParameterId -> setAttributeParameterId((String)attributeParameterId));
        propertyFunctionMap.put("attributeDurationId", attributeDurationId -> setAttributeDurationId((String)attributeDurationId));
        propertyFunctionMap.put("attributeComment", attributeComment -> setAttributeComment((String)attributeComment));
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
        return _seasonalTimeSeriesId;
    }

    public void setSeasonalTimeSeriesId(String seasonalTimeSeriesId)
    {
        _seasonalTimeSeriesId = seasonalTimeSeriesId;
    }

    public List<SeasonalValueBean> getSeasonalValues()
    {
        return _seasonalValues;
    }

    public void setSeasonalValues(List<SeasonalValueBean> seasonalValues)
    {
        _seasonalValues = seasonalValues;
    }

    public String getSpecifiedLevelId()
    {
        return _specifiedLevelId;
    }

    public void setSpecifiedLevelId(String specifiedLevelId)
    {
        _specifiedLevelId = specifiedLevelId;
    }

    public String getParameterTypeId()
    {
        return _parameterTypeId;
    }

    public void setParameterTypeId(String parameterTypeId)
    {
        _parameterTypeId = parameterTypeId;
    }

    public String getParameterId()
    {
        return _parameterId;
    }

    public void setParameterId(String parameterId)
    {
        _parameterId = parameterId;
    }

    public Double getSiParameterUnitsConstantValue()
    {
        return _siParameterUnitsConstantValue;
    }

    public void setSiParameterUnitsConstantValue(Double siParameterUnitsConstantValue)
    {
        _siParameterUnitsConstantValue = siParameterUnitsConstantValue;
    }

    public String getLevelUnitsId()
    {
        return _levelUnitsId;
    }

    public void setLevelUnitsId(String levelUnitsId)
    {
        _levelUnitsId = levelUnitsId;
    }

    public Date getLevelDate()
    {
        return _levelDate;
    }

    public void setLevelDate(Date levelDate)
    {
        // java.sql.Timestamp fails on equals against a Date.
        _levelDate = new Date(levelDate.getTime());
    }

    public String getLevelComment()
    {
        return _levelComment;
    }

    public void setLevelComment(String levelComment)
    {
        _levelComment = levelComment;
    }

    public Date getIntervalOrigin()
    {
        return _intervalOrigin;
    }

    public void setIntervalOrigin(Date intervalOrigin)
    {
        _intervalOrigin = intervalOrigin;
    }

    public Integer getIntervalMonths()
    {
        return _intervalMonths;
    }

    public void setIntervalMonths(Integer intervalMonths)
    {
        _intervalMonths = intervalMonths;
    }

    public Integer getIntervalMinutes()
    {
        return _intervalMinutes;
    }

    public void setIntervalMinutes(Integer intervalMinutes)
    {
        _intervalMinutes = intervalMinutes;
    }

    public String getInterpolateString()
    {
        return _interpolateString;
    }

    public void setInterpolateString(String interpolateString)
    {
        _interpolateString = interpolateString;
    }

    public String getDurationId()
    {
        return _durationId;
    }

    public void setDurationId(String durationId)
    {
        _durationId = durationId;
    }

    public BigDecimal getAttributeValue()
    {
        return _attributeValue;
    }

    public void setAttributeValue(BigDecimal attributeValue)
    {
        _attributeValue = attributeValue;
    }

    public String getAttributeUnitsId()
    {
        return _attributeUnitsId;
    }

    public void setAttributeUnitsId(String attributeUnitsId)
    {
        _attributeUnitsId = attributeUnitsId;
    }

    public String getAttributeParameterTypeId()
    {
        return _attributeParameterTypeId;
    }

    public void setAttributeParameterTypeId(String attributeParameterTypeId)
    {
        _attributeParameterTypeId = attributeParameterTypeId;
    }

    public String getAttributeParameterId()
    {
        return _attributeParameterId;
    }

    public void setAttributeParameterId(String attributeParameterId)
    {
        _attributeParameterId = attributeParameterId;
    }

    public String getAttributeDurationId()
    {
        return _attributeDurationId;
    }

    public void setAttributeDurationId(String attributeDurationId)
    {
        _attributeDurationId = attributeDurationId;
    }

    public String getAttributeComment()
    {
        return _attributeComment;
    }

    public void setAttributeComment(String attributeComment)
    {
        _attributeComment = attributeComment;
    }

    public String getLocationId()
    {
        return _locationId;
    }

    public void setLocationId(String locationId)
    {
        _locationId = locationId;
    }

    public String getOfficeId()
    {
        return _officeId;
    }

    public void setOfficeId(String officeId)
    {
        _officeId = officeId;
    }
}
