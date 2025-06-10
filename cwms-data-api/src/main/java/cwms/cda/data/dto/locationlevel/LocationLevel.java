/*
 *
 * MIT License
 *
 * Copyright (c) 2025 Hydrologic Engineering Center
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 *  to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER
 * DEALINGS IN THE
 * SOFTWARE.
 */

package cwms.cda.data.dto.locationlevel;

import java.math.BigDecimal;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonRootName;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import com.fasterxml.jackson.databind.annotation.JsonPOJOBuilder;
import cwms.cda.data.dto.CwmsDTO;
import cwms.cda.data.dto.CwmsDTOValidator;
import cwms.cda.formatters.Formats;
import cwms.cda.formatters.UnsupportedFormatException;
import cwms.cda.formatters.annotations.FormattableWith;
import cwms.cda.formatters.json.JsonV1;
import cwms.cda.formatters.json.JsonV2;
import cwms.cda.formatters.xml.XMLv2;
import io.swagger.v3.oas.annotations.media.Schema;

import hec.data.level.ILocationLevelRef;
import hec.data.level.JDomLocationLevelImpl;

@JsonRootName("LocationLevel")
@JsonDeserialize(builder = LocationLevel.Builder.class)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonNaming(PropertyNamingStrategies.KebabCaseStrategy.class)
@FormattableWith(contentType = Formats.JSONV2, formatter = JsonV2.class, aliases = {Formats.DEFAULT, Formats.JSON})
@FormattableWith(contentType = Formats.JSONV1, formatter = JsonV1.class)
@FormattableWith(contentType = Formats.XMLV2, formatter = XMLv2.class, aliases = {Formats.XML})
@JsonIgnoreProperties(ignoreUnknown = true)
public abstract class LocationLevel extends CwmsDTO {
    @JsonProperty(required = true)
    @Schema(description = "Name of the location level")

    private final String locationLevelId;
    @Schema(description = "Timeseries ID (e.g. from the times series catalog) to use as the "
            + "location level. Mutually exclusive with seasonalValues and "
            + "siParameterUnitsConstantValue")

    private final String specifiedLevelId;

    private final ZonedDateTime expirationDate;

    @Schema(description = "Data Type such as Stage, Elevation, or others.")

    private final String parameterId;
    @Schema(description = "To indicate if single or aggregate value",
            allowableValues = {"Inst", "Ave", "Min", "Max", "Total"})

    private final String parameterTypeId;
    @Schema(description = "Indicating whether or not to interpolate between seasonal values.",
            allowableValues = {"T", "F"})

    private final String interpolateString;

    @Schema(description = "Units the provided levels are in")

    private final String levelUnitsId;
    @Schema(description = "The date/time at which this location level configuration takes effect.")
    @JsonFormat(shape = JsonFormat.Shape.STRING)

    private final ZonedDateTime levelDate;

    private final String levelComment;
    @Schema(description = "0 if parameterTypeId is Inst. Otherwise duration indicating the time "
            + "window of the aggregate value.")

    private final String durationId;

    private final BigDecimal attributeValue;

    private final String attributeUnitsId;

    private final String attributeParameterTypeId;

    private final String attributeParameterId;

    private final String attributeDurationId;

    private final String attributeComment;

    LocationLevel(LocationLevel.Builder builder) {
        super(builder.officeId);
        specifiedLevelId = builder.specifiedLevelId;
        parameterTypeId = builder.parameterTypeId;
        levelUnitsId = builder.levelUnitsId;
        levelDate = builder.levelDate;
        levelComment = builder.levelComment;
        durationId = builder.durationId;
        attributeValue = builder.attributeValue;
        attributeUnitsId = builder.attributeUnitsId;
        attributeParameterTypeId = builder.attributeParameterTypeId;
        attributeParameterId = builder.attributeParameterId;
        attributeDurationId = builder.attributeDurationId;
        attributeComment = builder.attributeComment;
        locationLevelId = builder.locationId;
        parameterId = builder.parameterId;
        interpolateString = builder.interpolateString;
        expirationDate = builder.expirationDate;
    }

    public String getSpecifiedLevelId() {
        return specifiedLevelId;
    }

    public String getParameterTypeId() {
        return parameterTypeId;
    }

    public String getLevelUnitsId() {
        return levelUnitsId;
    }

    public ZonedDateTime getLevelDate() {
        return levelDate;
    }

    public ZonedDateTime getExpirationDate() {
        return expirationDate;
    }

    public String getLevelComment() {
        return levelComment;
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

    public String getParameterId() {
        return parameterId;
    }

    @JsonPOJOBuilder
    @JsonNaming(PropertyNamingStrategies.KebabCaseStrategy.class)
    public abstract static class Builder<T extends Builder<T>> {
        String specifiedLevelId;
        String parameterTypeId;
        String parameterId;
        String levelUnitsId;
        ZonedDateTime levelDate;
        String levelComment;
        String durationId;
        BigDecimal attributeValue;
        String attributeUnitsId;
        String attributeParameterTypeId;
        String attributeParameterId;
        String attributeDurationId;
        String attributeComment;
        String locationId;
        String officeId;
        String interpolateString;
        ZonedDateTime expirationDate;
        final Map<String, Consumer<Object>> propertyFunctionMap = new HashMap<>();

        @JsonCreator
        protected Builder(@JsonProperty(value = "location-level-id", required = true) String name,
                @JsonProperty(value = "level-date", required = true) ZonedDateTime lvlDate) {
            locationId = name;
            levelDate = lvlDate;
        }

        Builder(LocationLevel copyFrom) {
            withAttributeComment(copyFrom.getAttributeComment());
            withAttributeDurationId(copyFrom.getAttributeDurationId());
            withAttributeParameterId(copyFrom.getAttributeParameterId());
            withLocationLevelId(copyFrom.getLocationLevelId());
            withAttributeValue(copyFrom.getAttributeValue());
            withAttributeParameterTypeId(copyFrom.getAttributeParameterTypeId());
            withAttributeUnitsId(copyFrom.getAttributeUnitsId());
            withDurationId(copyFrom.getDurationId());
            withLevelComment(copyFrom.getLevelComment());
            withLevelDate(copyFrom.getLevelDate());
            withLevelUnitsId(copyFrom.getLevelUnitsId());
            withOfficeId(copyFrom.getOfficeId());
            withParameterTypeId(copyFrom.getParameterTypeId());
            withSpecifiedLevelId(copyFrom.getSpecifiedLevelId());
        }

        Builder(JDomLocationLevelImpl copyFrom) {
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
            withLevelComment(copyFrom.getLevelComment());
            Date copyLevelDate = copyFrom.getLevelDate();
            if (copyLevelDate != null) {
                withLevelDate(ZonedDateTime.ofInstant(copyLevelDate.toInstant(), ZoneId.of("UTC")));
            }
            withLevelUnitsId(copyFrom.getLevelUnitsId());
            withOfficeId(copyFrom.getOfficeId());
            withParameterId(copyFrom.getParameterId());
            withParameterTypeId(copyFrom.getParameterTypeId());
            withSpecifiedLevelId(copyFrom.getSpecifiedLevelId());
        }

        protected T self() {
            return (T) this;
        }

        @JsonIgnore
        public T withProperty(String propertyName, Object value) {
            Consumer<Object> function = propertyFunctionMap.get(propertyName);
            if (function == null) {
                throw new IllegalArgumentException("Property Name does not exist for Location "
                        + "Level");
            }
            function.accept(value);
            return self();
        }

        public T withSpecifiedLevelId(String specifiedLevelId) {
            this.specifiedLevelId = specifiedLevelId;
            return self();
        }

        public T withParameterTypeId(String parameterTypeId) {
            this.parameterTypeId = parameterTypeId;
            return self();
        }

        public T withParameterId(String parameterId) {
            this.parameterId = parameterId;
            return self();
        }

        public T withExpirationDate(ZonedDateTime expirationDate) {
            this.expirationDate = expirationDate;
            return self();
        }

        public T withLevelUnitsId(String levelUnitsId) {
            this.levelUnitsId = levelUnitsId;
            return self();
        }

        public T withLevelDate(ZonedDateTime levelDate) {
            this.levelDate = levelDate;
            return self();
        }

        public T withLevelComment(String levelComment) {
            this.levelComment = levelComment;
            return self();
        }

        public T withInterpolateString(String interpolateString) {
            this.interpolateString = interpolateString;
            return self();
        }

        public T withDurationId(String durationId) {
            this.durationId = durationId;
            return self();
        }

        public T withAttributeValue(BigDecimal attributeValue) {
            this.attributeValue = attributeValue;
            return self();
        }

        public T withAttributeUnitsId(String attributeUnitsId) {
            this.attributeUnitsId = attributeUnitsId;
            return self();
        }

        public T withAttributeParameterTypeId(String attributeParameterTypeId) {
            this.attributeParameterTypeId = attributeParameterTypeId;
            return self();
        }

        public T withAttributeParameterId(String attributeParameterId) {
            this.attributeParameterId = attributeParameterId;
            return self();
        }

        public T withAttributeDurationId(String attributeDurationId) {
            this.attributeDurationId = attributeDurationId;
            return self();
        }

        public T withAttributeComment(String attributeComment) {
            this.attributeComment = attributeComment;
            return self();
        }

        public T withLocationLevelId(String locationId) {
            this.locationId = locationId;
            return self();
        }

        public T withOfficeId(String officeId) {
            this.officeId = officeId;
            return self();
        }
    }

    public static LocationLevel getUpdatedLocationLevel(LocationLevel existingLevel,
            LocationLevel updatedLevel, ZonedDateTime unmarshalledDate) {

        String specifiedLevelId = (updatedLevel.getSpecifiedLevelId() == null
                ? existingLevel.getSpecifiedLevelId() : updatedLevel.getSpecifiedLevelId());
        String parameterTypeId = (updatedLevel.getParameterTypeId() == null
                ? existingLevel.getParameterTypeId() : updatedLevel.getParameterTypeId());
        String parameterId = (updatedLevel.getParameterId() == null
                ? existingLevel.getParameterId() : updatedLevel.getParameterId());

        String levelUnitsId = (updatedLevel.getLevelUnitsId() == null
                ? existingLevel.getLevelUnitsId() : updatedLevel.getLevelUnitsId());
        String levelComment = (updatedLevel.getLevelComment() == null
                ? existingLevel.getLevelComment() : updatedLevel.getLevelComment());

        String durationId = (updatedLevel.getDurationId() == null
                ? existingLevel.getDurationId() : updatedLevel.getDurationId());
        BigDecimal attributeValue = (updatedLevel.getAttributeValue() == null
                ? existingLevel.getAttributeValue() : updatedLevel.getAttributeValue());
        String attributeUnitsId = (updatedLevel.getAttributeUnitsId() == null
                ? existingLevel.getAttributeUnitsId() : updatedLevel.getAttributeUnitsId());
        String attributeParameterTypeId = (updatedLevel.getAttributeParameterTypeId() == null
                ? existingLevel.getAttributeParameterTypeId() :
                updatedLevel.getAttributeParameterTypeId());
        String attributeParameterId = (updatedLevel.getAttributeParameterId() == null
                ? existingLevel.getAttributeParameterId() : updatedLevel.getAttributeParameterId());
        String attributeDurationId = (updatedLevel.getAttributeDurationId() == null
                ? existingLevel.getAttributeDurationId() : updatedLevel.getAttributeDurationId());
        String attributeComment = (updatedLevel.getAttributeComment() == null
                ? existingLevel.getAttributeComment() : updatedLevel.getAttributeComment());
        String locationId = (updatedLevel.getLocationLevelId() == null
                ? existingLevel.getLocationLevelId() : updatedLevel.getLocationLevelId());
        String officeId = (updatedLevel.getOfficeId() == null
                ? existingLevel.getOfficeId() : updatedLevel.getOfficeId());

        if (existingLevel.getAttributeValue() == null) {
            attributeUnitsId = null;
        }

        LocationLevel.Builder builder = null;

        if (existingLevel instanceof VirtualLocationLevel) {
            VirtualLocationLevel virtualLevel = (VirtualLocationLevel) existingLevel;
            VirtualLocationLevel updatedVirtualLevel = (VirtualLocationLevel) updatedLevel;

            String constituentConnections = (updatedVirtualLevel.getConstituentConnections() == null
                    ? virtualLevel.getConstituentConnections() : updatedVirtualLevel.getConstituentConnections());
            List<VirtualLocationLevel.RatingConstituent> constituents = updatedVirtualLevel.getConstituents() == null
                    ? virtualLevel.getConstituents() : updatedVirtualLevel.getConstituents();

            builder = new VirtualLocationLevel.Builder(locationId, unmarshalledDate)
                    .withExpirationDate(updatedVirtualLevel.getExpirationDate() == null
                        ? virtualLevel.getExpirationDate() : updatedVirtualLevel.getExpirationDate())
                    .withConstituents(constituents)
                    .withConstituentConnections(constituentConnections);
        } else if (existingLevel instanceof ConstantLocationLevel) {
            ConstantLocationLevel constantLevel = (ConstantLocationLevel) existingLevel;
            ConstantLocationLevel updatedConstantLevel = (ConstantLocationLevel) updatedLevel;

            Double siParameterUnitsConstantValue = (updatedConstantLevel.getConstantValue() == null
                    ? constantLevel.getConstantValue() : updatedConstantLevel.getConstantValue());

            builder = new ConstantLocationLevel.Builder(locationId, unmarshalledDate)
                    .withConstantValue(siParameterUnitsConstantValue);
        } else if (existingLevel instanceof TimeSeriesLocationLevel) {
            TimeSeriesLocationLevel timeSeriesLevel = (TimeSeriesLocationLevel) existingLevel;
            TimeSeriesLocationLevel updatedTimeSeriesLevel = (TimeSeriesLocationLevel) updatedLevel;

            String seasonalTimeSeriesId = (updatedTimeSeriesLevel.getSeasonalTimeSeriesId() == null
                    ? timeSeriesLevel.getSeasonalTimeSeriesId() : updatedTimeSeriesLevel.getSeasonalTimeSeriesId());

            builder = new TimeSeriesLocationLevel.Builder(locationId, unmarshalledDate, seasonalTimeSeriesId);
        } else if (existingLevel instanceof SeasonalLocationLevel) {
            SeasonalLocationLevel seasonalLevel = (SeasonalLocationLevel) existingLevel;
            SeasonalLocationLevel updatedSeasonalLevel = (SeasonalLocationLevel) updatedLevel;

            ZonedDateTime intervalOrigin = (updatedSeasonalLevel.getIntervalOrigin() == null
                    ? seasonalLevel.getIntervalOrigin() : updatedSeasonalLevel.getIntervalOrigin());
            Integer intervalMinutes = (updatedSeasonalLevel.getIntervalMinutes() == null
                    ? seasonalLevel.getIntervalMinutes() : updatedSeasonalLevel.getIntervalMinutes());
            Integer intervalMonths = (updatedSeasonalLevel.getIntervalMonths() == null
                    ? seasonalLevel.getIntervalMonths() : updatedSeasonalLevel.getIntervalMonths());
            String interpolateString = (updatedSeasonalLevel.getInterpolateString() == null
                    ? seasonalLevel.getInterpolateString() : updatedSeasonalLevel.getInterpolateString());

            List<SeasonalValueBean> seasonalValues = (updatedSeasonalLevel.getSeasonalValues() == null
                    ? seasonalLevel.getSeasonalValues() : updatedSeasonalLevel.getSeasonalValues());

            if (seasonalLevel.getIntervalMonths() != null && seasonalLevel.getIntervalMonths() > 0) {
                intervalMinutes = null;
            } else if (seasonalLevel.getIntervalMinutes() != null
                    && seasonalLevel.getIntervalMinutes() > 0) {
                intervalMonths = null;
            }

            builder = new SeasonalLocationLevel.Builder(locationId, unmarshalledDate)
                    .withSeasonalValues(seasonalValues)
                    .withIntervalMinutes(intervalMinutes)
                    .withIntervalMonths(intervalMonths)
                    .withIntervalOrigin(intervalOrigin)
                    .withInterpolateString(interpolateString);
        }
        if (builder == null) {
            throw new UnsupportedFormatException("Unsupported Location Level type: "
                    + existingLevel.getClass().getName());
        }

        builder.withParameterTypeId(parameterTypeId)
                .withSpecifiedLevelId(specifiedLevelId)
                .withParameterId(parameterId)
                .withLevelUnitsId(levelUnitsId)
                .withLevelComment(levelComment)
                .withDurationId(durationId)
                .withAttributeValue(attributeValue)
                .withAttributeUnitsId(attributeUnitsId)
                .withAttributeParameterTypeId(attributeParameterTypeId)
                .withAttributeParameterId(attributeParameterId)
                .withAttributeDurationId(attributeDurationId)
                .withAttributeComment(attributeComment)
                .withOfficeId(officeId);

        if (builder instanceof SeasonalLocationLevel.Builder) {
            return ((SeasonalLocationLevel.Builder) builder).build();
        } else if (builder instanceof TimeSeriesLocationLevel.Builder) {
            return ((TimeSeriesLocationLevel.Builder) builder).build();
        } else if (builder instanceof ConstantLocationLevel.Builder) {
            return ((ConstantLocationLevel.Builder) builder).build();
        } else if (builder instanceof VirtualLocationLevel.Builder) {
            return ((VirtualLocationLevel.Builder) builder).build();
        } else {
            throw new UnsupportedFormatException("Unsupported Location Level type: "
                    + existingLevel.getClass().getName());
        }
    }

    @Override
    protected void validateInternal(CwmsDTOValidator validator) {
        super.validateInternal(validator);
        validator.required(getOfficeId(), "office-id");
        validator.required(getLocationLevelId(), "location-level-id");
        validator.required(getLevelDate(), "level-date");
    }
}
