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

package cwms.cda.data.dto;

import java.math.BigDecimal;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.function.Consumer;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonRootName;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import com.fasterxml.jackson.databind.annotation.JsonPOJOBuilder;
import cwms.cda.formatters.Formats;
import cwms.cda.formatters.annotations.FormattableWith;
import cwms.cda.formatters.json.JsonV1;
import cwms.cda.formatters.json.JsonV2;
import cwms.cda.formatters.xml.XMLv2;

import hec.data.level.ISeasonalValues;
import rma.util.RMAConst;

@JsonRootName("VirtualLocationLevel")
@JsonDeserialize(builder = VirtualLocationLevel.Builder.class)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonNaming(PropertyNamingStrategies.KebabCaseStrategy.class)
@FormattableWith(contentType = Formats.JSONV2, formatter = JsonV2.class, aliases = {Formats.DEFAULT, Formats.JSON})
@FormattableWith(contentType = Formats.JSONV1, formatter = JsonV1.class)
@FormattableWith(contentType = Formats.XMLV2, formatter = XMLv2.class, aliases = {Formats.XML})
public class VirtualLocationLevel extends LocationLevel {
	private final ZonedDateTime expirationDate;

	private final List<String> constituents;

	private final String constituentConnections;

	private VirtualLocationLevel(Builder builder) {
		super(builder);
		this.expirationDate = builder.expirationDate;
		this.constituents = builder.constituents;
		this.constituentConnections = builder.constituentConnections;
	}

	public ZonedDateTime getExpirationDate()
	{
		return expirationDate;
	}

	public List<String> getConstituents()
	{
		return constituents;
	}

	public String getConstituentConnections()
	{
		return constituentConnections;
	}

	@JsonPOJOBuilder
	@JsonNaming(PropertyNamingStrategies.KebabCaseStrategy.class)
	public static class Builder extends LocationLevel.Builder
	{
		private ZonedDateTime expirationDate;
		private List<String> constituents;
		private String constituentConnections;

		@JsonCreator
		public Builder(@JsonProperty(value = "location-level-id", required = true) String name,
				@JsonProperty(value = "level-date", required = true) ZonedDateTime lvlDate) {
			super(name, lvlDate);
		}

		public Builder withExpirationDate(ZonedDateTime expirationDate) {
			this.expirationDate = expirationDate;
			return this;
		}

		public Builder withConstituents(List<String> constituents) {
			this.constituents = constituents;
			return this;
		}

		public Builder withConstituentConnections(String constituentConnections) {
			this.constituentConnections = constituentConnections;
			return this;
		}

		@Override
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

		@Override
		public Builder withSeasonalTimeSeriesId(String seasonalTimeSeriesId) {
			this.seasonalTimeSeriesId = seasonalTimeSeriesId;
			return this;
		}

		@Override
		public Builder withSeasonalValues(List<SeasonalValueBean> seasonalValues) {
			this.seasonalValues = seasonalValues;
			return this;
		}

		@Override
		@JsonIgnore
		public Builder withISeasonalValues(ISeasonalValues values) {
			if (values != null) {
				// TODO: handle values.offset and values.origin
				withSeasonalValues(buildSeasonalValues(values));
			} else {
				this.seasonalValues = null;
			}

			return this;
		}

		@Override
		public Builder withSeasonalValue(SeasonalValueBean seasonalValue) {
			if (seasonalValues == null) {
				seasonalValues = new ArrayList<>();
			}
			seasonalValues.add(seasonalValue);
			return this;
		}

		@Override
		public Builder withSpecifiedLevelId(String specifiedLevelId) {
			this.specifiedLevelId = specifiedLevelId;
			return this;
		}

		@Override
		public Builder withParameterTypeId(String parameterTypeId) {
			this.parameterTypeId = parameterTypeId;
			return this;
		}

		@Override
		public Builder withParameterId(String parameterId) {
			this.parameterId = parameterId;
			return this;
		}

		@Override
		public Builder withConstantValue(Double value) {
			if (value != null && RMAConst.isUndefinedValue(value)) {
				value = null;
			}
			this.constantValue = value;
			return this;
		}

		@Override
		public Builder withLevelUnitsId(String levelUnitsId) {
			this.levelUnitsId = levelUnitsId;
			return this;
		}

		@Override
		public Builder withLevelDate(ZonedDateTime levelDate) {
			this.levelDate = levelDate;
			return this;
		}

		@Override
		public Builder withLevelComment(String levelComment) {
			this.levelComment = levelComment;
			return this;
		}

		@Override
		public Builder withIntervalOrigin(ZonedDateTime intervalOrigin) {
			this.intervalOrigin = intervalOrigin;
			return this;
		}

		@Override
		public Builder withIntervalOrigin(Date intervalOriginDate, ZonedDateTime effectiveDate) {
			if (intervalOriginDate != null && effectiveDate != null) {
				return withIntervalOrigin(ZonedDateTime.ofInstant(intervalOriginDate.toInstant(),
						effectiveDate.getZone()));
			} else {
				this.intervalOrigin = null;
				return this;
			}
		}

		@Override
		public Builder withIntervalMonths(Integer months) {
			if (months != null && RMAConst.isUndefinedValue(months)) {
				months = null;
			}
			this.intervalMonths = months;
			return this;
		}

		@Override
		public Builder withIntervalMinutes(Integer minutes) {
			if (minutes != null && RMAConst.isUndefinedValue(minutes)) {
				minutes = null;
			}
			this.intervalMinutes = minutes;
			return this;
		}

		@Override
		public Builder withInterpolateString(String interpolateString) {
			this.interpolateString = interpolateString;
			return this;
		}

		@Override
		public Builder withDurationId(String durationId) {
			this.durationId = durationId;
			return this;
		}

		@Override
		public Builder withAttributeValue(BigDecimal attributeValue) {
			this.attributeValue = attributeValue;
			return this;
		}

		@Override
		public Builder withAttributeUnitsId(String attributeUnitsId) {
			this.attributeUnitsId = attributeUnitsId;
			return this;
		}

		@Override
		public Builder withAttributeParameterTypeId(String attributeParameterTypeId) {
			this.attributeParameterTypeId = attributeParameterTypeId;
			return this;
		}

		@Override
		public Builder withAttributeParameterId(String attributeParameterId) {
			this.attributeParameterId = attributeParameterId;
			return this;
		}

		@Override
		public Builder withAttributeDurationId(String attributeDurationId) {
			this.attributeDurationId = attributeDurationId;
			return this;
		}

		@Override
		public Builder withAttributeComment(String attributeComment) {
			this.attributeComment = attributeComment;
			return this;
		}

		@Override
		public Builder withLocationLevelId(String locationId) {
			this.locationId = locationId;
			return this;
		}

		@Override
		public Builder withOfficeId(String officeId) {
			this.officeId = officeId;
			return this;
		}

		@Override
		public VirtualLocationLevel build()
		{
			return new VirtualLocationLevel(this);
		}
	}
}
