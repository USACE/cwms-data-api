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
import java.math.BigInteger;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.ArrayList;
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
import cwms.cda.api.errors.RequiredFieldException;
import cwms.cda.data.dto.CwmsDTOValidator;
import cwms.cda.formatters.Formats;
import cwms.cda.formatters.annotations.FormattableWith;
import cwms.cda.formatters.json.JsonV1;
import cwms.cda.formatters.json.JsonV2;
import cwms.cda.formatters.xml.XMLv2;
import io.swagger.v3.oas.annotations.media.Schema;

import hec.data.level.IParameterTypedValue;
import hec.data.level.ISeasonalInterval;
import hec.data.level.ISeasonalValue;
import hec.data.level.ISeasonalValues;
import hec.data.level.JDomLocationLevelImpl;
import rma.util.RMAConst;

@JsonRootName("SeasonalLocationLevel")
@JsonDeserialize(builder = SeasonalLocationLevel.Builder.class)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonNaming(PropertyNamingStrategies.KebabCaseStrategy.class)
@FormattableWith(contentType = Formats.JSONV2, formatter = JsonV2.class, aliases = {Formats.DEFAULT, Formats.JSON})
@FormattableWith(contentType = Formats.JSONV1, formatter = JsonV1.class)
@FormattableWith(contentType = Formats.XMLV2, formatter = XMLv2.class, aliases = {Formats.XML})
public final class SeasonalLocationLevel extends LocationLevel {
	@Schema(description = "The start point of provided seasonal values")
	@JsonFormat(shape = JsonFormat.Shape.STRING)

	private final ZonedDateTime intervalOrigin;

	private final Integer intervalMonths;

	private final Integer intervalMinutes;


	@Schema(description = "List of Repeating seasonal values. The values repeat after the "
			+ "specified interval."
			+ " A yearly interval seasonable could have 12 different values, one for each month for"
			+ " example. Mutually exclusive with seasonalTimeSeriesId and "
			+ "siParameterUnitsConstantValue")

	private final List<SeasonalValueBean> seasonalValues;

	private SeasonalLocationLevel(SeasonalLocationLevel.Builder builder) {
		super(builder);
		seasonalValues = builder.seasonalValues;
		intervalOrigin = builder.intervalOrigin;
		intervalMonths = builder.intervalMonths;
		intervalMinutes = builder.intervalMinutes;
		validate();
	}

	public List<SeasonalValueBean> getSeasonalValues() {
		return seasonalValues;
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

	@JsonPOJOBuilder
	@JsonNaming(PropertyNamingStrategies.KebabCaseStrategy.class)
	@JsonIgnoreProperties(ignoreUnknown = true)
	public static final class Builder extends LocationLevel.Builder {
		private List<SeasonalValueBean> seasonalValues;
		private ZonedDateTime intervalOrigin;
		private Integer intervalMonths;
		private Integer intervalMinutes;
		private final Map<String, Consumer<Object>> propertyFunctionMap = new HashMap<>();

		@JsonCreator
		public Builder(@JsonProperty(value = "location-level-id", required = true) String name,
				@JsonProperty(value = "level-date", required = true) ZonedDateTime lvlDate) {
			super(name, lvlDate);
			buildPropertyFunctions();
		}

		public Builder(SeasonalLocationLevel copyFrom) {
			super(copyFrom);
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
			withParameterTypeId(copyFrom.getParameterTypeId());
			withSeasonalValues(copyFrom.getSeasonalValues());
			withSpecifiedLevelId(copyFrom.getSpecifiedLevelId());
			buildPropertyFunctions();
		}

		public Builder(JDomLocationLevelImpl copyFrom) {
			super(copyFrom);
			withInterpolateString(copyFrom.getInterpolateString());
			withIntervalMinutes(copyFrom.getIntervalMinutes());
			withIntervalMonths(copyFrom.getIntervalMonths());
			Date intervalOriginDate = copyFrom.getIntervalOrigin();
			if (intervalOriginDate != null) {
				withIntervalOrigin(ZonedDateTime.ofInstant(intervalOriginDate.toInstant(),
						ZoneId.of("UTC")));
			}

			withISeasonalValues(copyFrom.getSeasonalValues());

			withSpecifiedLevelId(copyFrom.getSpecifiedLevelId());
			buildPropertyFunctions();
		}

		@JsonIgnore
		private void buildPropertyFunctions() {
			propertyFunctionMap.clear();
			propertyFunctionMap.put("location-level-id",
					nameVal -> withLocationLevelId((String) nameVal));
			propertyFunctionMap.put("seasonal-values",
					seasonalVals -> withSeasonalValues((List<SeasonalValueBean>) seasonalVals));
			propertyFunctionMap.put("office-id", officeIdVal -> withOfficeId((String) officeIdVal));
			propertyFunctionMap.put("specified-level-id",
					specifiedLevelIdVal -> withSpecifiedLevelId((String) specifiedLevelIdVal));
			propertyFunctionMap.put("parameter-type-id",
					parameterTypeIdVal -> withParameterTypeId((String) parameterTypeIdVal));
			propertyFunctionMap.put("parameter-id",
					parameterIdVal -> withParameterId((String) parameterIdVal));
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

		public SeasonalLocationLevel.Builder withSeasonalValues(List<SeasonalValueBean> seasonalValues) {
			this.seasonalValues = seasonalValues;
			return this;
		}

		@JsonIgnore
		public SeasonalLocationLevel.Builder withISeasonalValues(ISeasonalValues values) {
			if (values != null) {
				withSeasonalValues(buildSeasonalValues(values));
			} else {
				this.seasonalValues = null;
			}

			return this;
		}

		public SeasonalLocationLevel.Builder withSeasonalValue(SeasonalValueBean seasonalValue) {
			if (seasonalValues == null) {
				seasonalValues = new ArrayList<>();
			}
			seasonalValues.add(seasonalValue);
			return this;
		}

		public static SeasonalValueBean buildSeasonalValueBean(ISeasonalValue seasonalValue) {
			SeasonalValueBean retval = null;
			if (seasonalValue != null) {
				IParameterTypedValue value = seasonalValue.getValue();

				if (value != null) {
					SeasonalValueBean.Builder builder =
							new SeasonalValueBean.Builder(value.getSiParameterUnitsValue());

					ISeasonalInterval offset = seasonalValue.getOffset();
					if (offset != null) {
						builder.withOffsetMinutes(BigInteger.valueOf(offset.getTotalMinutes()))
								.withOffsetMonths(offset.getTotalMonths());
					}
					retval = builder.build();
				}
			}
			return retval;
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

		public SeasonalLocationLevel.Builder withIntervalOrigin(ZonedDateTime intervalOrigin) {
			this.intervalOrigin = intervalOrigin;
			return this;
		}

		public SeasonalLocationLevel.Builder withIntervalOrigin(Date intervalOriginDate, ZonedDateTime effectiveDate) {
			if (intervalOriginDate != null && effectiveDate != null) {
				return withIntervalOrigin(ZonedDateTime.ofInstant(intervalOriginDate.toInstant(),
						effectiveDate.getZone()));
			} else {
				this.intervalOrigin = null;
				return this;
			}
		}

		public SeasonalLocationLevel.Builder withIntervalMonths(Integer months) {
			if (months != null && RMAConst.isUndefinedValue(months)) {
				months = null;
			}
			this.intervalMonths = months;
			return this;
		}

		public SeasonalLocationLevel.Builder withIntervalMinutes(Integer minutes) {
			if(minutes != null && RMAConst.isUndefinedValue(minutes)) {
				minutes = null;
			}
			this.intervalMinutes = minutes;
			return this;
		}

		public SeasonalLocationLevel build() {
			return new SeasonalLocationLevel(this);
		}
	}

	@Override
	protected void validateInternal(CwmsDTOValidator validator) {
		super.validateInternal(validator);
		validator.required(getOfficeId(), "office-id");
		validator.required(getLocationLevelId(), "location-level-id");
		validator.required(getSeasonalValues(), "seasonal-values");
		if (getIntervalMonths() == null) {
			validator.required(getIntervalMinutes(), "interval-minutes");
		} else if (getIntervalMinutes() == null) {
			validator.required(getIntervalMonths(), "interval-months");
		} else {
			throw new RequiredFieldException("Either interval-minutes or interval-months must be set, but not both");
		}
		validator.mutuallyExclusive("Only one of the following can be defined at once for a seasonal location level: "
				+ "interval-minutes, interval-months",
				getIntervalMinutes(), getIntervalMonths());
	}
}
