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

import java.time.ZonedDateTime;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonRootName;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import com.fasterxml.jackson.databind.annotation.JsonPOJOBuilder;
import cwms.cda.data.dto.CwmsDTOValidator;
import cwms.cda.formatters.Formats;
import cwms.cda.formatters.annotations.FormattableWith;
import cwms.cda.formatters.json.JsonV1;
import cwms.cda.formatters.json.JsonV2;
import cwms.cda.formatters.xml.XMLv2;
import io.swagger.v3.oas.annotations.media.Schema;

import hec.data.level.JDomLocationLevelImpl;

@JsonRootName("TimeSeriesLocationLevel")
@JsonDeserialize(builder = TimeSeriesLocationLevel.Builder.class)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonNaming(PropertyNamingStrategies.KebabCaseStrategy.class)
@FormattableWith(contentType = Formats.JSONV2, formatter = JsonV2.class, aliases = {Formats.DEFAULT, Formats.JSON})
@FormattableWith(contentType = Formats.JSONV1, formatter = JsonV1.class)
@FormattableWith(contentType = Formats.XMLV2, formatter = XMLv2.class, aliases = {Formats.XML})
public final class TimeSeriesLocationLevel extends LocationLevel {
	@JsonProperty(required = true)
	@Schema(description = "Timeseries ID (e.g. from the times series catalog) to use as the "
			+ "location level. Mutually exclusive with seasonalValues and "
			+ "siParameterUnitsConstantValue")

	private final String seasonalTimeSeriesId;

	private TimeSeriesLocationLevel(TimeSeriesLocationLevel.Builder builder) {
		super(builder);
		seasonalTimeSeriesId = builder.seasonalTimeSeriesId;
	}

	public String getSeasonalTimeSeriesId() {
		return seasonalTimeSeriesId;
	}

	@JsonPOJOBuilder
	@JsonNaming(PropertyNamingStrategies.KebabCaseStrategy.class)
	@JsonIgnoreProperties(ignoreUnknown = true)
	public static final class Builder extends LocationLevel.Builder<TimeSeriesLocationLevel.Builder> {
		@JsonProperty(required = true)
		private String seasonalTimeSeriesId;

		@JsonCreator
		public Builder(@JsonProperty(value = "location-level-id", required = true) String name,
				@JsonProperty(value = "level-date", required = true) ZonedDateTime lvlDate,
				@JsonProperty(value = "seasonal-time-series-id", required = true) String seasonalTimeSeriesId) {
			super(name, lvlDate);
			this.seasonalTimeSeriesId = seasonalTimeSeriesId;
		}

		public Builder(TimeSeriesLocationLevel copyFrom) {
			super(copyFrom);
			this.seasonalTimeSeriesId = copyFrom.getSeasonalTimeSeriesId();
			withSpecifiedLevelId(copyFrom.getSpecifiedLevelId());
		}

		public TimeSeriesLocationLevel.Builder withSeasonalTimeSeriesId(String seasonalTimeSeriesId) {
			this.seasonalTimeSeriesId = seasonalTimeSeriesId;
			return this;
		}

		@Override
		public TimeSeriesLocationLevel build() {
			return new TimeSeriesLocationLevel(this);
		}
	}

	@Override
	protected void validateInternal(CwmsDTOValidator validator) {
		super.validateInternal(validator);
		validator.required(getOfficeId(), "office-id");
		validator.required(getLocationLevelId(), "location-level-id");
		validator.required(getSeasonalTimeSeriesId(), "seasonal-time-series-id");
	}
}
