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
import java.util.ArrayList;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonRootName;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import com.fasterxml.jackson.databind.annotation.JsonPOJOBuilder;
import cwms.cda.formatters.Formats;
import cwms.cda.formatters.annotations.FormattableWith;
import cwms.cda.formatters.json.JsonV1;
import cwms.cda.formatters.json.JsonV2;

@JsonRootName("VirtualLocationLevel")
@JsonDeserialize(builder = VirtualLocationLevel.Builder.class)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonNaming(PropertyNamingStrategies.KebabCaseStrategy.class)
@FormattableWith(contentType = Formats.JSONV2, formatter = JsonV2.class, aliases = {Formats.DEFAULT, Formats.JSON})
@FormattableWith(contentType = Formats.JSONV1, formatter = JsonV1.class)
public final class VirtualLocationLevel extends LocationLevel {
	private final List<RatingConstituent> constituents;

	private final String constituentConnections;

	private VirtualLocationLevel(Builder builder) {
		super(builder);
		this.constituents = builder.constituents;
		this.constituentConnections = builder.constituentConnections;
	}

	public List<RatingConstituent> getConstituents() {
		return constituents;
	}

	public String getConstituentConnections() {
		return constituentConnections;
	}

	@JsonPOJOBuilder
	@JsonNaming(PropertyNamingStrategies.KebabCaseStrategy.class)
	@JsonIgnoreProperties(ignoreUnknown = true)
	public static final class Builder extends LocationLevel.Builder<VirtualLocationLevel.Builder> {
		private List<RatingConstituent> constituents;
		private String constituentConnections;

		@JsonCreator
		public Builder(@JsonProperty(value = "location-level-id", required = true) String name,
				@JsonProperty(value = "level-date", required = true) ZonedDateTime lvlDate) {
			super(name, lvlDate);
		}

		public Builder(VirtualLocationLevel copyFrom) {
			super(copyFrom);
			this.constituents = copyFrom.constituents;
			this.constituentConnections = copyFrom.constituentConnections;
		}

		public Builder withConstituents(List<RatingConstituent> constituents) {
			this.constituents = constituents;
			return this;
		}

		public Builder withConstituentConnections(String constituentConnections) {
			this.constituentConnections = constituentConnections;
			return this;
		}

		@Override
		public VirtualLocationLevel build() {
			return new VirtualLocationLevel(this);
		}
	}

	@JsonRootName("RATING")
	@JsonPOJOBuilder
	@JsonNaming(PropertyNamingStrategies.KebabCaseStrategy.class)
	@JsonDeserialize(builder = RatingConstituent.Builder.class)
	@FormattableWith(contentType = Formats.JSONV2, formatter = JsonV2.class, aliases = {Formats.DEFAULT, Formats.JSON})
	@JsonSubTypes({
			@JsonSubTypes.Type(value = LocationLevelConstituent.class, name = "LOCATION_LEVEL"),
			@JsonSubTypes.Type(value = RatingConstituent.class, name = "RATING")
	})
	@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.EXISTING_PROPERTY, property = "type", visible = true)
	public static class RatingConstituent
	{
		private final String abbr;
		private final String type;
		private final String name;

		private RatingConstituent(Builder builder) {
			this.abbr = builder.abbr;
			this.type = builder.type;
			this.name = builder.name;
		}

		public String getAbbr()
		{
			return abbr;
		}

		public String getType()
		{
			return type;
		}

		public String getName() {
			return name;
		}

		public List<String> getConstituentList() {
			List<String> retVal = new ArrayList<>();
			retVal.add(abbr);
			retVal.add(type);
			retVal.add(name);
			return retVal;
		}

		@JsonPOJOBuilder
		@JsonNaming(PropertyNamingStrategies.KebabCaseStrategy.class)
		@JsonInclude(JsonInclude.Include.NON_NULL)
		public static class Builder {
			private final String abbr;
			private final String type;
			private final String name;

			@JsonCreator
			public Builder(@JsonProperty(value = "abbr", required = true) String abbr,
					@JsonProperty(value = "type", required = true) String type,
					@JsonProperty(value = "name", required = true) String name) {
				this.abbr = abbr;
				this.type = type;
				this.name = name;
			}

			public RatingConstituent build() {
				return new RatingConstituent(this);
			}
		}
	}

	@JsonPOJOBuilder
	@JsonNaming(PropertyNamingStrategies.KebabCaseStrategy.class)
	@JsonDeserialize(builder = LocationLevelConstituent.Builder.class)
	@FormattableWith(contentType = Formats.JSONV2, formatter = JsonV2.class, aliases = {Formats.DEFAULT, Formats.JSON})
	public static final class LocationLevelConstituent extends RatingConstituent
	{
		private final String attributeId;
		private final Double attributeValue;
		private final String attributeUnits;

		private LocationLevelConstituent(Builder builder) {
			super(builder);
			this.attributeId = builder.attributeId;
			this.attributeValue = builder.attributeValue;
			this.attributeUnits = builder.attributeUnits;
		}

		public String getAttributeId()
		{
			return attributeId;
		}

		public Double getAttributeValue()
		{
			return attributeValue;
		}

		public String getAttributeUnits()
		{
			return attributeUnits;
		}

		@Override
		public List<String> getConstituentList() {
			List<String> retVal = new ArrayList<>();
			retVal.add(super.getAbbr());
			retVal.add(super.getType());
			retVal.add(super.getName());
			if(attributeId != null) {
				retVal.add(attributeId);
			}
			if(attributeValue != null) {
				retVal.add(attributeValue.toString());
			}
			if(attributeUnits != null) {
				retVal.add(attributeUnits);
			}
			return retVal;
		}

		@JsonPOJOBuilder
		@JsonNaming(PropertyNamingStrategies.KebabCaseStrategy.class)
		@JsonInclude(JsonInclude.Include.NON_NULL)
		public static final class Builder extends RatingConstituent.Builder {
			@JsonProperty(required = true)
			private final String attributeId;
			private final Double attributeValue;
			private String attributeUnits;

			@JsonCreator
			public Builder(@JsonProperty(value = "abbr", required = true) String abbr,
					@JsonProperty(value = "type", required = true) String type,
					@JsonProperty(value = "name", required = true) String name,
					@JsonProperty(value = "attribute-id", required = true) String attributeId,
					@JsonProperty(value = "attribute-value", required = true) Double attributeValue) {
				super(abbr, type, name);
				this.attributeId = attributeId;
				this.attributeValue = attributeValue;
			}

			public Builder withAttributeUnits(String constituentAttributeUnits) {
				this.attributeUnits = constituentAttributeUnits;
				return this;
			}

			@Override
			public LocationLevelConstituent build() {
				return new LocationLevelConstituent(this);
			}
		}
	}
}
