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
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
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

@JsonRootName("VirtualLocationLevel")
@JsonDeserialize(builder = VirtualLocationLevel.Builder.class)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonNaming(PropertyNamingStrategies.KebabCaseStrategy.class)
@FormattableWith(contentType = Formats.JSONV2, formatter = JsonV2.class, aliases = {Formats.DEFAULT, Formats.JSON})
@FormattableWith(contentType = Formats.JSONV1, formatter = JsonV1.class)
@FormattableWith(contentType = Formats.XMLV2, formatter = XMLv2.class, aliases = {Formats.XML})
public final class VirtualLocationLevel extends LocationLevel {
	private final ZonedDateTime expirationDate;

	private final List<Constituent> constituents;

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

	public List<Constituent> getConstituents()
	{
		return constituents;
	}

	public String getConstituentConnections()
	{
		return constituentConnections;
	}

	@JsonPOJOBuilder
	@JsonNaming(PropertyNamingStrategies.KebabCaseStrategy.class)
	@JsonIgnoreProperties(ignoreUnknown = true)
	public static final class Builder extends LocationLevel.Builder
	{
		private ZonedDateTime expirationDate;
		private List<Constituent> constituents;
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

		public Builder withConstituents(List<Constituent> constituents) {
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
		public VirtualLocationLevel.Builder withSpecifiedLevelId(String specifiedLevelId) {
			this.specifiedLevelId = specifiedLevelId;
			return this;
		}

		@Override
		public VirtualLocationLevel.Builder withParameterTypeId(String parameterTypeId) {
			this.parameterTypeId = parameterTypeId;
			return this;
		}

		@Override
		public VirtualLocationLevel.Builder withParameterId(String parameterId) {
			this.parameterId = parameterId;
			return this;
		}

		@Override
		public VirtualLocationLevel.Builder withLevelUnitsId(String levelUnitsId) {
			this.levelUnitsId = levelUnitsId;
			return this;
		}

		@Override
		public VirtualLocationLevel.Builder withLevelDate(ZonedDateTime levelDate) {
			this.levelDate = levelDate;
			return this;
		}

		@Override
		public VirtualLocationLevel.Builder withLevelComment(String levelComment) {
			this.levelComment = levelComment;
			return this;
		}

		@Override
		public VirtualLocationLevel.Builder withDurationId(String durationId) {
			this.durationId = durationId;
			return this;
		}

		@Override
		public VirtualLocationLevel.Builder withAttributeValue(BigDecimal attributeValue) {
			this.attributeValue = attributeValue;
			return this;
		}

		@Override
		public VirtualLocationLevel.Builder withAttributeUnitsId(String attributeUnitsId) {
			this.attributeUnitsId = attributeUnitsId;
			return this;
		}

		@Override
		public VirtualLocationLevel.Builder withAttributeParameterTypeId(String attributeParameterTypeId) {
			this.attributeParameterTypeId = attributeParameterTypeId;
			return this;
		}

		@Override
		public VirtualLocationLevel.Builder withAttributeParameterId(String attributeParameterId) {
			this.attributeParameterId = attributeParameterId;
			return this;
		}

		@Override
		public VirtualLocationLevel.Builder withAttributeDurationId(String attributeDurationId) {
			this.attributeDurationId = attributeDurationId;
			return this;
		}

		@Override
		public VirtualLocationLevel.Builder withAttributeComment(String attributeComment) {
			this.attributeComment = attributeComment;
			return this;
		}

		@Override
		public VirtualLocationLevel.Builder withLocationLevelId(String locationId) {
			this.locationId = locationId;
			return this;
		}

		@Override
		public VirtualLocationLevel.Builder withOfficeId(String officeId) {
			this.officeId = officeId;
			return this;
		}

		@Override
		public VirtualLocationLevel.Builder withInterpolateString(String interpolateString) {
			this.interpolateString = interpolateString;
			return this;
		}

		@Override
		public VirtualLocationLevel build()
		{
			return new VirtualLocationLevel(this);
		}
	}

	@JsonPOJOBuilder
	@JsonNaming(PropertyNamingStrategies.KebabCaseStrategy.class)
	@JsonDeserialize(builder = Constituent.Builder.class)
	@FormattableWith(contentType = Formats.JSONV2, formatter = JsonV2.class, aliases = {Formats.DEFAULT, Formats.JSON})
	public static final class Constituent
	{
		private final String abbr;
		private final String type;
		private final String name;
		private final String attributeId;
		private final Double attributeValue;
		private final String attributeUnits;

		private Constituent(Builder builder)
		{
			this.abbr = builder.abbr;
			this.type = builder.type;
			this.name = builder.name;
			this.attributeId = builder.attributeId;
			this.attributeValue = builder.attributeValue;
			this.attributeUnits = builder.attributeUnits;
		}

		public String getAbbr()
		{
			return abbr;
		}

		public String getType()
		{
			return type;
		}

		public String getName()
		{
			return name;
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

		public List<String> getConstituentList()
		{
			List<String> retVal = new ArrayList<>();
			retVal.add(abbr);
			retVal.add(type);
			retVal.add(name);
			if(attributeId != null)
			{
				retVal.add(attributeId);
			}
			if(attributeValue != null)
			{
				retVal.add(attributeValue.toString());
			}
			if(attributeUnits != null)
			{
				retVal.add(attributeUnits);
			}
			return retVal;
		}

		@JsonPOJOBuilder
		@JsonNaming(PropertyNamingStrategies.KebabCaseStrategy.class)
		@JsonInclude(JsonInclude.Include.NON_NULL)
		public static final class Builder
		{
			private final String abbr;
			private final String type;
			private final String name;
			private String attributeId;
			private Double attributeValue;
			private String attributeUnits;

			@JsonCreator
			public Builder(@JsonProperty(value = "abbr", required = true) String abbr,
					@JsonProperty(value = "type", required = true) String type,
					@JsonProperty(value = "name", required = true) String name)
			{
				this.abbr = abbr;
				this.type = type;
				this.name = name;
			}

			public Builder withAttributeId(String constituentAttributeId)
			{
				this.attributeId = constituentAttributeId;
				return this;
			}

			public Builder withAttributeValue(Double constituentAttributeValue)
			{
				this.attributeValue = constituentAttributeValue;
				return this;
			}

			public Builder withAttributeUnits(String constituentAttributeUnits)
			{
				this.attributeUnits = constituentAttributeUnits;
				return this;
			}

			public Constituent build()
			{
				return new Constituent(this);
			}
		}
	}
}
