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
import java.util.HashMap;
import java.util.Map;
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
import cwms.cda.data.dto.CwmsDTOValidator;
import cwms.cda.formatters.Formats;
import cwms.cda.formatters.annotations.FormattableWith;
import cwms.cda.formatters.json.JsonV1;
import cwms.cda.formatters.json.JsonV2;
import cwms.cda.formatters.xml.XMLv2;
import io.swagger.v3.oas.annotations.media.Schema;

import hec.data.level.IParameterTypedValue;
import hec.data.level.JDomLocationLevelImpl;
import rma.util.RMAConst;

@JsonRootName("ConstantLocationLevel")
@JsonDeserialize(builder = ConstantLocationLevel.Builder.class)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonNaming(PropertyNamingStrategies.KebabCaseStrategy.class)
@FormattableWith(contentType = Formats.JSONV2, formatter = JsonV2.class, aliases = {Formats.DEFAULT, Formats.JSON})
@FormattableWith(contentType = Formats.JSONV1, formatter = JsonV1.class)
@FormattableWith(contentType = Formats.XMLV2, formatter = XMLv2.class, aliases = {Formats.XML})
public final class ConstantLocationLevel extends LocationLevel {
	@Schema(description = "Single value for this location level.")

	private final Double constantValue;

	private ConstantLocationLevel(ConstantLocationLevel.Builder builder) {
		super(builder);
		constantValue = builder.constantValue;
	}

	public Double getConstantValue() {
		return constantValue;
	}

	@JsonPOJOBuilder
	@JsonNaming(PropertyNamingStrategies.KebabCaseStrategy.class)
	@JsonIgnoreProperties(ignoreUnknown = true)
	public static class Builder extends LocationLevel.Builder {
		private Double constantValue;
		final Map<String, Consumer<Object>> propertyFunctionMap = new HashMap<>();

		@JsonCreator
		public Builder(@JsonProperty(value = "location-level-id", required = true) String name,
				@JsonProperty(value = "level-date", required = true) ZonedDateTime lvlDate) {
			super(name, lvlDate);
			buildPropertyFunctions();
		}

		public Builder(ConstantLocationLevel copyFrom) {
			super(copyFrom);
			withConstantValue(copyFrom.getConstantValue());
			buildPropertyFunctions();
		}

		public Builder(JDomLocationLevelImpl copyFrom) {
			super(copyFrom);
			IParameterTypedValue constantLevel = copyFrom.getConstantLevel();
			if (constantLevel != null) {
				withConstantValue(constantLevel.getSiParameterUnitsValue());
			}
			buildPropertyFunctions();
		}

		@JsonIgnore
		private void buildPropertyFunctions() {
			propertyFunctionMap.clear();
			propertyFunctionMap.put("location-level-id",
					nameVal -> super.withLocationLevelId((String) nameVal));
			propertyFunctionMap.put("office-id", officeIdVal -> withOfficeId((String) officeIdVal));
			propertyFunctionMap.put("specified-level-id",
					specifiedLevelIdVal -> withSpecifiedLevelId((String) specifiedLevelIdVal));
			propertyFunctionMap.put("parameter-type-id",
					parameterTypeIdVal -> withParameterTypeId((String) parameterTypeIdVal));
			propertyFunctionMap.put("constant-value",
					constantVal -> withConstantValue((Double) constantVal));
			propertyFunctionMap.put("parameter-id",
					parameterIdVal -> withParameterId((String) parameterIdVal));
			propertyFunctionMap.put("si-parameter-units-constant-value",
					paramUnitsConstVal -> withConstantValue((Double) paramUnitsConstVal));
			propertyFunctionMap.put("level-units-id",
					levelUnitsIdVal -> withLevelUnitsId((String) levelUnitsIdVal));
			propertyFunctionMap.put("level-date",
					levelDateVal -> super.withLevelDate((ZonedDateTime) levelDateVal));
			propertyFunctionMap.put("level-comment",
					levelCommentVal -> withLevelComment((String) levelCommentVal));
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

		public ConstantLocationLevel.Builder withConstantValue(Double value) {
			if (value != null && RMAConst.isUndefinedValue(value)) {
				value = null;
			}
			this.constantValue = value;
			return this;
		}

		public ConstantLocationLevel build() {
			return new ConstantLocationLevel(this);
		}
	}

	@Override
	protected void validateInternal(CwmsDTOValidator validator) {
		super.validateInternal(validator);
		validator.required(getOfficeId(), "office-id");
		validator.required(getLocationLevelId(), "location-level-id");
		validator.required(getConstantValue(), "constant-value");
	}
}
