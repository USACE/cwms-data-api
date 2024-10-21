/*
 * MIT License
 * Copyright (c) 2024 Hydrologic Engineering Center
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

package cwms.cda.data.dto.location.kind;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import cwms.cda.data.dto.CwmsDTOValidator;
import cwms.cda.formatters.Formats;
import cwms.cda.formatters.annotations.FormattableWith;
import cwms.cda.formatters.json.JsonV1;


@FormattableWith(contentType = Formats.JSONV1, formatter = JsonV1.class, aliases = {Formats.DEFAULT, Formats.JSON})
@JsonDeserialize(builder = GateChange.Builder.class)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonTypeName("gate-change")
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonNaming(PropertyNamingStrategies.KebabCaseStrategy.class)
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type", visible = true)
@JsonPropertyOrder({"project-id", "change-date", "reference-elevation", "pool-elevation", "protected",
    "discharge-computation-type", "reason-type", "notes", "type"})
public class GateChange  extends PhysicalStructureChange<GateSetting> {
    @JsonProperty("reference-elevation")
    private final Double referenceElevation;
    private static final String type = "gate-change";

    GateChange(Builder builder) {
        super(builder);
        this.referenceElevation = builder.referenceElevation;
    }

    public Double getReferenceElevation() {
        return referenceElevation;
    }

    public String getType() {
        return type;
    }

    @Override
    protected void validateInternal(CwmsDTOValidator validator) {
        validator.required(getPoolElevation(), "pool-elevation");
        super.validateInternal(validator);
    }

    public int compareTo(GateChange other) {
        return this.getChangeDate().compareTo(other.getChangeDate());
    }

    public static final class Builder extends PhysicalStructureChange.Builder<GateChange.Builder, GateSetting> {
        @JsonProperty("reference-elevation")
        private Double referenceElevation;

        public Builder() {
            super();
        }

        public Builder(GateChange gateChange) {
            super(gateChange);
            this.referenceElevation = gateChange.referenceElevation;
        }

        public Builder referenceElevation(Double referenceElevation) {
            this.referenceElevation = referenceElevation;
            return this;
        }

        public Builder withType(String type) {
            return this;
        }

        @Override
        GateChange.Builder self() {
            return this;
        }

        @Override
        public GateChange build() {
            return new GateChange(this);
        }
    }
}
