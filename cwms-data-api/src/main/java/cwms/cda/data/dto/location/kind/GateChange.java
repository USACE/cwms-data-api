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

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import cwms.cda.formatters.Formats;
import cwms.cda.formatters.annotations.FormattableWith;
import cwms.cda.formatters.json.JsonV1;
import java.util.Objects;

@FormattableWith(contentType = Formats.JSONV1, formatter = JsonV1.class, aliases = {Formats.DEFAULT, Formats.JSON})
@JsonDeserialize(builder = GateChange.Builder.class)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonNaming(PropertyNamingStrategies.KebabCaseStrategy.class)
@JsonPropertyOrder({"project-id", "change-date", "reference-elevation", "pool-elevation", "protected", "discharge-computation-type", "reason-type", "notes"})
public class GateChange  extends PhysicalStructureChange<GateSetting> {
    private final Double referenceElevation;

    @JsonProperty(required = true)
    private final double poolElevation;

    GateChange(Builder builder) {
        super(builder);
        this.poolElevation = Objects.requireNonNull(builder.poolElevation, "Pool elevation is required when creating a GateChange");
        this.referenceElevation = builder.referenceElevation;
    }

    public double getPoolElevation() {
        return poolElevation;
    }

    public Double getReferenceElevation() {
        return referenceElevation;
    }

    public static final class Builder extends PhysicalStructureChange.Builder<GateChange.Builder, GateSetting> {
        private Double referenceElevation;

        public Builder() {
            super();
        }

        public Builder(GateChange gateChange) {
            super(gateChange);
            this.referenceElevation = gateChange.referenceElevation;
            this.poolElevation = gateChange.poolElevation;
        }

        public Builder referenceElevation(Double referenceElevation) {
            this.referenceElevation = referenceElevation;
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
