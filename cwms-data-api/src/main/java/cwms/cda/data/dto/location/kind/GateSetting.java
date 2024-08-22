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
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import cwms.cda.formatters.Formats;
import cwms.cda.formatters.annotations.FormattableWith;
import cwms.cda.formatters.json.JsonV1;

@FormattableWith(contentType = Formats.JSONV1, formatter = JsonV1.class, aliases = {Formats.DEFAULT, Formats.JSON})
@JsonDeserialize(builder = GateSetting.Builder.class)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonNaming(PropertyNamingStrategies.KebabCaseStrategy.class)
@JsonTypeName("gate-setting")
public final class GateSetting extends Setting {
    @JsonProperty(required = true)
    private final Double opening;
    @JsonProperty(required = true)
    private final String openingParameter;
    @JsonProperty(required = true)
    private final String openingUnits;
    @JsonProperty(required = true)
    private final Double invertElevation;

    private GateSetting(Builder builder) {
        super(builder);
        opening = builder.opening;
        openingParameter = builder.openingParameter;
        openingUnits = builder.openingUnits;
        invertElevation = builder.invertElevation;
    }

    public Double getInvertElevation() {
        return invertElevation;
    }

    public Double getOpening() {
        return opening;
    }

    public String getOpeningParameter() {
        return openingParameter;
    }

    public String getOpeningUnits() {
        return openingUnits;
    }

    public static final class Builder extends Setting.Builder<GateSetting.Builder> {
        private Double opening;
        private String openingParameter;
        private String openingUnits;
        private Double invertElevation;

        @Override
        protected Builder self() {
            return this;
        }

        public Builder withOpening(Double opening) {
            this.opening = opening;
            return self();
        }

        public Builder withOpeningParameter(String openingParameter) {
            this.openingParameter = openingParameter;
            return self();
        }

        public Builder withOpeningUnits(String openingUnits) {
            this.openingUnits = openingUnits;
            return self();
        }

        public Builder withInvertElevation(Double invertElevation) {
            this.invertElevation = invertElevation;
            return self();
        }

        @Override
        public GateSetting build() {
            return new GateSetting(this);
        }
    }
}
