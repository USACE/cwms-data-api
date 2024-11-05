/*
 * MIT License
 *
 * Copyright (c) 2024 Hydrologic Engineering Center
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
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
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
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
@JsonDeserialize(builder = TurbineSetting.Builder.class)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonNaming(PropertyNamingStrategies.KebabCaseStrategy.class)
@JsonTypeName("turbine-setting")
public final class TurbineSetting extends Setting {

    @JsonProperty(required = true)
    private final String dischargeUnits;
    @JsonProperty(required = true)
    private final Double oldDischarge;
    @JsonProperty(required = true)
    private final Double newDischarge;
    @JsonProperty(required = true)
    private final String generationUnits;
    private final Double scheduledLoad;
    private final Double realPower;

    private TurbineSetting(Builder builder) {
        super(builder);
        this.dischargeUnits = builder.dischargeUnits;
        this.oldDischarge = builder.oldDischarge;
        this.newDischarge = builder.newDischarge;
        this.scheduledLoad = builder.scheduledLoad;
        this.generationUnits = builder.generationUnits;
        this.realPower = builder.realPower;
    }

    public String getDischargeUnits() {
        return dischargeUnits;
    }

    public Double getOldDischarge() {
        return oldDischarge;
    }

    public Double getNewDischarge() {
        return newDischarge;
    }

    public Double getScheduledLoad() {
        return scheduledLoad;
    }

    public String getGenerationUnits() {
        return generationUnits;
    }

    public Double getRealPower() {
        return realPower;
    }

    public static final class Builder extends Setting.Builder<Builder> {

        private String dischargeUnits;
        private Double oldDischarge;
        private Double newDischarge;
        private Double scheduledLoad;
        private String generationUnits;
        private Double realPower;

        public Builder withDischargeUnits(String dischargeUnits) {
            this.dischargeUnits = dischargeUnits;
            return this;
        }

        public Builder withOldDischarge(Double oldDischarge) {
            this.oldDischarge = oldDischarge;
            return this;
        }

        public Builder withNewDischarge(Double newDischarge) {
            this.newDischarge = newDischarge;
            return this;
        }

        public Builder withScheduledLoad(Double scheduledLoad) {
            this.scheduledLoad = scheduledLoad;
            return this;
        }

        public Builder withGenerationUnits(String generationUnits) {
            this.generationUnits = generationUnits;
            return this;
        }

        public Builder withRealPower(Double realPower) {
            this.realPower = realPower;
            return this;
        }

        @Override
        protected Builder self() {
            return this;
        }

        @Override
        public TurbineSetting build() {
            return new TurbineSetting(this);
        }
    }

}
