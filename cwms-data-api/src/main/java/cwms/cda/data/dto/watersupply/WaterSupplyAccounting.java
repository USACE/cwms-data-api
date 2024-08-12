/*
 *
 * MIT License
 *
 * Copyright (c) 2024 Hydrologic Engineering Center
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

package cwms.cda.data.dto.watersupply;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import cwms.cda.data.dto.CwmsDTOBase;
import cwms.cda.formatters.Formats;
import cwms.cda.formatters.annotations.FormattableWith;
import cwms.cda.formatters.json.JsonV1;
import java.util.List;

@FormattableWith(contentType = Formats.JSONV1, formatter = JsonV1.class,
        aliases = {Formats.DEFAULT, Formats.JSON})
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonDeserialize(builder = WaterSupplyAccounting.Builder.class)
@FormattableWith(contentType = Formats.JSONV1, formatter = JsonV1.class)
@JsonNaming(PropertyNamingStrategies.KebabCaseStrategy.class)
public final class WaterSupplyAccounting extends CwmsDTOBase {
    @JsonProperty(required = true)
    private final String contractName;
    @JsonProperty(required = true)
    private final WaterUser waterUser;
    private final List<PumpAccounting> pumpAccounting;

    private WaterSupplyAccounting(Builder builder) {
        this.contractName = builder.contractName;
        this.waterUser = builder.waterUser;
        this.pumpAccounting = builder.pumpAccounting;
    }

    public String getContractName() {
        return this.contractName;
    }

    public WaterUser getWaterUser() {
        return this.waterUser;
    }

    public List<PumpAccounting> getPumpAccounting() {
        return this.pumpAccounting;
    }

    public static final class Builder {
        private String contractName;
        private WaterUser waterUser;
        private List<PumpAccounting> pumpAccounting;

        public Builder withContractName(String contractName) {
            this.contractName = contractName;
            return this;
        }

        public Builder withWaterUser(WaterUser waterUser) {
            this.waterUser = waterUser;
            return this;
        }

        public Builder withPumpAccounting(
                List<PumpAccounting> pumpAccounting) {
            this.pumpAccounting = pumpAccounting;
            return this;
        }

        public WaterSupplyAccounting build() {
            return new WaterSupplyAccounting(this);
        }
    }
}
