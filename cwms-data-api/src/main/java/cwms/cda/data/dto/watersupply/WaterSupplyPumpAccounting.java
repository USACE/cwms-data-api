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
import cwms.cda.data.dto.CwmsId;
import cwms.cda.data.dto.LookupType;
import cwms.cda.formatters.Formats;
import cwms.cda.formatters.annotations.FormattableWith;
import cwms.cda.formatters.json.JsonV1;
import java.time.Instant;

@FormattableWith(contentType = Formats.JSONV1, formatter = JsonV1.class,
        aliases = {Formats.DEFAULT, Formats.JSON})
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonDeserialize(builder = WaterSupplyPumpAccounting.Builder.class)
@JsonNaming(PropertyNamingStrategies.KebabCaseStrategy.class)
public class WaterSupplyPumpAccounting extends CwmsDTOBase {
    @JsonProperty(required = true)
    private final WaterUser waterUser;
    @JsonProperty(required = true)
    private final String contractName;
    @JsonProperty(required = true)
    private final CwmsId pumpLocation;
    @JsonProperty(required = true)
    private final LookupType transferType;
    @JsonProperty(required = true)
    private Double flow;
    @JsonProperty(required = true)
    private final Instant transferDate;
    private final String comment;

    private WaterSupplyPumpAccounting(Builder builder) {
        this.waterUser = builder.waterUser;
        this.contractName = builder.contractName;
        this.pumpLocation = builder.pumpLocation;
        this.transferType = builder.transferType;
        this.flow = builder.flow;
        this.transferDate = builder.transferDate;
        this.comment = builder.comment;
    }

    public WaterUser getWaterUser() {
        return this.waterUser;
    }

    public String getContractName() {
        return this.contractName;
    }

    public CwmsId getPumpLocation() {
        return this.pumpLocation;
    }

    public LookupType getTransferType() {
        return this.transferType;
    }

    public Double getFlow() {
        return this.flow;
    }

    public Instant getTransferDate() {
        return this.transferDate;
    }

    public String getComment() {
        return this.comment;
    }

    public void setUndefined() {
        this.flow = Double.NEGATIVE_INFINITY;
    }

    public static final class Builder {
        private WaterUser waterUser;
        private String contractName;
        private CwmsId pumpLocation;
        private LookupType transferType;
        private Double flow;
        private Instant transferDate;
        private String comment;

        public Builder withWaterUser(WaterUser waterUser) {
            this.waterUser = waterUser;
            return this;
        }

        public Builder withContractName(String contractName) {
            this.contractName = contractName;
            return this;
        }

        public Builder withPumpLocation(CwmsId pumpLocation) {
            this.pumpLocation = pumpLocation;
            return this;
        }

        public Builder withTransferType(LookupType transferType) {
            this.transferType = transferType;
            return this;
        }

        public Builder withFlow(Double flow) {
            this.flow = flow;
            return this;
        }

        public Builder withTransferDate(Instant transferDate) {
            this.transferDate = transferDate;
            return this;
        }

        public Builder withComment(String comment) {
            this.comment = comment;
            return this;
        }

        public WaterSupplyPumpAccounting build() {
            return new WaterSupplyPumpAccounting(this);
        }
    }
}
