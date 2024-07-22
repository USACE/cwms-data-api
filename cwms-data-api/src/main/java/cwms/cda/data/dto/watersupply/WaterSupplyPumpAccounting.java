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
@JsonNaming(PropertyNamingStrategies.KebabCaseStrategy.class)
public class WaterSupplyPumpAccounting extends CwmsDTOBase {
    @JsonProperty(required = true)
    private WaterUser waterUser;
    @JsonProperty(required = true)
    private String contractName;
    @JsonProperty(required = true)
    private CwmsId pumpLocation;
    @JsonProperty(required = true)
    private LookupType transferType;
    @JsonProperty(required = true)
    private Double flow;
    @JsonProperty(required = true)
    private Instant transferDate;
    private String comment;

    private WaterSupplyPumpAccounting() {
    }

    public WaterSupplyPumpAccounting(WaterUser waterUser, String contractName, CwmsId pumpLocation,
            LookupType transferType, Double flow, Instant transferDate, String comment) {
        this.waterUser = waterUser;
        this.contractName = contractName;
        this.pumpLocation = pumpLocation;
        this.transferType = transferType;
        this.flow = flow;
        this.transferDate = transferDate;
        this.comment = comment;
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
}
