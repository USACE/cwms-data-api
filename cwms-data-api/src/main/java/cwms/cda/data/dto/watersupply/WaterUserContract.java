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

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import cwms.cda.data.dto.CwmsDTO;
import cwms.cda.data.dto.CwmsId;
import cwms.cda.data.dto.LookupType;
import cwms.cda.formatters.Formats;
import cwms.cda.formatters.annotations.FormattableWith;
import cwms.cda.formatters.json.JsonV1;
import java.util.Date;

@FormattableWith(contentType = Formats.JSONV1, formatter = JsonV1.class)
@JsonDeserialize(builder = WaterUserContract.Builder.class)
public final class WaterUserContract extends CwmsDTO {

    @JsonProperty(required = true)
    private final WaterUser waterUser;
    @JsonProperty(required = true)
    private final CwmsId contractId;
    @JsonProperty(required = true)
    private final LookupType contractType;
    @JsonProperty(required = true)
    private final Date contractEffectiveDate;
    @JsonProperty(required = true)
    private final Date contractExpirationDate;
    @JsonProperty(required = true)
    private final Double contractedStorage;
    @JsonProperty(required = true)
    private final Double initialUseAllocation;
    @JsonProperty(required = true)
    private final Double futureUseAllocation;
    @JsonProperty(required = true)
    private final String storageUnitsId;
    @JsonProperty(required = true)
    private final Double futureUsePercentActivated;
    @JsonProperty(required = true)
    private final Double totalAllocPercentActivated;
    private final WaterSupplyPump pumpOutLocation;
    private final WaterSupplyPump  pumpOutBelowLocation;
    private final WaterSupplyPump  pumpInLocation;

    private WaterUserContract(Builder builder) {
        super(builder.officeId);
        this.waterUser = builder.waterUser;
        this.contractId = builder.contractId;
        this.contractType = builder.contractType;
        this.contractEffectiveDate = builder.contractEffectiveDate;
        this.contractExpirationDate = builder.contractExpirationDate;
        this.contractedStorage = builder.contractedStorage;
        this.initialUseAllocation = builder.initialUseAllocation;
        this.futureUseAllocation = builder.futureUseAllocation;
        this.storageUnitsId = builder.storageUnitsId;
        this.futureUsePercentActivated = builder.futureUsePercentActivated;
        this.totalAllocPercentActivated = builder.totalAllocPercentActivated;
        this.pumpOutLocation = builder.pumpOutLocation;
        this.pumpOutBelowLocation = builder.pumpOutBelowLocation;
        this.pumpInLocation = builder.pumpInLocation;
    }

    public LookupType getContractType() {
        return this.contractType;
    }

    public Date getContractEffectiveDate() {
        return this.contractEffectiveDate;
    }

    public Date getContractExpirationDate() {
        return this.contractExpirationDate;
    }

    public Double getContractedStorage() {
        return this.contractedStorage;
    }

    public Double getInitialUseAllocation() {
        return this.initialUseAllocation;
    }

    public Double getFutureUseAllocation() {
        return this.futureUseAllocation;
    }

    public String getStorageUnitsId() {
        return this.storageUnitsId;
    }

    public Double getFutureUsePercentActivated() {
        return this.futureUsePercentActivated;
    }

    public Double getTotalAllocPercentActivated() {
        return this.totalAllocPercentActivated;
    }

    public WaterSupplyPump getPumpOutLocation() {
        return this.pumpOutLocation;
    }

    public WaterSupplyPump getPumpOutBelowLocation() {
        return this.pumpOutBelowLocation;
    }

    public WaterSupplyPump getPumpInLocation() {
        return this.pumpInLocation;
    }

    public WaterUser getWaterUser() {
        return this.waterUser;
    }

    public CwmsId getContractId() {
        return this.contractId;
    }

    public static class Builder {
        private String officeId;
        private WaterUser waterUser;
        private CwmsId contractId;
        private LookupType contractType;
        private Date contractEffectiveDate;
        private Date contractExpirationDate;
        private Double contractedStorage;
        private Double initialUseAllocation;
        private Double futureUseAllocation;
        private String storageUnitsId;
        private Double futureUsePercentActivated;
        private Double totalAllocPercentActivated;
        private WaterSupplyPump pumpOutLocation;
        private WaterSupplyPump pumpOutBelowLocation;
        private WaterSupplyPump pumpInLocation;

        public Builder withOfficeId(String officeId) {
            this.officeId = officeId;
            return this;
        }

        public Builder withWaterUser(WaterUser waterUser) {
            this.waterUser = waterUser;
            return this;
        }

        public Builder withContractId(CwmsId contractId) {
            this.contractId = contractId;
            return this;
        }

        public Builder withContractType(LookupType contractType) {
            this.contractType = contractType;
            return this;
        }

        public Builder withContractEffectiveDate(Date contractEffectiveDate) {
            this.contractEffectiveDate = contractEffectiveDate;
            return this;
        }

        public Builder withContractExpirationDate(Date contractExpirationDate) {
            this.contractExpirationDate = contractExpirationDate;
            return this;
        }

        public Builder withContractedStorage(Double contractedStorage) {
            this.contractedStorage = contractedStorage;
            return this;
        }

        public Builder withInitialUseAllocation(Double initialUseAllocation) {
            this.initialUseAllocation = initialUseAllocation;
            return this;
        }

        public Builder withFutureUseAllocation(Double futureUseAllocation) {
            this.futureUseAllocation = futureUseAllocation;
            return this;
        }

        public Builder withStorageUnitsId(String storageUnitsId) {
            this.storageUnitsId = storageUnitsId;
            return this;
        }

        public Builder withFutureUsePercentActivated(Double futureUsePercentActivated) {
            this.futureUsePercentActivated = futureUsePercentActivated;
            return this;
        }

        public Builder withTotalAllocPercentActivated(Double totalAllocPercentActivated) {
            this.totalAllocPercentActivated = totalAllocPercentActivated;
            return this;
        }

        public Builder withPumpOutLocation(WaterSupplyPump pumpOutLocation) {
            this.pumpOutLocation = pumpOutLocation;
            return this;
        }

        public Builder withPumpOutBelowLocation(WaterSupplyPump pumpOutBelowLocation) {
            this.pumpOutBelowLocation = pumpOutBelowLocation;
            return this;
        }

        public Builder withPumpInLocation(WaterSupplyPump pumpInLocation) {
            this.pumpInLocation = pumpInLocation;
            return this;
        }

        public WaterUserContract build() {
            return new WaterUserContract(this);
        }
    }


}
