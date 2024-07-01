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

package cwms.cda.data.dto.watersupply;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import cwms.cda.api.errors.FieldException;
import cwms.cda.data.dto.CwmsDTOBase;
import cwms.cda.formatters.Formats;
import cwms.cda.formatters.annotations.FormattableWith;
import cwms.cda.formatters.json.JsonV1;

@FormattableWith(contentType = Formats.JSONV1, formatter = JsonV1.class)
@JsonDeserialize(builder = WaterSupply.Builder.class)
public class WaterSupply implements CwmsDTOBase {
    private final String contractName;
    private final Integer contractNumber;
    private final String waterUser;
    private final WaterUser user;
    private final WaterUserContract contract;

    private WaterSupply(Builder builder) {
        contractName = builder.contractName;
        contractNumber = builder.contractNumber;
        waterUser = builder.waterUser;
        user = builder.user;
        contract = builder.contract;
    }

    public String getContractName() {
        return contractName;
    }

    public Integer getContractNumber() {
        return contractNumber;
    }

    public String getWaterUser() {
        return waterUser;
    }

    public WaterUser getUser() {
        return user;
    }

    public WaterUserContract getContract() {
        return contract;
    }

    public static class Builder {
        private String contractName;
        private Integer contractNumber;
        private String waterUser;
        private WaterUser user;
        private WaterUserContract contract;

        public Builder withContractName(String contractName) {
            this.contractName = contractName;
            return this;
        }

        public Builder withContractNumber(Integer contractNumber) {
            this.contractNumber = contractNumber;
            return this;
        }

        public Builder withWaterUser(String waterUser) {
            this.waterUser = waterUser;
            return this;
        }

        public Builder withUser(WaterUser user) {
            this.user = user;
            return this;
        }

        public Builder withContract(WaterUserContract contract) {
            this.contract = contract;
            return this;
        }

        public WaterSupply build() {
            return new WaterSupply(this);
        }
    }

    @Override
    public void validate() throws FieldException {
        if (contractName == null || contractName.isEmpty()) {
            throw new FieldException("Contract Name cannot be null or empty");
        }
        if (contractNumber == null) {
            throw new FieldException("Contract Number cannot be null");
        }
        if (waterUser == null || waterUser.isEmpty()) {
            throw new FieldException("Water User cannot be null or empty");
        }
        if (user == null) {
            throw new FieldException("User Type cannot be null");
        }
        if (user.getEntityName() == null) {
            throw new FieldException("User Type Entity Name cannot be null");
        }
        if (user.getWaterRight() == null) {
            throw new FieldException("User Type Water Right cannot be null");
        }
        if (contract == null) {
            throw new FieldException("Contract Type cannot be null");
        }
        if (contract.getWaterUserContractRef() == null) {
            throw new FieldException("Water User Contract Ref Type cannot be null");
        }
        if (contract.getWaterSupplyContract() == null) {
            throw new FieldException("Water Supply Contract Type cannot be null");
        }
        if (contract.getContractEffectiveDate() == null) {
            throw new FieldException("Contract Effective Date cannot be null");
        }
        if (contract.getInitialUseAllocation() == null) {
            throw new FieldException("Contract Initial Use Allocation cannot be null");
        }
        if (contract.getContractedStorage() == null) {
            throw new FieldException("Contracted Storage cannot be null");
        }
    }
}
