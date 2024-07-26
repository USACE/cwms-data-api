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

import org.jetbrains.annotations.NotNull;

public class AccountingKey implements Comparable<AccountingKey> {
    private final WaterUser waterUser;
    private final String contractName;

    private AccountingKey(Builder builder) {
        this.waterUser = builder.waterUser;
        this.contractName = builder.contractName;
    }

    public WaterUser getWaterUser() {
        return waterUser;
    }

    public String getContractName() {
        return contractName;
    }

    @Override
    public int hashCode() {
        return waterUser.hashCode() + contractName.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        }
        if (!(obj instanceof AccountingKey)) {
            return false;
        }
        AccountingKey key = (AccountingKey) obj;
        return compareWaterUser(waterUser, key.getWaterUser()) && key.getContractName().equals(contractName);
    }

    @Override
    public int compareTo(@NotNull AccountingKey key) {
        if (key == this) {
            return 0;
        }
        int i = contractName.compareTo(key.getContractName());
        if (i == 0) {
            i = compareWaterUser(waterUser, key.getWaterUser()) ? 0 : 1;
        }
        return i;
    }

    private boolean compareWaterUser(WaterUser waterUser1, WaterUser waterUser2) {
        return waterUser1.getEntityName().equals(waterUser2.getEntityName())
                && waterUser1.getProjectId().getName().equals(waterUser2.getProjectId().getName())
                && waterUser1.getProjectId().getOfficeId().equals(waterUser2.getProjectId().getOfficeId())
                && waterUser1.getWaterRight().equals(waterUser2.getWaterRight());
    }

    public static final class Builder {
        private WaterUser waterUser;
        private String contractName;

        public Builder withWaterUser(WaterUser waterUser) {
            this.waterUser = waterUser;
            return this;
        }

        public Builder withContractName(String contractName) {
            this.contractName = contractName;
            return this;
        }

        public AccountingKey build() {
            return new AccountingKey(this);
        }
    }
}
