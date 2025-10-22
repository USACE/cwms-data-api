/*
 *
 * MIT License
 *
 * Copyright (c) 2025 Hydrologic Engineering Center
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

package cwms.cda.data.dto;

import java.util.Objects;

public class PoolNameType extends CwmsDTOBase {
    private final String poolName;
    private final String officeId;

    public PoolNameType(String poolName, String officeId) {
        this.poolName = poolName;
        this.officeId = officeId;
    }

    public String getPoolName() {
        return poolName;
    }

    public String getOfficeId() {
        return officeId;
    }

    @Override
    public int hashCode() {
        String poolNameString = getCaseInsensitiveValue(this.poolName);
        String officeIdString = getCaseInsensitiveValue(this.officeId);

        int hash = 7;
        hash = 47 * hash + Objects.hashCode(poolNameString);
        hash = 47 * hash + Objects.hashCode(officeIdString);
        return hash;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        PoolNameType other = (PoolNameType) obj;
        String poolName = getCaseInsensitiveValue(this.poolName);
        String otherPoolName = getCaseInsensitiveValue(other.poolName);
        if (!Objects.equals(poolName, otherPoolName)) {
            return false;
        }
        String officeId = getCaseInsensitiveValue(this.officeId);
        String otherOfficeId = getCaseInsensitiveValue(other.officeId);
        return Objects.equals(officeId, otherOfficeId);
    }

    String getCaseInsensitiveValue(String value) {
        String output = value;
        if (output != null) {
            output = output.toLowerCase();
        }
        return output;
    }
}
