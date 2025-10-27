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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import cwms.cda.formatters.Formats;
import cwms.cda.formatters.annotations.FormattableWith;
import cwms.cda.formatters.json.JsonV1;
import java.util.Objects;

@FormattableWith(contentType = Formats.JSONV1, formatter = JsonV1.class, aliases = {Formats.DEFAULT, Formats.JSON})
@JsonNaming(PropertyNamingStrategies.KebabCaseStrategy.class)
@JsonPropertyOrder({"pool-name", "office-id"})
public class PoolNameType extends CwmsDTOBase {
    @JsonIgnore
    private final CwmsId pool;

    @JsonCreator(mode = JsonCreator.Mode.PROPERTIES)
    public PoolNameType(@JsonProperty("pool-name") String poolName, @JsonProperty("office-id") String officeId) {
        this.pool = CwmsId.buildCwmsId(officeId, poolName);
    }

    public String getPoolName() {
        return pool.getName();
    }

    public String getOfficeId() {
        return pool.getOfficeId();
    }

    @Override
    public int hashCode() {
        String poolNameString = getCaseInsensitiveValue(this.pool.getName());
        String officeIdString = getCaseInsensitiveValue(this.pool.getOfficeId());

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
        String poolName = getCaseInsensitiveValue(this.pool.getName());
        String otherPoolName = getCaseInsensitiveValue(other.pool.getOfficeId());
        if (!Objects.equals(poolName, otherPoolName)) {
            return false;
        }
        String officeId = getCaseInsensitiveValue(this.pool.getOfficeId());
        String otherOfficeId = getCaseInsensitiveValue(other.pool.getOfficeId());
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
