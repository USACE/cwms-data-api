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

package cwms.cda.data.dto.locationlevel;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import com.fasterxml.jackson.databind.annotation.JsonPOJOBuilder;

import java.math.BigInteger;

@JsonDeserialize(builder = SeasonalValueBean.Builder.class)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonNaming(PropertyNamingStrategies.KebabCaseStrategy.class)
public class SeasonalValueBean {
    private final Double value;
    private final Integer offsetMonths;
    private final BigInteger offsetMinutes;

    private SeasonalValueBean(Builder builder) {
        this.value = builder.value;
        this.offsetMinutes = builder.offsetMinutes;
        this.offsetMonths = builder.offsetMonths;
    }

    public Double getValue()
    {
        return value;
    }
    public BigInteger getOffsetMinutes()
    {
        return offsetMinutes;
    }
    public Integer getOffsetMonths()
    {
        return offsetMonths;
    }

    @JsonPOJOBuilder
    @JsonNaming(PropertyNamingStrategies.KebabCaseStrategy.class)
    public static class Builder {
        private Double value;
        private Integer offsetMonths;
        private BigInteger offsetMinutes;

        public Builder() {
            //No-op
        }

        public Builder(Double value) {
            this.value = value;
            this.offsetMonths = null;
            this.offsetMinutes = null;
        }

        public Builder(String value) {
            this.value = Double.valueOf(value);
            this.offsetMonths = null;
            this.offsetMinutes = null;
        }

        public Builder(SeasonalValueBean bean) {
            this.value = bean.getValue();
            this.offsetMonths = bean.getOffsetMonths();
            this.offsetMinutes = bean.getOffsetMinutes();
        }

        public Builder withValue(Double value) {
            this.value = value;
            return this;
        }

        public Builder withOffsetMinutes(BigInteger totalOffsetMinutes) {
            offsetMinutes = totalOffsetMinutes;
            return this;
        }

        @JsonProperty(value = "offset-months")
        public Builder withOffsetMonths(Integer totalOffsetMonths) {
            offsetMonths = totalOffsetMonths;
            return this;
        }


        @JsonIgnore
        public Builder withOffsetMonths(Byte totalOffsetMonths) {
            if (totalOffsetMonths != null) {
                offsetMonths = totalOffsetMonths.intValue();
            } else {
                offsetMonths = null;
            }
            return this;
        }

        public SeasonalValueBean build() {
            return new SeasonalValueBean(this);
        }

    }

}
