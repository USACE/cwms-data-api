/*
 * MIT License
 *
 * Copyright (c) 2025 Hydrologic Engineering Center
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
package cwms.cda.data.dto;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import cwms.cda.formatters.Formats;
import cwms.cda.formatters.annotations.FormattableWith;
import cwms.cda.formatters.json.JsonV1;

@FormattableWith(contentType = Formats.JSONV1, formatter = JsonV1.class, aliases = {Formats.JSON, Formats.DEFAULT})
@JsonDeserialize(builder = AccessToWaterTimeSeriesIdentifier.Builder.class)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonNaming(PropertyNamingStrategies.KebabCaseStrategy.class)
public final class AccessToWaterTimeSeriesIdentifier extends CwmsDTOBase {
    @JsonProperty(required = true)
    private final CwmsId locationId;
    @JsonProperty(required = true)
    private final TimeSeriesIdentifierDescriptor timeSeriesIdDescriptor;
    @JsonProperty(required = true)
    private final String tsType;

    private AccessToWaterTimeSeriesIdentifier(Builder builder) {
        this.locationId = builder.locationId;
        this.timeSeriesIdDescriptor = builder.timeSeriesIdDescriptor;
        this.tsType = builder.tsType;
    }

    public CwmsId getLocationId() {
        return locationId;
    }

    public TimeSeriesIdentifierDescriptor getTimeSeriesIdDescriptor() {
        return timeSeriesIdDescriptor;
    }

    public String getTsType() {
        return tsType;
    }

    public static class Builder {
        private CwmsId locationId;
        private TimeSeriesIdentifierDescriptor timeSeriesIdDescriptor;
        private String tsType;

        public Builder withLocationId(CwmsId locationId) {
            this.locationId = locationId;
            return this;
        }

        public Builder withTimeSeriesIdDescriptor(TimeSeriesIdentifierDescriptor timeSeriesIdDescriptor) {
            this.timeSeriesIdDescriptor = timeSeriesIdDescriptor;
            return this;
        }

        public Builder withTsType(String tsType) {
            this.tsType = tsType;
            return this;
        }

        public AccessToWaterTimeSeriesIdentifier build() {
            return new AccessToWaterTimeSeriesIdentifier(this);
        }

    }
}
