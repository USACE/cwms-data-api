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

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonNaming;

import cwms.cda.formatters.Formats;
import cwms.cda.formatters.annotations.FormattableWith;
import cwms.cda.formatters.json.JsonV1;
import java.util.HashMap;
import java.util.Map;

@FormattableWith(contentType = Formats.JSONV1, formatter = JsonV1.class, aliases = {Formats.DEFAULT, Formats.JSON})
@JsonDeserialize(builder = TimeSeriesIdentifiersByType.Builder.class)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonNaming(PropertyNamingStrategies.KebabCaseStrategy.class)
public final class TimeSeriesIdentifiersByType extends CwmsDTOBase {
    @JsonProperty(required = true)
    private final CwmsId locationId;

    private final Map<String, TimeSeriesMetaData> timeSeriesIds;

    private TimeSeriesIdentifiersByType(Builder builder) {
        this.locationId = builder.locationId;
        this.timeSeriesIds = builder.timeSeriesIds;
    }

    public CwmsId getLocationId() {
        return locationId;
    }

    public Map<String, TimeSeriesMetaData> getTimeSeriesIds() {
        return timeSeriesIds;
    }

    public static class Builder {
        private CwmsId locationId;
        private Map<String, TimeSeriesMetaData> timeSeriesIds = new HashMap<>();

        public Builder withLocationId(CwmsId locationId) {
            this.locationId = locationId;
            return this;
        }

        public Builder withTimeSeriesIds(Map<String, TimeSeriesMetaData> timeSeriesIds) {
            this.timeSeriesIds = timeSeriesIds;
            return this;
        }

        @JsonIgnore
        public Builder withTimeSeriesId(String type, TimeSeriesMetaData tsId) {
            this.timeSeriesIds.put(type, tsId);
            return this;
        }

        public TimeSeriesIdentifiersByType build() {
            return new TimeSeriesIdentifiersByType(this);
        }
    }
}
