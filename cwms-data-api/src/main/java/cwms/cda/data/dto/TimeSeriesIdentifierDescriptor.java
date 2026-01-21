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
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import com.fasterxml.jackson.databind.annotation.JsonPOJOBuilder;
import cwms.cda.formatters.Formats;
import cwms.cda.formatters.annotations.FormattableWith;
import cwms.cda.formatters.json.JsonV2;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;

// DTO version of usace.cwms.db.dao.ifc.ts.TimeSeriesIdentifierDescriptor
@JsonDeserialize(builder = cwms.cda.data.dto.TimeSeriesIdentifierDescriptor.Builder.class)
@JsonInclude(JsonInclude.Include.NON_EMPTY)
@JsonNaming(PropertyNamingStrategies.KebabCaseStrategy.class)
@FormattableWith(contentType = Formats.JSONV2, formatter = JsonV2.class, aliases = {Formats.DEFAULT, Formats.JSON})
public class TimeSeriesIdentifierDescriptor extends CwmsDTO {
    private final String timeSeriesId;
    private final String timezoneName;
    private final Long intervalOffsetMinutes;
    private final boolean active;
    private final List<String> aliases;

    private TimeSeriesIdentifierDescriptor(Builder builder) {
        super(builder.officeId);
        this.timeSeriesId = builder.timeSeriesId;
        this.timezoneName = builder.timezoneName;
        this.intervalOffsetMinutes = builder.intervalOffsetMinutes;
        this.active = builder.active;
        this.aliases = builder.aliases;
    }

    public String getTimeSeriesId() {
        return timeSeriesId;
    }

    public String getTimezoneName() {
        return timezoneName;
    }

    public Long getIntervalOffsetMinutes() {
        return intervalOffsetMinutes;
    }

    public boolean isActive() {
        return active;
    }

    public List<String> getAliases() {
        return aliases;
    }

    @JsonPOJOBuilder
    @JsonNaming(PropertyNamingStrategies.KebabCaseStrategy.class)
    public static class Builder {
        private String officeId;
        private String timeSeriesId;
        private String timezoneName;
        private Long intervalOffsetMinutes;
        private boolean active;
        private List<String> aliases = new ArrayList<>();

        public Builder withOfficeId(String officeId) {
            this.officeId = officeId;
            return this;
        }

        public Builder withTimeSeriesId(String timeSeriesId) {
            this.timeSeriesId = timeSeriesId;
            return this;
        }

        public Builder withZoneId(ZoneId zoneId) {
            String tzName = null;

            if (zoneId != null) {
                tzName = zoneId.getId();
            }
            return withTimezoneName(tzName);
        }

        public Builder withTimezoneName(String timezoneName) {
            this.timezoneName = timezoneName;
            return this;
        }

        public Builder withIntervalOffsetMinutes(Long intervalOffsetMinutes) {
            this.intervalOffsetMinutes = intervalOffsetMinutes;
            return this;
        }

        public Builder withActive(boolean active) {
            this.active = active;
            return this;
        }

        public Builder withTimeSeriesIdentifierDescriptor(TimeSeriesIdentifierDescriptor tsid) {
            this.officeId = tsid.getOfficeId();
            this.timeSeriesId = tsid.getTimeSeriesId();
            this.timezoneName = tsid.getTimezoneName();
            this.intervalOffsetMinutes = tsid.getIntervalOffsetMinutes();
            this.active = tsid.isActive();
            this.aliases = tsid.getAliases();
            return this;
        }

        public Builder withAliases(List<String> aliases) {
            this.aliases = aliases;
            return this;
        }

        public TimeSeriesIdentifierDescriptor build() {
            return new TimeSeriesIdentifierDescriptor(this);
        }
    }
}
