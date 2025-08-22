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
package cwms.cda.data.dto;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonFormat.Shape;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import io.swagger.v3.oas.annotations.media.Schema;

import java.sql.Timestamp;
import java.time.ZoneId;
import java.time.ZonedDateTime;

@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonDeserialize(builder = TimeSeriesExtents.Builder.class)
@JsonNaming(PropertyNamingStrategies.KebabCaseStrategy.class)
@Schema(description = "TimeSeries extent information")
public class TimeSeriesExtents extends TimeExtents {

    @Schema(description = "TimeSeries version to which this extent information applies")
    @JsonFormat(shape = Shape.STRING)
    private final ZonedDateTime versionTime;

    @Schema(description = "Last update in the timeseries")
    @JsonFormat(shape = Shape.STRING)
    private final ZonedDateTime lastUpdate;

    private TimeSeriesExtents(Builder builder) {
        super(builder);
        this.versionTime = builder.versionTime;
        this.lastUpdate = builder.lastUpdate;
    }

    public ZonedDateTime getVersionTime() {
        return versionTime;
    }

    public ZonedDateTime getLastUpdate() {
        return lastUpdate;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder extends TimeExtents.Builder {
        private ZonedDateTime versionTime;
        private ZonedDateTime lastUpdate;

        @Override
        public Builder withLatestTime(ZonedDateTime end) {
            return (Builder) super.withLatestTime(end);
        }

        @Override
        public Builder withEarliestTime(ZonedDateTime start) {
            return (Builder) super.withEarliestTime(start);
        }

        public Builder withVersionTime(ZonedDateTime versionTime) {
            this.versionTime = versionTime;
            return this;
        }

        public Builder withLastUpdate(ZonedDateTime lastUpdate) {
            this.lastUpdate = lastUpdate;
            return this;
        }

        public TimeSeriesExtents build() {
            return new TimeSeriesExtents(this);
        }
    }
}
