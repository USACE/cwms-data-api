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
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import cwms.cda.formatters.Formats;
import cwms.cda.formatters.annotations.FormattableWith;
import cwms.cda.formatters.json.JsonV1;
import io.swagger.v3.oas.annotations.media.Schema;

import java.time.ZonedDateTime;

@FormattableWith(contentType = Formats.JSONV1, formatter = JsonV1.class, aliases = {Formats.DEFAULT, Formats.JSON})
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonDeserialize(builder = TimeExtents.Builder.class)
@Schema(description = "Represents the start and end times of an extent")
@JsonNaming(PropertyNamingStrategies.KebabCaseStrategy.class)
public class TimeExtents extends CwmsDTOBase {

    @Schema(description = "Earliest value in the timeseries")
    @JsonFormat(shape = Shape.STRING)
    private final ZonedDateTime earliestTime;

    @Schema(description = "Latest value in the timeseries")
    @JsonFormat(shape = Shape.STRING)
    private final ZonedDateTime latestTime;

    TimeExtents(Builder builder) {
        this.earliestTime = builder.earliestTime;
        this.latestTime = builder.latestTime;
    }

    public ZonedDateTime getEarliestTime() {
        return this.earliestTime;
    }

    public ZonedDateTime getLatestTime() {
        return this.latestTime;
    }

    public static class Builder {
        private ZonedDateTime earliestTime;
        private ZonedDateTime latestTime;

        public Builder withEarliestTime(ZonedDateTime start) {
            this.earliestTime = start;
            return this;
        }

        public Builder withLatestTime(ZonedDateTime end) {
            this.latestTime = end;
            return this;
        }

        public TimeExtents build() {
            return new TimeExtents(this);
        }
    }
}

