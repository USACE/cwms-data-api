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

package cwms.cda.data.dto.rating;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import cwms.cda.data.dto.CwmsDTOValidator;
import cwms.cda.data.dto.basin.Basin;
import io.swagger.v3.oas.annotations.media.ArraySchema;
import io.swagger.v3.oas.annotations.media.Schema;
import java.time.Instant;
import java.util.List;
import java.util.Optional;

@JsonDeserialize(builder = RateInputTimeSeries.RateInputTimeSeriesBuilder.class)
public final class RateInputTimeSeries extends RateInput {

    @ArraySchema(
        schema = @Schema(description = "A collection of time series identifiers of the time series to rate, " +
            "in position order of the parameters of the rating." +
            "Must be size 1 for reverse rate.",
            requiredMode = Schema.RequiredMode.REQUIRED)
    )
    @JsonProperty(required = true)
    private final List<String> timeSeriesIds;

    @Schema(description = "The start of the time window to rate.",
        requiredMode = Schema.RequiredMode.REQUIRED)
    @JsonProperty(required = true)
    private final Instant startTime;

    @Schema(description = "The end of the time window to rate.",
        requiredMode = Schema.RequiredMode.REQUIRED)
    @JsonProperty(required = true)
    private final Instant endTime;

    @Schema(description = "Specifies the version date of the retrieve time series.",
        requiredMode = Schema.RequiredMode.NOT_REQUIRED)
    private final Instant versionDate;

    @Schema(description = "Specifies whether to trim missing values from the ends of the retrieved time series.",
        requiredMode = Schema.RequiredMode.NOT_REQUIRED)
    private final boolean trim;

    @Schema(description = "Specifies whether the time window starts on or after the specified time.",
        requiredMode = Schema.RequiredMode.NOT_REQUIRED)
    private final boolean startInclusive;

    @Schema(description = "Specifies whether the time window ends on or before the specified time.",
        requiredMode = Schema.RequiredMode.NOT_REQUIRED)
    private final boolean endInclusive;

    @Schema(description = "Specifies whether to retrieve the latest value before the start of the time window.",
        requiredMode = Schema.RequiredMode.NOT_REQUIRED)
    private final boolean previous;

    @Schema(description = "Specifies whether to retrieve the earliest value after the end of the time window.",
        requiredMode = Schema.RequiredMode.NOT_REQUIRED)
    private final boolean next;

    public RateInputTimeSeries(RateInputTimeSeriesBuilder builder) {
        super(builder);
        this.timeSeriesIds = builder.timeSeriesIds;
        this.startTime = builder.startTime;
        this.endTime = builder.endTime;
        this.versionDate = builder.versionDate;
        this.trim = builder.trim;
        this.startInclusive = builder.startInclusive;
        this.endInclusive = builder.endInclusive;
        this.previous = builder.previous;
        this.next = builder.next;
    }

    public List<String> getTimeSeriesIds() {
        return timeSeriesIds;
    }

    public Instant getStartTime() {
        return startTime;
    }

    public Instant getEndTime() {
        return endTime;
    }

    public Optional<Instant> getVersionDate() {
        return Optional.ofNullable(versionDate);
    }

    public boolean getTrim() {
        return trim;
    }

    public boolean getStartInclusive() {
        return startInclusive;
    }

    public boolean getEndInclusive() {
        return endInclusive;
    }

    public boolean getPrevious() {
        return previous;
    }

    public boolean getNext() {
        return next;
    }

    @Override
    protected void validateInternal(CwmsDTOValidator validator) {
        super.validateInternal(validator);
        validator.validate(() -> {
            if(getTimeSeriesIds().isEmpty()) {
                throw new IllegalArgumentException("At least one time series must be provided");
            }
        });
    }

    public static class RateInputTimeSeriesBuilder extends RateInputBuilder<RateInputTimeSeriesBuilder> {
        private List<String> timeSeriesIds;
        private Instant startTime;
        private Instant endTime;
        private Instant versionDate;
        private boolean trim = false;
        private boolean startInclusive = true;
        private boolean endInclusive = true;
        private boolean previous = false;
        private boolean next = false;

        @Override
        protected RateInputTimeSeriesBuilder self() {
            return this;
        }

        @Override
        public RateInputTimeSeries build() {
            return new RateInputTimeSeries(this);
        }

        public RateInputTimeSeriesBuilder withTimeSeriesIds(List<String> timeSeriesIds) {
            this.timeSeriesIds = timeSeriesIds;
            return this;
        }

        public RateInputTimeSeriesBuilder withStartTime(Instant startTime) {
            this.startTime = startTime;
            return this;
        }

        public RateInputTimeSeriesBuilder withEndTime(Instant endTime) {
            this.endTime = endTime;
            return this;
        }

        public RateInputTimeSeriesBuilder withVersionDate(Instant versionDate) {
            this.versionDate = versionDate;
            return this;
        }

        public RateInputTimeSeriesBuilder withTrim(boolean trim) {
            this.trim = trim;
            return this;
        }

        public RateInputTimeSeriesBuilder withStartInclusive(boolean startInclusive) {
            this.startInclusive = startInclusive;
            return this;
        }

        public RateInputTimeSeriesBuilder withEndInclusive(boolean endInclusive) {
            this.endInclusive = endInclusive;
            return this;
        }

        public RateInputTimeSeriesBuilder withPrevious(boolean previous) {
            this.previous = previous;
            return this;
        }

        public RateInputTimeSeriesBuilder withNext(boolean next) {
            this.next = next;
            return this;
        }
    }
}
