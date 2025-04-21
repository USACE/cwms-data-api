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

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import cwms.cda.data.dto.CwmsDTOBase;
import cwms.cda.data.dto.basin.Basin;
import cwms.cda.formatters.Formats;
import cwms.cda.formatters.annotations.FormattableWith;
import cwms.cda.formatters.json.JsonV1;
import cwms.cda.formatters.json.JsonV2;
import io.swagger.v3.oas.annotations.media.Schema;
import java.time.Instant;
import java.util.Optional;

@FormattableWith(contentType = Formats.JSONV1, formatter = JsonV1.class, aliases = {Formats.DEFAULT, Formats.JSON})
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonNaming(PropertyNamingStrategies.KebabCaseStrategy.class)
@JsonSubTypes({
    @JsonSubTypes.Type(value = RateInputValues.class, name = "RateInputValues"),
    @JsonSubTypes.Type(value = RateInputTimeSeries.class, name = "RateInputTimeSeries")
})
public abstract class RateInput extends CwmsDTOBase {

    @Schema(description = "The units of the output values",
        requiredMode = Schema.RequiredMode.REQUIRED)
    @JsonProperty(required = true)
    private final String outputUnits;

    @Schema(description = "A specific date/time to use as the \"current time\" of the rating.  " +
        "No ratings with a create date later than this will be used. Useful for performing historical ratings. " +
        "If not specified or NULL, the current time is use.",
        requiredMode = Schema.RequiredMode.NOT_REQUIRED)
    private final Instant ratingTime;

    @Schema(description = "A flag specifying whether to round the rated values according to the " +
        "rounding spec contained in the rating specification.",
        requiredMode = Schema.RequiredMode.NOT_REQUIRED)
    private final boolean round;

    protected RateInput(RateInputBuilder<?> builder) {
        this.ratingTime = builder.ratingTime;
        this.outputUnits = builder.outputUnits;
        this.round = builder.round;
    }

    public final Optional<Instant> getRatingTime() {
        return Optional.ofNullable(ratingTime);
    }

    public final String getOutputUnits() {
        return outputUnits;
    }

    public boolean getRound() {
        return round;
    }

    protected abstract static class RateInputBuilder<T extends RateInputBuilder<T>> {
        private boolean round = false;
        private Instant ratingTime;
        private String outputUnits;

        protected abstract T self();

        public abstract RateInput build();

        public T withRatingTime(Instant ratingTime) {
            this.ratingTime = ratingTime;
            return self();
        }

        public T withOutputUnits(String outputUnits) {
            this.outputUnits = outputUnits;
            return self();
        }

        public T withRound(boolean round) {
            this.round = round;
            return self();
        }
    }
}
