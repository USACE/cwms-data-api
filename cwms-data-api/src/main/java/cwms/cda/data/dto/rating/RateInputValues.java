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
import cwms.cda.formatters.Formats;
import cwms.cda.formatters.annotations.FormattableWith;
import cwms.cda.formatters.json.JsonV1;
import io.swagger.v3.oas.annotations.media.ArraySchema;
import io.swagger.v3.oas.annotations.media.Schema;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

@FormattableWith(contentType = Formats.JSONV1, formatter = JsonV1.class, aliases = {Formats.DEFAULT, Formats.JSON})
@JsonDeserialize(builder = RateInputValues.RateInputValuesBuilder.class)
public final class RateInputValues extends RateInput {

    @ArraySchema(
        schema = @Schema(
            description = "The input values. Each value array must be the same length. " +
                "Must be length 1 for reverse rate.",
            requiredMode = Schema.RequiredMode.REQUIRED
        )
    )
    @JsonProperty(required = true)
    private final List<List<Double>> values;

    @ArraySchema(
        schema = @Schema(
            description = "The date/time for each independent parameter value. " +
                "Represents milliseconds since 1970-01-01 (Unix Epoch), always UTC" +
                "Must be of the same length as each values array.",
            requiredMode = Schema.RequiredMode.NOT_REQUIRED,
            implementation = Long.class
        )
    )
    private final List<Long> valueTimes;

    @ArraySchema(
        schema = @Schema(
            description = "The unit of input values and the desired unit of the output. " +
                "Length of the array must be equal to the number of input value arrays.",
            requiredMode = Schema.RequiredMode.REQUIRED
        )
    )
    @JsonProperty(required = true)
    private final List<String> inputUnits;

    private RateInputValues(RateInputValuesBuilder builder) {
        super(builder);
        this.inputUnits = builder.inputUnits;
        this.values = builder.values;
        this.valueTimes = builder.valueTimes;
    }

    public List<List<Double>> getValues() {
        return values;
    }

    public List<Long> getValueTimes() {
        return valueTimes;
    }

    public List<String> getInputUnits() {
        return inputUnits;
    }

    @Override
    protected void validateInternal(CwmsDTOValidator validator) {
        super.validateInternal(validator);
        validator.validate(() -> {
            if(getValues().isEmpty()) {
                throw new IllegalArgumentException("At least one input value must be provided");
            }
            if(getValues().get(0).isEmpty()) {
                throw new IllegalArgumentException("At least one input value must be provided");
            }
            if(getValues().stream().map(List::size).distinct().count() > 1) {
                throw new IllegalArgumentException("The number of values must be the same for each input");
            }
            if(getInputUnits().size() != getValues().size()){
                throw new IllegalArgumentException("The number of input units must match the number of values");
            }
            if (!getValueTimes().isEmpty() && getValues().get(0).size() != getValueTimes().size()) {
                throw new IllegalArgumentException("The number of values must match the number of value times");
            }
        });
    }

    public static final class RateInputValuesBuilder extends RateInputBuilder<RateInputValuesBuilder> {
        private List<Long> valueTimes = new ArrayList<>();
        private List<List<Double>> values;
        private List<String> inputUnits;

        @Override
        protected RateInputValuesBuilder self() {
            return this;
        }

        @Override
        public RateInputValues build() {
            return new RateInputValues(this);
        }

        public RateInputValuesBuilder withValues(List<List<Double>> values) {
            this.values = values;
            return self();
        }

        public RateInputValuesBuilder withValueTimes(List<Long> valueTimes) {
            this.valueTimes = valueTimes;
            return self();
        }

        public RateInputValuesBuilder withInputUnits(List<String> inputUnits) {
            this.inputUnits = inputUnits;
            return self();
        }
    }
}
