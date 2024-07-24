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

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import cwms.cda.formatters.Formats;
import cwms.cda.formatters.annotations.FormattableWith;
import cwms.cda.formatters.json.JsonV1;

@FormattableWith(contentType = Formats.JSONV1, formatter = JsonV1.class, aliases = {Formats.JSON, Formats.DEFAULT})
@JsonDeserialize(builder = LookupType.Builder.class)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonNaming(PropertyNamingStrategies.KebabCaseStrategy.class)
public final class LookupType extends CwmsDTO {
    @JsonProperty(required = true)
    private final String displayValue;
    private final String tooltip;
    private final boolean active;

    private LookupType(Builder builder) {
        super(builder.officeId);
        this.displayValue = builder.displayValue;
        this.tooltip = builder.tooltip;
        this.active = builder.active;
    }

    public String getDisplayValue() {
        return displayValue;
    }

    public String getTooltip() {
        return tooltip;
    }

    public boolean getActive() {
        return active;
    }

    public static class Builder {
        private String officeId;
        private String displayValue;
        private String tooltip;
        private boolean active;

        public Builder withOfficeId(String officeId) {
            this.officeId = officeId;
            return this;
        }

        public Builder withDisplayValue(String displayValue) {
            this.displayValue = displayValue;
            return this;
        }

        public Builder withTooltip(String tooltip) {
            this.tooltip = tooltip;
            return this;
        }

        public Builder withActive(boolean active) {
            this.active = active;
            return this;
        }

        public LookupType build() {
            return new LookupType(this);
        }
    }
}