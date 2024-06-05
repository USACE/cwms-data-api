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
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import cwms.cda.api.errors.FieldException;
import cwms.cda.formatters.Formats;
import cwms.cda.formatters.annotations.FormattableWith;
import cwms.cda.formatters.json.JsonV1;

import java.util.Objects;

@FormattableWith(contentType = Formats.JSON, formatter = JsonV1.class)
@JsonDeserialize(builder = LookupType.Builder.class)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonNaming(PropertyNamingStrategies.KebabCaseStrategy.class)
public final class LookupType implements CwmsDTOBase {

    private final String officeId;
    private final String displayValue;
    private final String tooltip;
    private final boolean active;

    private LookupType(Builder builder) {
        this.officeId = builder.officeId;
        this.displayValue = builder.displayValue;
        this.tooltip = builder.tooltip;
        this.active = builder.active;
    }

    @Override
    public void validate() throws FieldException {
        if (this.officeId == null || this.officeId.trim().isEmpty()) {
            throw new FieldException("The 'officeId' field of a LookupType cannot be null or empty.");
        }
        if (this.displayValue == null || this.displayValue.trim().isEmpty()) {
            throw new FieldException("The 'displayValue' field of a LookupType cannot be null or empty.");
        }
    }

    public String getOfficeId() {
        return officeId;
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

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        LookupType that = (LookupType) o;
        return Objects.equals(getOfficeId(), that.getOfficeId())
                && Objects.equals(getDisplayValue(), that.getDisplayValue())
                && Objects.equals(getTooltip(), that.getTooltip())
                && Objects.equals(getActive(), that.getActive());
    }

    @Override
    public int hashCode() {
        int result = Objects.hashCode(getOfficeId());
        result = 31 * result + Objects.hashCode(getDisplayValue());
        result = 31 * result + Objects.hashCode(getTooltip());
        result = 31 * result + Objects.hashCode(getActive());
        return result;
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