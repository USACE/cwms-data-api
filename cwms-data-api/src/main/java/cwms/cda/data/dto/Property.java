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
import com.fasterxml.jackson.annotation.JsonRootName;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import cwms.cda.api.errors.FieldException;
import cwms.cda.formatters.Formats;
import cwms.cda.formatters.annotations.FormattableWith;
import cwms.cda.formatters.json.JsonV1;

import java.util.Objects;

@JsonRootName("property")
@FormattableWith(contentType = Formats.JSON, formatter = JsonV1.class)
@JsonDeserialize(builder = Property.Builder.class)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonNaming(PropertyNamingStrategies.KebabCaseStrategy.class)
public final class Property implements CwmsDTOBase {

    private final String category;
    private final String name;
    private final String officeId;
    private final String value;
    private final String comment; // added comment field

    private Property(Builder builder) {
        this.category = builder.category;
        this.name = builder.name;
        this.officeId = builder.officeId;
        this.value = builder.value;
        this.comment = builder.comment; // included comment in constructor
    }
    
    @Override
    public void validate() throws FieldException {

        if (this.category == null || this.category.trim().isEmpty()) {
            throw new FieldException("The 'category' field of a Property cannot be null or empty.");
        }
        if (this.name == null || this.name.trim().isEmpty()) {
            throw new FieldException("The 'name' field of a Property cannot be null or empty.");
        }
        if (this.officeId == null || this.officeId.trim().isEmpty()) {
            throw new FieldException("The 'office' field of a Property cannot be null or empty.");
        }
    }

    public String getOfficeId() {
        return officeId;
    }

    public String getName() {
        return name;
    }

    public String getCategory() {
        return category;
    }

    public String getValue() {
        return value;
    }

    public String getComment() {
        return comment;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        Property property = (Property) o;
        return Objects.equals(getCategory(), property.getCategory())
                && Objects.equals(getName(), property.getName())
                && Objects.equals(getOfficeId(), property.getOfficeId())
                && Objects.equals(getValue(), property.getValue())
                && Objects.equals(getComment(), property.getComment());
    }

    @Override
    public int hashCode() {
        int result = Objects.hashCode(getCategory());
        result = 31 * result + Objects.hashCode(getName());
        result = 31 * result + Objects.hashCode(getOfficeId());
        result = 31 * result + Objects.hashCode(getValue());
        result = 31 * result + Objects.hashCode(getComment()); // incorporated comment in hashcode
        return result;
    }

    public static class Builder {
        private String category;
        private String name;
        private String officeId;
        private String value;
        private String comment; // added comment to builder

        public Builder withCategory(String category) {
            this.category = category;
            return this;
        }

        public Builder withName(String name) {
            this.name = name;
            return this;
        }

        public Builder withOfficeId(String office) {
            this.officeId = office;
            return this;
        }

        public Builder withValue(String value) {
            this.value = value;
            return this;
        }

        public Builder withComment(String comment) { // new method to include comment in builder
            this.comment = comment;
            return this;
        }

        public Property build() {
            return new Property(this);
        }
    }
}
