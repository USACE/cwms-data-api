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
import cwms.cda.formatters.json.JsonV2;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;
import java.util.Objects;

@XmlRootElement(name = "property")
@XmlAccessorType(XmlAccessType.FIELD)
@FormattableWith(contentType = Formats.JSONV2, formatter = JsonV2.class)
@JsonDeserialize(builder = Property.Builder.class)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonNaming(PropertyNamingStrategies.KebabCaseStrategy.class)
public final class Property implements CwmsDTOBase {

    private final String category;
    private final String name;
    private final String office;
    private final String value;

    private Property(Builder builder) {
        this.category = builder.category;
        this.name = builder.name;
        this.office = builder.office;
        this.value = builder.value;
    }
    
    @Override
    public void validate() throws FieldException {

        if (this.category == null || this.category.trim().isEmpty()) {
            throw new FieldException("The 'category' field of a Property cannot be null or empty.");
        }
        if (this.name == null || this.name.trim().isEmpty()) {
            throw new FieldException("The 'name' field of a Property cannot be null or empty.");
        }
        if (this.office == null || this.office.trim().isEmpty()) {
            throw new FieldException("The 'office' field of a Property cannot be null or empty.");
        }
        if (this.value == null || this.value.trim().isEmpty()) {
            throw new FieldException("The 'value' field of a Property cannot be null or empty.");
        }
    }

    public String getOffice() {
        return office;
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
                && Objects.equals(getOffice(), property.getOffice())
                && Objects.equals(getValue(), property.getValue());
    }

    @Override
    public int hashCode() {
        int result = Objects.hashCode(getCategory());
        result = 31 * result + Objects.hashCode(getName());
        result = 31 * result + Objects.hashCode(getOffice());
        result = 31 * result + Objects.hashCode(getValue());
        return result;
    }

    public static class Builder {
        private String category;
        private String name;
        private String office;
        private String value;

        public Builder withCategory(String category) {
            this.category = category;
            return this;
        }

        public Builder withName(String name) {
            this.name = name;
            return this;
        }

        public Builder withOffice(String office) {
            this.office = office;
            return this;
        }

        public Builder withValue(String value) {
            this.value = value;
            return this;
        }

        public Property build() {
            return new Property(this);
        }
    }
}
