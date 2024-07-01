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
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import cwms.cda.api.errors.FieldException;
import cwms.cda.api.errors.RequiredFieldException;
import cwms.cda.formatters.Formats;
import cwms.cda.formatters.annotations.FormattableWith;
import cwms.cda.formatters.json.JsonV1;
import java.util.ArrayList;
import java.util.List;

@FormattableWith(contentType = Formats.JSON, formatter = JsonV1.class)
@JsonDeserialize(builder = CwmsId.Builder.class)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonNaming(PropertyNamingStrategies.KebabCaseStrategy.class)
@JsonPropertyOrder({"officeId", "name"})
public final class CwmsId implements CwmsDTOBase {

    private final String officeId;
    private final String name;

    public CwmsId(Builder builder) {
        this.officeId = builder.officeId;
        this.name = builder.name;
    }

    @Override
    public void validate() throws FieldException {
        List<String> missingFields = new ArrayList<>();
        if (this.officeId == null || this.officeId.isEmpty()) {
            missingFields.add("officeId");
        }
        if (this.name == null || this.name.isEmpty()) {
            missingFields.add("name");
        }
        if (!missingFields.isEmpty()) {
            throw new RequiredFieldException(missingFields);
        }
    }

    public String getOfficeId() {
        return officeId;
    }

    public String getName() {
        return name;
    }

    public static class Builder {
        private String officeId;
        private String name;

        public Builder withOfficeId(String officeId) {
            this.officeId = officeId;
            return this;
        }

        public Builder withName(String name) {
            this.name = name;
            return this;
        }

        public CwmsId build() {
            return new CwmsId(this);
        }
    }
}