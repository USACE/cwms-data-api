/*
 *
 * MIT License
 *
 * Copyright (c) 2024 Hydrologic Engineering Center
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 *  to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
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
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER
 * DEALINGS IN THE
 * SOFTWARE.
 */

package cwms.cda.data.dto.watersupply;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import cwms.cda.data.dto.CwmsDTOBase;
import cwms.cda.formatters.Formats;
import cwms.cda.formatters.annotations.FormattableWith;
import cwms.cda.formatters.json.JsonV1;

@JsonDeserialize(builder = PumpColumn.Builder.class)
@FormattableWith(contentType = Formats.JSONV1, formatter = JsonV1.class,
        aliases = {Formats.DEFAULT, Formats.JSON})
public final class PumpColumn extends CwmsDTOBase {
    @JsonProperty(value = "name", required = true)
    private final String name;
    @JsonProperty(value = "ordinal", required = true)
    private final int ordinal;
    @JsonProperty(value = "datatype", required = true)
    private final String dataType;

    private PumpColumn(Builder builder) {
        name = builder.name;
        ordinal = builder.ordinal;
        dataType = builder.dataType;
    }

    public String getName() {
        return name;
    }

    public int getOrdinal() {
        return ordinal;
    }

    public String getDataType() {
        return dataType;
    }

    public static class Builder {
        @JsonProperty("name")
        private String name;
        @JsonProperty("ordinal")
        private int ordinal;
        @JsonProperty("datatype")
        private String dataType;

        public Builder withName(String name) {
            this.name = name;
            return this;
        }

        public Builder withOrdinal(int ordinal) {
            this.ordinal = ordinal;
            return this;
        }

        public Builder withDataType(String dataType) {
            this.dataType = dataType;
            return this;
        }

        public PumpColumn build() {
            return new PumpColumn(this);
        }
    }
}
